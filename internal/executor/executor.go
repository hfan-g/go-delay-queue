package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"feng/delay-queue/internal/config"
	"feng/delay-queue/internal/logger"
	"feng/delay-queue/internal/model"
)

type result struct {
	TaskId  string
	Code    int
	Content string
	Header  map[string][]string
}

type Executor struct {
	taskChan   chan *model.Task
	resultChan chan *result
	poolNum    int
	wg         *sync.WaitGroup
	ctx        context.Context
	client     *http.Client
}

func NewExecutor(ctx context.Context, cfg *config.ExecutorConfig, wg *sync.WaitGroup) *Executor {
	return &Executor{
		taskChan:   make(chan *model.Task, 1024),
		resultChan: make(chan *result, 1024),
		poolNum:    cfg.PoolNum,
		wg:         wg,
		ctx:        ctx,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        30,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 5 * time.Second,
		},
	}
}

func (e *Executor) Submit(t *model.Task) error {
	e.taskChan <- t
	return nil
}

func (e *Executor) GetResultChan() <-chan *result {
	return e.resultChan
}

func (e *Executor) Work() {
	for i := 0; i < e.poolNum; i++ {
		e.wg.Add(1)
		go e.worker()
	}
}

func (e *Executor) worker() {
	defer e.wg.Done()
	for {
		select {
		case task, ok := <-e.taskChan:
			if !ok {
				continue
			}
			e.execute(task)
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Executor) execute(t *model.Task) {
	reader := bytes.NewReader([]byte(t.Payload))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", t.CallbackURL, reader)
	if err != nil {
		e.resultChan <- &result{
			TaskId: t.ID,
		}
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		e.resultChan <- &result{
			TaskId: t.ID,
		}
		return
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Get().Error("关闭响应体失败", "error", closeErr.Error())
		}
	}()
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Get().Error("Execute ReadAll error: " + err.Error())
		content = []byte{}
	}

	e.resultChan <- &result{
		TaskId:  t.ID,
		Content: string(content),
		Code:    resp.StatusCode,
		Header:  resp.Header,
	}
}
