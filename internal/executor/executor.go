package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"feng/delay-queue/internal/model"
	"io"
	"net/http"
	"sync"
	"time"
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
}

func NewExecutor(ctx context.Context, poolNum int, wg *sync.WaitGroup) *Executor {
	return &Executor{
		taskChan:   make(chan *model.Task, 1024),
		resultChan: make(chan *result, 1024),
		poolNum:    poolNum,
		wg:         wg,
		ctx: ctx,
	}
}

func (e *Executor) Sublimt(t *model.Task) error {
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
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	json, _ := json.Marshal(t.Payload)
	reader := bytes.NewReader(json)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", t.CallbackURL, reader)
	if err != nil {
		e.resultChan <- &result{
			TaskId: t.ID,
		}
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		e.resultChan <- &result{
			TaskId: t.ID,
		}
		return
	}

	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)

	e.resultChan <- &result{
		TaskId:  t.ID,
		Content: string(content),
		Code:    resp.StatusCode,
		Header:  resp.Header,
	}
}
