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
	wg         sync.WaitGroup
}

func NewExecutor(poolNum int) *Executor {
	return &Executor{
		taskChan:   make(chan *model.Task, 1024),
		resultChan: make(chan *result, 1024),
		poolNum:    poolNum,
	}
}

func (e *Executor) Sublimt(t *model.Task) error {
	e.taskChan <- t
	return nil
}

func (e *Executor) GetResult() *result {
	return <-e.resultChan
}

func (e *Executor) Work() {
	for i := 0; i < e.poolNum; i++ {
		e.worker()
	}
}

func (e *Executor) worker() {
	e.wg.Go(func() {
		for {
			task, ok := <-e.taskChan
			if !ok {
				continue
			}
			e.execute(task)
		}
	})
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
