package scheduler

import (
	"context"
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Scheduler struct {
	Store    store.Store
	Tw       *wheel.Wheel
	TimW     *wheel.TimingWheel
	Executor *executor.Executor
	Wg       *sync.WaitGroup
	Ctx		context.Context
}

func (s *Scheduler) AddTask(t *model.Task) error {
	if err := s.Store.CreateTask(t); err != nil {
		return err
	}
	s.TimW.AddTask(t)
	s.Store.UpdateStatus(t.ID, model.StatusPending, model.StatusReady)

	return nil
}

func (s *Scheduler) HandleExpiredTask(task wheel.ScheduleTask) {
	id := task.GetID()

	fullTask, err := s.Store.GetTask(id)
	if err != nil {
		fmt.Printf("gettask fail ID: %s", id)
		return
	}

	if err := s.Store.UpdateStatus(fullTask.ID, model.StatusReady, model.StatusProcessing); err != nil {
		return
	}

	s.Executor.Sublimt(fullTask)
}

func (s *Scheduler) Result() {
	s.Wg.Add(1)
	go func() {
		defer s.Wg.Done()
		for {
			select {
			case res := <-s.Executor.GetResultChan():
				if res == nil {
					continue
				}
				if res.Code == http.StatusOK {
					s.Store.UpdateStatus(res.TaskId, model.StatusProcessing, model.StatusSuccess)
					fmt.Printf("执行成功 ID: %s \n", res.TaskId)
				} else {
					fmt.Printf("执行失败！ ID: %s \n", res.TaskId)

					// 失败了查看重试次数, 如果超过了最大测试参数直接返回
					t, _ := s.Store.GetTask(res.TaskId)
					if t.RetryCount >= t.MaxRetry {
						s.Store.UpdateStatus(res.TaskId, model.StatusProcessing, model.StatusDead)
						continue
					}
					// 获取下次执行时间，默认5秒
					t.ExecuteAt = time.Now().Add(5 * time.Second)
					t.RetryCount++
					s.retryTask(t, t.ExecuteAt, t.RetryCount)
				}
			case <-s.Ctx.Done():
				return
			}
		}
	}()
}

func (s *Scheduler) Recover() {
	tasks := s.Store.GetProcesingTasks()
	for _, t := range tasks {
		s.TimW.AddTask(t)
	}

	tasts := s.Store.GetReadyTasks()
	for _, t := range tasts {
		s.TimW.AddTask(t)
	}
}

func (s *Scheduler) retryTask(t *model.Task, executeAt time.Time, retryCount int) error {
	err := s.Store.RequeueTask(t.ID, model.StatusProcessing, model.StatusPending, executeAt, retryCount)
	if err != nil {
		return fmt.Errorf("retry task fail, ID: %s", t.ID)
	}

	err = s.TimW.AddTask(t)
	if err != nil {
		return fmt.Errorf("retry task fail, err: %s", err)
	}
	s.Store.UpdateStatus(t.ID, model.StatusPending, model.StatusReady)

	return nil
}
