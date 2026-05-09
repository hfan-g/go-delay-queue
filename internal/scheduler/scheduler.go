package scheduler

import (
	"context"
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/logger"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Scheduler struct {
	Store         store.Store
	TimW          *wheel.TimingWheel
	Executor      *executor.Executor
	Wg            *sync.WaitGroup
	Ctx           context.Context
	RetryInterval time.Duration
}

func (s *Scheduler) AddTask(ctx context.Context, t *model.Task) error {
	if err := s.Store.CreateTask(ctx, t); err != nil {
		return err
	}
	s.TimW.AddTask(t)
	s.Store.UpdateStatus(ctx, t.ID, model.StatusPending, model.StatusReady)

	return nil
}

func (s *Scheduler) HandleExpiredTask(task wheel.ScheduleTask) {
	id := task.GetID()

	fullTask, err := s.Store.GetTask(s.Ctx, id)
	if err != nil {
		logger.Get().Error("get task fail",
			"id", id,
		)
		return
	}

	// 幂等检查: 只有 Ready 状态才执行
	if fullTask.Status != model.StatusReady {
		return
	}

	if err := s.Store.UpdateStatus(s.Ctx, fullTask.ID, model.StatusReady, model.StatusProcessing); err != nil {
		return
	}

	s.Executor.Submit(fullTask)
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
					s.Store.UpdateStatus(s.Ctx, res.TaskId, model.StatusProcessing, model.StatusSuccess)
					logger.Get().Info("执行成功",
						"id", res.TaskId,
					)
				} else {
					logger.Get().Error("执行失败！",
						"id", res.TaskId,
					)
					// 失败了查看重试次数, 如果超过了最大测试参数直接返回
					t, err := s.Store.GetTask(s.Ctx, res.TaskId)
					if err != nil {
						logger.Get().Error("获取任务失败",
							"id", res.TaskId,
							"error", err.Error(),
						)
						continue
					}
					if t.RetryCount >= t.MaxRetry {
						s.Store.UpdateStatus(s.Ctx, res.TaskId, model.StatusProcessing, model.StatusDead)
						continue
					}
					t.ExecuteAt = time.Now().Add(s.RetryInterval)
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
	tasks := s.Store.GetProcessingTasks(s.Ctx)
	for _, t := range tasks {
		s.TimW.AddTask(t)
	}

	tasts := s.Store.GetReadyTasks(s.Ctx)
	for _, t := range tasts {
		s.TimW.AddTask(t)
	}
}

func (s *Scheduler) retryTask(t *model.Task, executeAt time.Time, retryCount int) error {
	err := s.Store.RequeueTask(s.Ctx, t.ID, model.StatusProcessing, model.StatusPending, executeAt, retryCount)
	if err != nil {
		return fmt.Errorf("retry task fail, ID: %s", t.ID)
	}

	err = s.TimW.AddTask(t)
	if err != nil {
		return fmt.Errorf("retry task fail, err: %s", err)
	}
	s.Store.UpdateStatus(s.Ctx, t.ID, model.StatusPending, model.StatusReady)

	return nil
}
