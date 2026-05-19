package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/logger"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
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
	if err := s.TimW.AddTask(t); err != nil {
		return err
	}
	if err := s.Store.UpdateStatus(ctx, t.ID, model.StatusPending, model.StatusReady); err != nil {
		return err
	}

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

	if err := s.Executor.Submit(fullTask); err != nil {
		logger.Get().Error("HandleExpiredTask Submit error: "+err.Error(), "ID", fullTask.ID)
	}
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
					if err := s.Store.UpdateStatus(s.Ctx, res.TaskId, model.StatusProcessing, model.StatusSuccess); err != nil {
						logger.Get().Error("执行成功，但是修改状态失败", "id", res.TaskId)
					}
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
						if err := s.Store.UpdateStatus(s.Ctx, res.TaskId, model.StatusProcessing, model.StatusDead); err != nil {
							logger.Get().Error("UpdateStatus err: "+err.Error(), "id", res.TaskId)
						}
						continue
					}
					t.ExecuteAt = time.Now().Add(s.RetryInterval)
					t.RetryCount++
					if err := s.retryTask(t, t.ExecuteAt, t.RetryCount); err != nil {
						logger.Get().Error("retryTask 失败", "error", err.Error())
					}
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
		if err := s.TimW.AddTask(t); err != nil {
			logger.Get().Error("advance AddTask error: "+err.Error(), "id", t.ID)
		}
	}

	tasts := s.Store.GetReadyTasks(s.Ctx)
	for _, t := range tasts {
		if err := s.TimW.AddTask(t); err != nil {
			logger.Get().Error("Recover AddTask error: "+err.Error(), "id", t.ID)
		}
	}
}

func (s *Scheduler) retryTask(t *model.Task, executeAt time.Time, retryCount int) error {
	err := s.Store.RequeueTask(s.Ctx, t.ID, model.StatusProcessing, model.StatusPending, executeAt, retryCount)
	if err != nil {
		return fmt.Errorf("retry task fail, RequeueTask err: %s", err)
	}

	if err = s.TimW.AddTask(t); err != nil {
		return fmt.Errorf("retry task fail, AddTask err: %s", err)
	}
	if err = s.Store.UpdateStatus(s.Ctx, t.ID, model.StatusPending, model.StatusReady); err != nil {
		return fmt.Errorf("retry task fail, updateStatus err: %s", err)
	}

	return nil
}
