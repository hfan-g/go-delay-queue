package scheduler

import (
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
	"net/http"
)

type Scheduler struct {
	Store    store.Store
	Tw       *wheel.Wheel
	TimW     *wheel.TimingWheel
	Executor *executor.Executor
}

func (s *Scheduler) AddTask(t *model.Task) error {
	if err := s.Store.CreateTask(t); err != nil {
		return err
	}
	s.TimW.AddTask(t)

	return nil
}

func (s *Scheduler) HandleExpiredTask(task wheel.ScheduleTask) {
	id := task.GetID()

	fullTask, err := s.Store.GetTask(id)
	if err != nil {
		fmt.Printf("gettask fail ID: %s", id)
		return
	}

	fmt.Printf("Task %s status: %d\n", fullTask.ID, fullTask.Status)

	s.Executor.Sublimt(fullTask)
}

func (s *Scheduler) Result() {
	go func() {
		for {
			res := s.Executor.GetResult()
			if res.Code == http.StatusOK {
				fmt.Printf("执行成功 ID: %s \n", res.TaskId)
			} else {
				fmt.Printf("执行失败！ ID: %s \n", res.TaskId)
			}
		}
	}()
}
