package scheduler

import (
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
)

type Scheduler struct {
	store 	store.Store
	tw  	*wheel.Wheel
	executor *executor.Executor
}

func (s *Scheduler) AddTask(t *model.Task) error {
	if err := s.store.CreateTask(t); err != nil {
		return err
	}

	if err := s.tw.AddTask(t); err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) onTaskExpired() error {
	tasks := s.store.GetReadyTasks()
	for _, task := range tasks {
		fmt.Printf("Task %s expired: %s\n", task.ID, task.Payload)
	}
	return nil
}