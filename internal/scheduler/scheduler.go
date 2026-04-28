package scheduler

import (
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
)

type Scheduler struct {
	Store store.Store
	Tw    *wheel.Wheel
}

func NewScheduler(s store.Store, tw *wheel.Wheel) *Scheduler {
	return &Scheduler{
		Store: s,
		Tw:    tw,
	}
}

func (s *Scheduler) AddTask(t *model.Task) error {
	if err := s.Store.CreateTask(t); err != nil {
		return err
	}
	s.Tw.AddTask(t)

	return nil
}

func (s *Scheduler) HandleExpiredTask(task wheel.ScheduleTask) {
	id := task.GetID()

	fullTask, err := s.Store.GetTask(id)
	if err != nil {
		fmt.Printf("gettask fail ID: %s", id)
		return
	}

	fmt.Printf("Task %s expired: %s\n", fullTask.ID, fullTask.ExecuteAt)
}
