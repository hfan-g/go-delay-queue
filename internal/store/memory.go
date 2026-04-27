package store

import (
	"feng/delay-queue/internal/model"
)

type MemoryStore struct {
	tasks map[string]model.Task
}

func NewMemoryStore() Store {
	return &MemoryStore{
		tasks: make(map[string]model.Task),
	}
}

func (s *MemoryStore) AddTask(task model.Task) error {
	s.tasks[task.ID] = task
	return nil
}