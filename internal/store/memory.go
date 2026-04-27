package store

import (
	"feng/delay-queue/internal/model"
	"fmt"
	"time"
)

type MemoryStore struct {
	tasks map[string]model.Task
}

func NewMemoryStore() Store {
	return &MemoryStore{
		tasks: make(map[string]model.Task),
	}
}

func (s *MemoryStore) CreateTask(task *model.Task) error {
	s.tasks[task.ID] = *task
	return nil
}

func (s *MemoryStore) GetReadyTasks() []*model.Task {
	var readyTasks []*model.Task
	for id, task := range s.tasks {
		fmt.Printf("id: %s \n", id)
		if !task.ExecuteAt.After(time.Now()) {
			readyTasks = append(readyTasks, &task)
		}
	}
	return readyTasks
}