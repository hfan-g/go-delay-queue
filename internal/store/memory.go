package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"feng/delay-queue/internal/model"
)

type MemoryStore struct {
	tasks map[string]model.Task
	mu    sync.Mutex
}

func NewMemoryStore() Store {
	return &MemoryStore{
		tasks: make(map[string]model.Task),
	}
}

func (s *MemoryStore) CreateTask(ctx context.Context, task *model.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID] = *task
	return nil
}

func (s *MemoryStore) GetReadyTasks(ctx context.Context) []*model.Task {
	return []*model.Task{} // 内存无法持久化
}

func (s *MemoryStore) GetProcessingTasks(ctx context.Context) []*model.Task {
	return []*model.Task{} // 内存无法持久化
}

func (s *MemoryStore) GetTask(ctx context.Context, id string) (*model.Task, error) {
	task, ok := s.tasks[id]
	if !ok {
		return nil, fmt.Errorf("task not found")
	}
	return &task, nil
}

func (s *MemoryStore) UpdateStatus(ctx context.Context, id string, oldStatus model.TaskStatus, newStatus model.TaskStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("task not found")
	}
	if task.Status != oldStatus {
		return fmt.Errorf("task status fail, ID: %s, status: %d, newStatus: %d", task.ID, task.Status, newStatus)
	}
	task.Status = newStatus
	s.tasks[id] = task

	return nil
}

func (s *MemoryStore) RequeueTask(
	ctx context.Context,
	id string,
	oldStatus model.TaskStatus,
	newStatus model.TaskStatus,
	newExecAt time.Time,
	newRetryCount int,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[id]
	if !ok {
		return fmt.Errorf("task not found")
	}
	if task.Status != oldStatus {
		return fmt.Errorf("task status fail, ID: %s, status: %d", task.ID, task.Status)
	}
	task.Status = newStatus
	task.ExecuteAt = newExecAt
	task.RetryCount = newRetryCount
	s.tasks[id] = task

	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}
