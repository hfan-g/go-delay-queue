package store

import (
	"context"
	"time"

	"feng/delay-queue/internal/model"
)

type Store interface {
	CreateTask(ctx context.Context, task *model.Task) error
	GetTask(ctx context.Context, id string) (*model.Task, error)
	GetReadyTasks(ctx context.Context) []*model.Task
	GetProcessingTasks(ctx context.Context) []*model.Task
	UpdateStatus(ctx context.Context, id string, oldStatus model.TaskStatus, newStatus model.TaskStatus) error
	RequeueTask(ctx context.Context, id string, oldStatus model.TaskStatus, newStatus model.TaskStatus, newExecAt time.Time, newRetryCount int) error
	Close() error
}
