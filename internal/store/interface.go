package store

import (
	"feng/delay-queue/internal/model"
	"time"
)

type Store interface {
	CreateTask(task *model.Task) error
	GetTask(id string) (*model.Task, error)
	GetReadyTasks() []*model.Task
	UpdateStatus(id string, oldStatus model.TaskStatus, newStatus model.TaskStatus) error
	RequeueTask(id string, oldStatus model.TaskStatus, newStatus model.TaskStatus, newExecAt time.Time, newRetryCount int) error
}