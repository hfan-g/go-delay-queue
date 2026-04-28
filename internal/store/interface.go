package store

import (
	"feng/delay-queue/internal/model"
)

type Store interface {
	CreateTask(task *model.Task) error
	GetTask(id string) (*model.Task, error)
	GetReadyTasks() []*model.Task
}