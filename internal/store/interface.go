package store

import (
	"feng/delay-queue/internal/model"
)

type Store interface {
	AddTask(task model.Task) error
}