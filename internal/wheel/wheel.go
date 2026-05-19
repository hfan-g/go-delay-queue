package wheel

import (
	"time"
)

// 任务接口，让时间轮不直接依赖具体Task结构
type ScheduleTask interface {
	GetID() string
	GetExecuteAt() time.Time
	SetExecuteAt(time.Time)
}

// slot 时间轮的单个槽位
type slot struct {
	tasks []ScheduleTask
}

type Wheel struct {
	tickDuration time.Duration
	tickCount    int
	slots        []*slot
	currentPos   int
}

func NewWheel(tickDuration time.Duration, tickCount int) *Wheel {
	w := &Wheel{
		tickDuration: tickDuration,
		tickCount:    tickCount,
		slots:        make([]*slot, tickCount),
		currentPos:   0,
	}

	for i := 0; i < tickCount; i++ {
		w.slots[i] = &slot{tasks: []ScheduleTask{}}
	}
	return w
}

func (w *Wheel) totalSpan() time.Duration {
	return w.tickDuration * time.Duration(w.tickCount)
}
