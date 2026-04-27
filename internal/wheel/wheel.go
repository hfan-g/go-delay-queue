package wheel

import (
	"container/list"
	"time"
)

// 任务接口，让时间轮不直接依赖具体Task结构
type TaskInterface interface {
	ExecuteAt() time.Time
	SetExecuteAt(time.Time)
}

// slot 时间轮的单个槽位
type slot struct {
	tasks *list.List
}

type Wheel struct {
	tickDuration time.Duration
	tickCount	int
	slots 		[]*slot
	currentPos 	int
	addTaskChan	chan TaskInterface
}


func NewWheel(tickDuration time.Duration, tickCount int) *Wheel {
	w := &Wheel{
		tickDuration: tickDuration,
		tickCount: tickCount,
		slots: make([]*slot, tickCount),
		currentPos: 0,
		addTaskChan: make(chan TaskInterface, 1024),
	}

	for i := 0; i < tickCount; i++ {
		w.slots[i] = &slot{tasks: list.New()}
	}
	return w
}

// 计算任务应放在哪个槽位
func (w *Wheel) getPosition(executeAt time.Time) int {
	delay := time.Until(executeAt)
	// 圈数逻辑简化：只讨论单层轮，让多层轮去管长延时
	setps := int(delay / w.tickDuration)
	return (w.currentPos + setps) % w.tickCount
}





