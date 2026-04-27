package wheel

import (
	"container/list"
	"sync"
	"time"
)

// 任务接口，让时间轮不直接依赖具体Task结构
type ScheduleTask interface {
	GetExecuteAt() time.Time
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
	addTaskChan	chan ScheduleTask
	wg	sync.WaitGroup
}


func NewWheel(tickDuration time.Duration, tickCount int) *Wheel {
	w := &Wheel{
		tickDuration: tickDuration,
		tickCount: tickCount,
		slots: make([]*slot, tickCount),
		currentPos: 0,
		addTaskChan: make(chan ScheduleTask, 1024),
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

func (w *Wheel) AddTask(task ScheduleTask) error {
	pos := w.getPosition(task.GetExecuteAt())
	if pos <= 0 {

	}

	w.slots[pos].tasks.PushBack(task)
	return nil
}

func (w *Wheel) Start() {
	ticker := time.NewTicker(w.tickDuration)
	w.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		for {
			select {
			case <-ticker.C:
				w.tick()
			case
				return
			}
		}
	}
}



