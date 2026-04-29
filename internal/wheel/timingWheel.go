package wheel

import (
	"fmt"
	"time"
)

type LayerConfig struct {
	TickDuration time.Duration
	TickCount    int
}

type TimingWheel struct {
	ticker        *time.Ticker
	wheelLayers   []*Wheel
	quit          chan struct{}
	onTaskExpired func(task ScheduleTask)
	addTaskChan   chan ScheduleTask
}

func NewTimingWheel(layers []LayerConfig, callback func(task ScheduleTask)) *TimingWheel {
	tw := &TimingWheel{
		quit:          make(chan struct{}),
		addTaskChan:   make(chan ScheduleTask, 1024),
		onTaskExpired: callback,
	}
	tw.ticker = time.NewTicker(layers[0].TickDuration)
	for _, layer := range layers {
		tw.wheelLayers = append(tw.wheelLayers, NewWheel(layer.TickDuration, layer.TickCount, callback))
	}
	return tw
}

func (tw *TimingWheel) AddTask(task ScheduleTask) error {
	tw.addTaskChan <- task
	return nil
}

func (tw *TimingWheel) Start() {
	go func() {
		for {
			select {
			case <-tw.ticker.C:
				tw.tick()
			case task := <-tw.addTaskChan:
				tw.addTask(task)
			case <-tw.quit:
				return
			}
		}
	}()

}

func (tw *TimingWheel) tick() {
	l0 := tw.wheelLayers[0]
	slot := l0.slots[l0.currentPos]
	for e := slot.tasks.Front(); e != nil; {
		task := e.Value.(ScheduleTask)
		l0.onTaskExpired(task)
		next := e.Next()
		slot.tasks.Remove(e)
		e = next
	}
	l0.currentPos = (l0.currentPos + 1) % l0.tickCount
	if l0.currentPos == 0 {
		tw.advance(1)
	}
}

func (tw *TimingWheel) advance(layerIndex int) {
	w := tw.wheelLayers[layerIndex]
	w.currentPos = (w.currentPos + 1) % w.tickCount
	slot := w.slots[w.currentPos]
	for e := slot.tasks.Front(); e != nil; {
		task := e.Value.(ScheduleTask)
		tw.AddTask(task)
		next := e.Next()
		slot.tasks.Remove(e)
		e = next
	}
	if w.currentPos == 0 && layerIndex+1 < len(tw.wheelLayers) {
		tw.advance(layerIndex + 1)
	}
}

func (tw *TimingWheel) addTask(task ScheduleTask) error {
	delay := time.Until(task.GetExecuteAt())
	fmt.Println("当前时间:", time.Now())
	fmt.Printf("add task ID: %s, delay: %s executeAt : %s\n", task.GetID(), delay, task.GetExecuteAt())
	t := time.Now()           // 本地时区（系统设置）
	fmt.Println(t.Location()) // 输出例如 "Local" 或 "Asia/Shanghai"
	if delay <= 0 {
		fmt.Printf("delay task ID: %s", task.GetID())
		tw.onTaskExpired(task)
		return nil
	}

	if delay >= tw.wheelLayers[len(tw.wheelLayers)-1].totalSpan() {
		return fmt.Errorf("task delay %v exceeds max span %v", delay, tw.wheelLayers[len(tw.wheelLayers)-1].totalSpan())
	}

	for _, wheel := range tw.wheelLayers {
		if delay < wheel.totalSpan() {
			steps := int(delay / wheel.tickDuration)
			pos := (wheel.currentPos + steps) % wheel.tickCount
			wheel.slots[pos].tasks.PushBack(task)
			return nil
		}
	}
	return nil
}
