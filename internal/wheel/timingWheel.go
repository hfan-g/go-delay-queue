package wheel

import (
	"context"
	"fmt"
	"time"
)

type LayerConfig struct {
	TickDuration time.Duration `yaml:"tick_duration"`
	TickCount    int           `yaml:"tick_count"`
}

type TimingWheel struct {
	ticker        *time.Ticker
	wheelLayers   []*Wheel
	onTaskExpired func(task ScheduleTask)
	addTaskChan   chan ScheduleTask
	ctx context.Context
}

func NewTimingWheel(ctx context.Context, layers []LayerConfig, callback func(task ScheduleTask)) *TimingWheel {
	tw := &TimingWheel{
		addTaskChan:   make(chan ScheduleTask, 1024),
		onTaskExpired: callback,
		ctx: ctx,
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
			case <-tw.ctx.Done():
				close(tw.addTaskChan)
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
	if delay <= 0 {
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
