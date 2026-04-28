package scheduler

import (
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"testing"
	"time"
)

func TestScheduler_WaitForExpiredTasks(t *testing.T) {
	store := store.NewMemoryStore()
	sched := &Scheduler{Store: store} // 先部分初始化

	// 定义回调，抓住 sched
	callback := func(task wheel.ScheduleTask) {
		sched.HandleExpiredTask(task)
	}

	// 创建时间轮并传入回调
	w := wheel.NewWheel(1 * time.Second, 60, callback)
	sched.Tw = w

	// 启动时间轮
	w.Start()

    // 7. 添加两个延迟任务
    now := time.Now()
    err := sched.AddTask(&model.Task{
        ID:          "task-1",
        Payload:     "hello-2s",
        ExecuteAt:   now.Add(2 * time.Second),
    })
    if err != nil {
        t.Fatalf("add task-1 failed: %v", err)
    }

    err = sched.AddTask(&model.Task{
        ID:          "task-2",
        Payload:     "hello-4s",
        ExecuteAt:   now.Add(4 * time.Second),
    })
    if err != nil {
        t.Fatalf("add task-2 failed: %v", err)
    }

    time.Sleep(6 * time.Second)
}
