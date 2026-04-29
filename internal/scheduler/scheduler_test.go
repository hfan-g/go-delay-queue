package scheduler

import (
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
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
	w := wheel.NewWheel(1*time.Second, 60, callback)
	sched.Tw = w

	// 启动时间轮
	w.Start()

	// 7. 添加两个延迟任务
	now := time.Now()
	err := sched.AddTask(&model.Task{
		ID:        "task-1",
		Payload:   "hello-2s",
		ExecuteAt: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-1 failed: %v", err)
	}

	err = sched.AddTask(&model.Task{
		ID:        "task-2",
		Payload:   "hello-4s",
		ExecuteAt: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-2 failed: %v", err)
	}

	time.Sleep(6 * time.Second)
}

func TestScheduler_TimingWheelTasks(t *testing.T) {
	store := store.NewMemoryStore()
	sched := &Scheduler{Store: store} // 先部分初始化

	// 定义回调，抓住 sched
	callback := func(task wheel.ScheduleTask) {
		sched.HandleExpiredTask(task)
	}

	layers := []wheel.LayerConfig{
		{
			TickDuration: 10 * time.Millisecond,
			TickCount:    10,
		},
		{
			TickDuration: 100 * time.Millisecond,
			TickCount:    10,
		},
		{
			TickDuration: time.Second,
			TickCount:    10,
		},
	}

	// layers := []wheel.LayerConfig{
	// 	{
	// 		TickDuration: time.Second,
	// 		TickCount:    60,
	// 	},
	// 	{
	// 		TickDuration: time.Minute,
	// 		TickCount:    60,
	// 	},
	// 	{
	// 		TickDuration: time.Hour,
	// 		TickCount:    24,
	// 	},
	// }

	// 创建时间轮并传入回调
	tw := wheel.NewTimingWheel(layers, callback)
	sched.TimW = tw

	// 启动时间轮
	tw.Start()

	fmt.Println("start")

	// 7. 添加两个延迟任务
	now := time.Now()
	err := sched.AddTask(&model.Task{
		ID:        "task-1",
		Payload:   "hello-2s",
		ExecuteAt: now.Add(1 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-1 failed: %v", err)
	}

	err = sched.AddTask(&model.Task{
		ID:        "task-2",
		Payload:   "hello-4s",
		ExecuteAt: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-2 failed: %v", err)
	}

	err = sched.AddTask(&model.Task{
		ID:        "task-3",
		Payload:   "hello-3s",
		ExecuteAt: now.Add(20 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-3 failed: %v", err)
	}

	time.Sleep(10 * time.Second)
}

func TestScheduler_Executor(t *testing.T) {
	store := store.NewMemoryStore()
	sched := &Scheduler{Store: store} // 先部分初始化

	// 定义回调，抓住 sched
	callback := func(task wheel.ScheduleTask) {
		sched.HandleExpiredTask(task)
	}

	layers := []wheel.LayerConfig{
		{
			TickDuration: 10 * time.Millisecond,
			TickCount:    10,
		},
		{
			TickDuration: 100 * time.Millisecond,
			TickCount:    10,
		},
		{
			TickDuration: time.Second,
			TickCount:    10,
		},
	}
	tw := wheel.NewTimingWheel(layers, callback)
	sched.TimW = tw
    sched.Executor = executor.NewExecutor(10)

	// 启动时间轮
	tw.Start()

    //启动执行器
    sched.Executor.Work()
    sched.Result()

	now := time.Now()
	err := sched.AddTask(&model.Task{
		ID:        "task-1",
		Payload:   "hello-2s",
        CallbackURL: "http://127.0.0.1:9501/api/order/1/cancel",
		ExecuteAt: now.Add(1 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-1 failed: %v", err)
	}

	err = sched.AddTask(&model.Task{
		ID:        "task-2",
		Payload:   "hello-4s",
        CallbackURL: "http://127.0.0.1:9501/api/order/2/cancel",
		ExecuteAt: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-2 failed: %v", err)
	}

	err = sched.AddTask(&model.Task{
		ID:        "task-3",
		Payload:   "hello-3s",
        CallbackURL: "http://127.0.0.1:9501/api/order/3/cancel",
		ExecuteAt: now.Add(20 * time.Second),
	})
	if err != nil {
		t.Fatalf("add task-3 failed: %v", err)
	}

	time.Sleep(10 * time.Second)
}
