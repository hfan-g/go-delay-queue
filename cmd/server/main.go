package main

import (
	"feng/delay-queue/internal/api"
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/scheduler"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
	"net/http"
	"time"
)

func main() {
	store := store.NewMemoryStore()
	sched := &scheduler.Scheduler{Store: store} // 先部分初始化

	// 定义回调
	callback := func(task wheel.ScheduleTask) {
		sched.HandleExpiredTask(task)
	}

	layers := []wheel.LayerConfig{
		{
			TickDuration: time.Second,
			TickCount:    60,
		},
		{
			TickDuration: time.Minute,
			TickCount:    60,
		},
		{
			TickDuration: time.Hour,
			TickCount:    24,
		},
	}
	tw := wheel.NewTimingWheel(layers, callback)
	sched.TimW = tw
	sched.Executor = executor.NewExecutor(10)

	// 启动时间轮
	sched.TimW.Start()

	// 启动执行器
	sched.Executor.Work()
	sched.Result()
	// 恢复数据
	sched.Recover()

	handels := api.NewHandel(sched)
	http.HandleFunc("/task/add", handels.AddTask)
	fmt.Print("service start!\n")
	http.ListenAndServe(":8088", nil)
}
