package main

import (
	"feng/delay-queue/internal/api"
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/scheduler"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	var wg sync.WaitGroup
	store := store.NewMemoryStore()
	sched := &scheduler.Scheduler{Store: store, StopChan: make(chan struct{}), Wg: &wg} // 先部分初始化

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
	sched.Executor = executor.NewExecutor(10, &wg)

	// 启动时间轮
	sched.TimW.Start()

	// 启动执行器
	sched.Executor.Work()
	sched.Result()
	// 恢复数据
	sched.Recover()

	handels := api.NewHandel(sched)
	http.HandleFunc("/task/add", handels.AddTask)

	go func() {
		fmt.Print("service start!\n")
		http.ListenAndServe(":8088", nil)
	}()

	//监听退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	fmt.Printf("\n收到信号 %v，开始优雅退出...\n", sigCh)
	sched.Executor.Stop()
	sched.TimW.Stop()
	sched.Stop()
	wg.Wait()
	fmt.Println("所有任务处理完毕，退出")
}
