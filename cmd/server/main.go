package main

import (
	"context"
	"feng/delay-queue/internal/api"
	"feng/delay-queue/internal/config"
	"feng/delay-queue/internal/executor"
	"feng/delay-queue/internal/logger"
	"feng/delay-queue/internal/scheduler"
	"feng/delay-queue/internal/store"
	"feng/delay-queue/internal/wheel"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	if err := config.InitConfig(); err != nil {
		panic("初始化配置失败: " + err.Error())
	}
	cfg := config.GetConfig()

	logger.Init(
		cfg.Logger.Level,
		cfg.Logger.Path,
		cfg.Logger.MaxSize,
		cfg.Logger.MaxAge,
		cfg.Logger.MaxBackups,
	)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	store := store.NewRedisStore(&cfg.Redis)
	sched := &scheduler.Scheduler{
		Store:         store,
		Wg:            &wg,
		Ctx:           ctx,
		RetryInterval: cfg.Scheduler.RetryInterval,
	}

	// 定义回调
	callback := func(task wheel.ScheduleTask) {
		sched.HandleExpiredTask(task)
	}

	tw := wheel.NewTimingWheel(ctx, cfg.Wheel.Layers, callback)
	sched.TimW = tw
	sched.Executor = executor.NewExecutor(ctx, &cfg.Executor, &wg)

	// 启动时间轮
	sched.TimW.Start()

	// 启动执行器
	sched.Executor.Work()
	sched.Result()
	// 恢复数据
	sched.Recover()

	handels := api.NewHandel(sched)
	http.HandleFunc("/task/add", handels.AddTask)

	server := &http.Server{
		Addr:         cfg.HTTP.Addr,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}

	go func() {
		logger.Get().Info("service starting",
			"addr", cfg.HTTP.Addr,
		)
		if err := server.ListenAndServe(); err != nil {
			logger.Get().Error("service start fail!",
				"addr", cfg.HTTP.Addr,
				"error", err.Error(),
			)
		}
	}()

	//监听退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	logger.Get().Info("收到退出信号, 开始优雅退出...")
	cancel()
	wg.Wait()
	if err := store.Close(); err != nil {
		logger.Get().Error("关闭 Redis 连接失败")
	}
	logger.Get().Info("所有任务处理完毕，退出")
}
