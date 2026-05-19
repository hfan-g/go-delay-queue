package store

import (
	"context"
	"feng/delay-queue/internal/config"
	"feng/delay-queue/internal/logger"
	"feng/delay-queue/internal/model"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	taskHashKeyPrefix   = "delay-queue:task:h:"
	TaskStatusKeyPrefix = "delay-queue:status:"
)

type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(cfg *config.RedisConfig) *RedisStore {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		panic("Redis 连接失败: " + err.Error())
	}
	logger.Get().Info("Redis 连接成功", "pong", pong)

	return &RedisStore{
		rdb: rdb,
	}
}

func (r *RedisStore) CreateTask(ctx context.Context, task *model.Task) error {
	script := redis.NewScript(`
		local key = KEYS[1]
		local statusKey = KEYS[2]
		if redis.call("HEXISTS", key, "id") == 1 then
			return 0
		end
		for i = 1, #ARGV, 2 do
			redis.call("HSET", key, ARGV[i], ARGV[i+1])
		end
		redis.call("SADD", statusKey, ARGV[2])
		return 1
	`)
	args := []interface{}{
		"id", task.ID,
		"callback_url", task.CallbackURL,
		"payload", task.Payload,
		"execute_at", task.ExecuteAt.Unix(),
		"retry_count", task.RetryCount,
		"max_retry", task.MaxRetry,
		"status", int(task.Status),
		"created_at", task.CreatedAt,
	}
	key := taskHashKey(task.ID)
	statusKey := taskStatusKey(int(task.Status))
	result, err := script.Run(ctx, r.rdb, []string{key, statusKey}, args...).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return fmt.Errorf("ID 已存在, CreateTask 失败")
	}
	return nil
}

func (r *RedisStore) GetTask(ctx context.Context, id string) (*model.Task, error) {
	key := taskHashKey(id)
	raw, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("task not found")
	}

	return parseTaskHash(raw)
}

func (r *RedisStore) GetReadyTasks(ctx context.Context) []*model.Task {
	key := taskStatusKey(int(model.StatusReady))
	ids, err := r.rdb.SMembers(ctx, key).Result()
	if err != nil {
		logger.Get().Error("redisStore GetReadyTasks smembers fail",
			"error", err.Error(),
		)
		return []*model.Task{}
	}

	if len(ids) == 0 {
		return []*model.Task{}
	}

	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))

	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, taskHashKey(id))
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return []*model.Task{}
	}

	var tasks []*model.Task
	for i, id := range ids {
		data, err := cmds[i].Result()
		if err != nil {
			logger.Get().Error("redisStore GetReadyTasks error",
				"id", id,
				"error", err.Error(),
			)
			continue
		}
		task, err := parseTaskHash(data)
		if err != nil {
			logger.Get().Error("redisStore parseTaskHash error",
				"id", id,
				"error", err.Error(),
			)
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks
}

func (r *RedisStore) GetProcessingTasks(ctx context.Context) []*model.Task {
	key := taskStatusKey(int(model.StatusProcessing))
	ids, err := r.rdb.SMembers(ctx, key).Result()
	if err != nil {
		logger.Get().Error("redisStore GetProcessingTasks smembers fail",
			"error", err.Error(),
		)
		return []*model.Task{}
	}

	if len(ids) == 0 {
		return []*model.Task{}
	}

	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))

	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, taskHashKey(id))
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return []*model.Task{}
	}

	var tasks []*model.Task
	for i, id := range ids {
		data, err := cmds[i].Result()
		if err != nil {
			logger.Get().Error("redisStore GetProcessingTasks error",
				"id", id,
				"error", err.Error(),
			)
			continue
		}
		task, err := parseTaskHash(data)
		if err != nil {
			logger.Get().Error("redisStore parseTaskHash error",
				"id", id,
				"error", err.Error(),
			)
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks
}
func (r *RedisStore) UpdateStatus(ctx context.Context, id string, oldStatus model.TaskStatus, newStatus model.TaskStatus) error {
	script := redis.NewScript(`
		local key = KEYS[1]
		local oldStatusKey = KEYS[2]
		local newStatusKey = KEYS[3]
		local id = ARGV[1]
		local oldStatus = ARGV[2]
		local newStatus = ARGV[3]
		local status = redis.call("HGET", key, "status")
		if status == oldStatus then
			redis.call("HSET", key, "status", newStatus)
			redis.call("SREM", oldStatusKey, id)
			redis.call("SADD", newStatusKey, id)
			return 1
		else
			return 0
		end
	`)

	// 执行脚本
	key := taskHashKey(id)
	oldStatusKey := taskStatusKey(int(oldStatus))
	newStatusKey := taskStatusKey(int(newStatus))
	result, err := script.Run(
		ctx,
		r.rdb,
		[]string{key, oldStatusKey, newStatusKey},
		id,
		strconv.Itoa(int(oldStatus)),
		strconv.Itoa(int(newStatus)),
	).Int()
	if err != nil {
		return fmt.Errorf("updateStatus error : %w", err)
	}

	if result == 1 {
		return nil
	} else {
		return fmt.Errorf("updateStatus error")
	}
}
func (r *RedisStore) RequeueTask(
	ctx context.Context,
	id string,
	oldStatus model.TaskStatus,
	newStatus model.TaskStatus,
	newExecAt time.Time,
	newRetryCount int,
) error {
	script := redis.NewScript(`
		local key = KEYS[1]
		local oldStatusKey = KEYS[2]
		local newStatusKey = KEYS[3]
		local id = ARGV[1]
		local oldStatus = ARGV[2]
		local newStatus = ARGV[3]
		local status = redis.call("HGET", key, "status")
		if status == oldStatus then
			redis.call("HSET", key, "status", newStatus, "execute_at", ARGV[4], "retry_count", ARGV[5])
			redis.call("SREM", oldStatusKey, id)
			redis.call("SADD", newStatusKey, id)
			return 1
		else
			return 0
		end
	`)

	// 执行脚本
	key := taskHashKey(id)
	oldStatusKey := taskStatusKey(int(oldStatus))
	newStatusKey := taskStatusKey(int(newStatus))
	result, err := script.Run(
		ctx,
		r.rdb,
		[]string{key, oldStatusKey, newStatusKey},
		id,
		strconv.Itoa(int(oldStatus)),
		strconv.Itoa(int(newStatus)),
		newExecAt.Unix(),
		newRetryCount,
	).Int()
	if err != nil {
		return fmt.Errorf("updateStatus error : %w", err)
	}

	if result == 1 {
		return nil
	} else {
		return fmt.Errorf("updateStatus error")
	}
}

func (r *RedisStore) Close() error {
	return r.rdb.Close()
}

func taskHashKey(id string) string {
	return taskHashKeyPrefix + id
}

func taskStatusKey(status int) string {
	return TaskStatusKeyPrefix + strconv.Itoa(status)
}

func parseTaskHash(raw map[string]string) (*model.Task, error) {
	var t model.Task
	var err error
	t.ID = raw["id"]
	t.CallbackURL = raw["callback_url"]
	t.Payload = raw["payload"]

	var sec int64
	if v, exists := raw["execute_at"]; exists {
		sec, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("execute_at invalid : %w", err)
		}
		t.ExecuteAt = time.Unix(sec, 0)
	}

	t.RetryCount, err = strconv.Atoi(raw["retry_count"])
	if err != nil {
		return nil, fmt.Errorf("retry_count invalid : %w", err)
	}

	t.MaxRetry, err = strconv.Atoi(raw["max_retry"])
	if err != nil {
		return nil, fmt.Errorf("max_retry invalid : %w", err)
	}

	s, err := strconv.Atoi(raw["status"])
	if err != nil {
		return nil, fmt.Errorf("status 格式错误: %w", err)
	}
	t.Status = model.TaskStatus(s)

	if v, exists := raw["created_at"]; exists {
		t.CreatedAt, err = time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return nil, fmt.Errorf("created_at invalid : %w", err)
		}
	}

	return &t, nil
}
