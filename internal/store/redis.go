package store

import (
	"context"
	"feng/delay-queue/internal/model"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// 任务使用 HASH；前缀需与其它类型键区分（例如勿与 STRING 的 delay-queue:task:<id> 共用同一 key）。
const (
	taskHashKeyPrefix   = "delay-queue:task:h:"
	TaskStatusKeyPrefix = "delay-queue:status:"
)

type RedisStore struct {
	rdb *redis.Client
	ctx context.Context
}

func NewRedisStore() *RedisStore {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Redis 连接失败: %v", err))
	}
	fmt.Println("Redis 连接成功:", pong)

	return &RedisStore{
		rdb: rdb,
		ctx: ctx,
	}
}

func (r *RedisStore) CreateTask(task *model.Task) error {
	key := taskHashKey(task.ID)
	pipe := r.rdb.TxPipeline()
	pipe.HSet(r.ctx, key, map[string]any{
		"id":           task.ID,
		"callback_url": task.CallbackURL,
		"payload":      task.Payload,
		"execute_at":   task.ExecuteAt.Unix(),
		"retry_count":  task.RetryCount,
		"max_retry":    task.MaxRetry,
		"status":       int(task.Status),
		"created_at":   task.CreatedAt,
	})
	key = taskStatusKey(int(task.Status))
	pipe.SAdd(r.ctx, key, task.ID)

	_, err := pipe.Exec(r.ctx)
	return err
}

func (r *RedisStore) GetTask(id string) (*model.Task, error) {
	key := taskHashKey(id)
	raw, err := r.rdb.HGetAll(r.ctx, key).Result()
	if err != nil {
		return nil, err
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("task not found")
	}

	var t model.Task
	t.ID = raw["id"]
	t.CallbackURL = raw["callback_url"]
	t.Payload = raw["payload"]

	if v, exists := raw["execute_at"]; exists {
		sec, err := strconv.ParseInt(v, 10, 64)
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

func (r *RedisStore) GetReadyTasks() []*model.Task {
	key := taskStatusKey(int(model.StatusReady))
	ids, err:= r.rdb.SMembers(r.ctx, key).Result()
	if err != nil {

	}

	if len(ids) == 0 {
		return []*model.Task{}
	}
	for _, id := range ids {
		// key = 
	}

}
func (r *RedisStore) GetProcesingTasks() []*model.Task {

	return []*model.Task{}
}
func (r *RedisStore) UpdateStatus(id string, oldStatus model.TaskStatus, newStatus model.TaskStatus) error {
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
	result, err := script.Run(r.ctx, r.rdb, []string{key, oldStatusKey, newStatusKey}, id, strconv.Itoa(int(oldStatus)), strconv.Itoa(int(newStatus))).Result()
	if err != nil {
		return fmt.Errorf("updateStatus error : %w", err)
	}

	if result.(int64) == 1 {
		fmt.Printf("更新成功, 状态为 %d, id: %s\n", newStatus, id)
		return nil
	} else {
		fmt.Printf("状态不为 %d, 新状态 %d id: %s 未更新\n", oldStatus, newStatus, id)
		return fmt.Errorf("updateStatus error")
	}
}
func (r *RedisStore) RequeueTask(
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
		r.ctx,
		r.rdb,
		[]string{key, oldStatusKey, newStatusKey},
		id,
		strconv.Itoa(int(oldStatus)),
		strconv.Itoa(int(newStatus)),
		newExecAt.Unix(),
		newRetryCount,
	).Result()
	if err != nil {
		return fmt.Errorf("updateStatus error : %w", err)
	}

	if result.(int64) == 1 {
		fmt.Printf("更新成功, 状态为 %d, id: %s\n", newStatus, id)
		return nil
	} else {
		fmt.Printf("状态不为 %d, 新状态 %d id: %s 未更新\n", oldStatus, newStatus, id)
		return fmt.Errorf("updateStatus error")
	}
}

func taskHashKey(id string) string {
	return taskHashKeyPrefix + id
}

func taskStatusKey(status int) string {
	return TaskStatusKeyPrefix + strconv.Itoa(status)
}
