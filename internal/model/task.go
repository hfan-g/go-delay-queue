package model

import "time"

type TaskStatus int

const (
    StatusPending    TaskStatus = iota // 等待加入调度
    StatusReady                         // 已加入时间轮，等待执行
    StatusProcessing                    // 执行中
    StatusSuccess                       // 成功
    StatusFailure                       // 失败(待重试)
    StatusDead                          // 重试耗尽，人工处理
)

type Task struct {
    ID          string      `json:"id"`
    CallbackURL string      `json:"callback_url"`
    Payload     string      `json:"payload"`      // 透传给PHP的参数
    ExecuteAt   time.Time   `json:"execute_at"`   // 期望执行时间
    RetryCount  int         `json:"retry_count"`  // 已重试次数
    MaxRetry    int         `json:"max_retry"`    // 最大重试次数
    Status      TaskStatus  `json:"status"`
    CreatedAt   time.Time   `json:"created_at"`
}

func (t *Task) GetExecuteAt() time.Time {
    return t.ExecuteAt
}

func (t *Task) SetExecuteAt(executeAt time.Time) {
    t.ExecuteAt = executeAt
}

func (t *Task) GetID() string {
    return t.ID
}