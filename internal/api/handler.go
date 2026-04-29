package api

import (
	"encoding/json"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/scheduler"
	"net/http"
	"time"
)

type Handel struct {
	s *scheduler.Scheduler
}

type APIResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func NewHandel(s *scheduler.Scheduler) *Handel {
	return &Handel{
		s: s,
	}
}

func (h *Handel) AddTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonResponse(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}
	callbackUrl := r.FormValue("callback_url")
	payload := r.FormValue("payload")
	executeAtValue := r.FormValue("execute_at")
	if callbackUrl == "" || payload == "" || executeAtValue == "" {
		jsonResponse(w, http.StatusBadRequest, "请求参数错误", nil)
		return
	}

	executeAt, err := time.ParseInLocation("2006-01-02 15:04:05", executeAtValue, time.Local)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, "Invalid execute_at format", nil)
		return
	}

	task := model.Task{
		ID:          r.FormValue("id"),
		CallbackURL: callbackUrl,
		Payload:     payload,
		ExecuteAt:   executeAt,
		Status:      model.StatusPending,
		RetryCount:  0,
		MaxRetry:    3,
		CreatedAt:   time.Now(),
	}

	err = h.s.AddTask(&task)
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, "Failed to add task", nil)
		return
	}

	jsonResponse(w, http.StatusOK, "Task added successfully", map[string]string{"task": task.ID})
}

func jsonResponse(w http.ResponseWriter, code int, message string, data interface{}) {
	// 这里可以使用json.Marshal将data转换为JSON格式，并写入响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	response := APIResponse{
		Code:    code,
		Message: message,
		Data:    data,
	}

	json.NewEncoder(w).Encode(response)
}
