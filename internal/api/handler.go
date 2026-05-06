package api

import (
	"encoding/json"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/scheduler"
	"net/http"
	"strconv"
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
	executeAtUnix, err := strconv.ParseInt(executeAtValue, 10, 64)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, "execute_at 必须是有效的整数时间戳", nil)
		return
	}
	if callbackUrl == "" || payload == "" {
		jsonResponse(w, http.StatusBadRequest, "请求参数错误", nil)
		return
	}

	executeAt := time.Unix(int64(executeAtUnix), 0)

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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	response := APIResponse{
		Code:    code,
		Message: message,
		Data:    data,
	}

	json.NewEncoder(w).Encode(response)
}
