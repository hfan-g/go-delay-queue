package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"feng/delay-queue/internal/logger"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/scheduler"
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

	err = h.s.AddTask(r.Context(), &task)
	if err != nil {
		jsonResponse(w, http.StatusInternalServerError, "Failed to add task: "+err.Error(), nil)
		return
	}

	jsonResponse(w, http.StatusOK, "success", map[string]string{"task": task.ID})
}

func (h *Handel) GetTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonResponse(w, http.StatusMethodNotAllowed, "Method not allowed", nil)
		return
	}
	path := r.URL.Path
	id := strings.TrimPrefix(path, "/task/")
	if id == "" {
		jsonResponse(w, http.StatusBadRequest, "id 不能为空", nil)
		return
	}

	task, err := h.s.Store.GetTask(r.Context(), id)
	if err != nil {
		jsonResponse(w, http.StatusNotFound, "任务不存在", nil)
		return
	}
	jsonResponse(w, http.StatusOK, "success", task)
}

func jsonResponse(w http.ResponseWriter, code int, message string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	response := APIResponse{
		Code:    code,
		Message: message,
		Data:    data,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Get().Error("jsonResponse error: " + err.Error())
	}
}
