package api

import (
	"encoding/json"
	"feng/delay-queue/internal/model"
	"feng/delay-queue/internal/store"
	"fmt"
	"net/http"
	"time"
)

type APIResponse struct {
	Code    int                    `json:"code"`
	Message string                 `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func AddTask(w http.ResponseWriter, r *http.Request) {
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

	executeAt, err := time.Parse(time.RFC3339, executeAtValue)
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, "Invalid execute_at format", nil)
		return
	}

	fmt.Print(executeAt, "\n")

	task := model.Task{
		ID:          r.FormValue("id"),
		CallbackURL: callbackUrl,
		Payload:     payload,
		ExecuteAt:   executeAt,
		Status:      model.StatusPending,
	}

	fmt.Print(task, "\n")

	// 这里可以调用store.AddTask(task)将任务添加到存储中
	memoryStore := store.NewMemoryStore() // 这里使用内存存储，实际应用中应该使用持久化存储
	err = memoryStore.AddTask(task)
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
