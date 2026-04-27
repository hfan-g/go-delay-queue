package main

import (
	"feng/delay-queue/internal/api"
	"feng/delay-queue/internal/store"
	"fmt"
	"net/http"
)

func main() {

	store := store.NewMemoryStore()
	

	http.HandleFunc("/task/add", api.AddTask)
	fmt.Print("service start!")
	http.ListenAndServe(":8088", nil)
}
