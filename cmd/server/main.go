package main

import (
	"feng/delay-queue/internal/api"
	"fmt"
	"net/http"
)

func main() {

	http.HandleFunc("/task/add", api.AddTask)
	fmt.Print("service start!")
	http.ListenAndServe(":8088", nil)
}
