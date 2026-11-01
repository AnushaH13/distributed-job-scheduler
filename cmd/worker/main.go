package main

import (
	"fmt"
	"os"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

func main() {
	cfg, err := config.Load(config.ServiceWorker)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker service: invalid configuration:\n%v\n", err)
		os.Exit(1)
	}

	fmt.Println("worker service: configuration loaded:", cfg.Summary())
}
