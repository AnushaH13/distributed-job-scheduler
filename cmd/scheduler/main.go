package main

import (
	"fmt"
	"os"

	"github.com/anushah13/distributed-job-scheduler/internal/config"
)

func main() {
	cfg, err := config.Load(config.ServiceScheduler)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scheduler service: invalid configuration:\n%v\n", err)
		os.Exit(1)
	}

	fmt.Println("scheduler service: configuration loaded:", cfg.Summary())
}
