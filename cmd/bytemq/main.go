package main

import (
	"fmt"
	"io"
	"os"

	"github.com/sharvesh/bytemq/internal/config"
)

func main() {
	os.Exit(run(os.Args, os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout io.Writer, stderr io.Writer) int {
	mode := config.ModeDev
	if len(args) > 1 {
		mode = config.RuntimeMode(args[1])
	}

	cfg := config.Default(mode)
	cfg.DatabaseURL = getenv("BYTEMQ_DATABASE_URL")
	cfg.WorkerID = getenv("BYTEMQ_WORKER_ID")

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid configuration for mode %q: %v\n", mode, err)
		return 1
	}

	fmt.Fprintf(stdout, "bytemq starting in %s mode\n", cfg.Mode)
	return 0
}
