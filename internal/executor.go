package internal

import (
	"context"
	"os/exec"
)

type Executor func(ctx context.Context, args ...string) error

func RunScript(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, args[0], args...)
	return cmd.Run()
}
