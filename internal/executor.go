package internal

import (
	"context"
	"fmt"
	"os/exec"
)

type Executor func(ctx context.Context, name string, args ...string) error

func RunScript(ctx context.Context, name string, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no args")
	}
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.Run()
}
