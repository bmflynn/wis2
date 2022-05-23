package internal

import (
	"context"
	"testing"
	"time"
)

func TestRunScript(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := RunScript(ctx, "/bin/ls", ".")
	if err != nil {
		t.Errorf("Should succeed with full path to binary, got %+v", err)
	}

	err = RunScript(ctx, "ls", ".")
	if err != nil {
		t.Errorf("Should succeed with just binary name, got %s", err)
	}

	err = RunScript(ctx, ".......I.dont.exist", "arg", "arg")
	if err == nil {
		t.Errorf("Should fail for binary that doesn't exist, got %v", err)
	}
}
