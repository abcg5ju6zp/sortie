package workspace

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/sortie-ai/sortie/internal/agent/sshutil"
)

// BeforeRemoveOutcomeKind classifies the outcome of one before_remove
// hook launch the way a resumable cleanup round needs it recorded. Its
// string values match the hook_state vocabulary persisted with a round.
type BeforeRemoveOutcomeKind string

const (
	// BeforeRemoveSucceeded: the hook exited with status 0.
	BeforeRemoveSucceeded BeforeRemoveOutcomeKind = "succeeded"
	// BeforeRemoveFailed: non-zero exit or a local launch failure. The
	// hook demonstrably ran (or could not start locally); it must never
	// be re-launched for the same round.
	BeforeRemoveFailed BeforeRemoveOutcomeKind = "failed"
	// BeforeRemoveTimeout: the hook timed out or was cancelled. Whether
	// the script reached its own end is unknowable, so it must never be
	// re-launched for the same round.
	BeforeRemoveTimeout BeforeRemoveOutcomeKind = "timeout"
	// BeforeRemoveHostUnreachable: the launch targeted a pinned SSH host
	// and failed with the exit status OpenSSH reserves for its own
	// connection failures. The hook was never delivered to the host, so
	// the round may launch it again after the host returns.
	BeforeRemoveHostUnreachable BeforeRemoveOutcomeKind = "host_unreachable"
)

// ClassifyBeforeRemoveOutcome turns a RunHook result into the
// before_remove outcome recorded by a cleanup round.
//
// hookErr nil is a success. A timeout is always BeforeRemoveTimeout. A
// non-zero exit is BeforeRemoveHostUnreachable only when sshHost is
// non-empty (the hook launch carried a remote target) and the exit
// status is the SSH connection-failure status; every other failure,
// including the same status for a local hook, is BeforeRemoveFailed.
func ClassifyBeforeRemoveOutcome(hookErr *HookError, sshHost string) BeforeRemoveOutcomeKind {
	if hookErr == nil {
		return BeforeRemoveSucceeded
	}
	if hookErr.Op == "timeout" {
		return BeforeRemoveTimeout
	}
	if sshHost != "" && hookErr.Op == "run" && hookErr.ExitCode >= 0 &&
		sshutil.ConnectionFailed(hookErr.ExitCode) {
		return BeforeRemoveHostUnreachable
	}
	return BeforeRemoveFailed
}

// verifyRemovablePath applies the well-formedness checks every direct
// workspace-tree removal must satisfy: the path is non-empty, absolute,
// in canonical form, and is not the filesystem root. It deliberately
// does not stat the path: callers classify a missing entry separately
// from an unsafe one.
func verifyRemovablePath(path string) error {
	if path == "" {
		return errors.New("workspace path must not be empty")
	}
	if !filepath.IsAbs(path) {
		return errors.New("workspace path must be absolute")
	}
	cleaned := filepath.Clean(path)
	if cleaned != path {
		return fmt.Errorf("workspace path must be in canonical form: got %q, cleaned to %q", path, cleaned)
	}
	if cleaned == "/" {
		return errors.New("refusing to remove filesystem root")
	}
	return nil
}

// VerifyWorkspaceRemovable reports whether path is a well-formed
// workspace tree path that a cleanup round is allowed to remove. The
// checks are deliberately path-only: a path that does not exist is
// well-formed and must be classified by the caller (typically as an
// already-verified-gone result) rather than rejected here.
func VerifyWorkspaceRemovable(path string) error {
	return verifyRemovablePath(path)
}

// WorkspaceExistence is the point-in-time filesystem classification of a
// pinned workspace path.
type WorkspaceExistence int

const (
	// WorkspaceAbsent: nothing exists at the path.
	WorkspaceAbsent WorkspaceExistence = iota
	// WorkspaceSafeDirectory: the path is a directory and not a symlink.
	WorkspaceSafeDirectory
	// WorkspaceUnsafeEntry: the path is a symlink or a non-directory
	// entry; removal and hook execution must both be refused.
	WorkspaceUnsafeEntry
)

// InspectWorkspace classifies the current filesystem entry at path
// without following symlinks. It does not perform well-formedness
// checks; pair it with [VerifyWorkspaceRemovable].
func InspectWorkspace(path string) (WorkspaceExistence, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return WorkspaceAbsent, nil
		}
		return WorkspaceAbsent, err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return WorkspaceUnsafeEntry, nil
	}
	if !fi.IsDir() {
		return WorkspaceUnsafeEntry, nil
	}
	return WorkspaceSafeDirectory, nil
}

// RemoveWorkspaceTree removes the directory tree at path. It performs
// no hook execution and no path verification itself: callers run
// [VerifyWorkspaceRemovable] and [InspectWorkspace] first, and remove
// only after the pinned before_remove hook stage is confirmed. Removal
// is idempotent; a later inspection reports [WorkspaceAbsent].
func RemoveWorkspaceTree(path string) error {
	return os.RemoveAll(path)
}

// runBeforeRemoveHook executes the before_remove hook with the same
// logging and process-group semantics Cleanup and CleanupByPath used
// before the resumable cleanup rounds existed, so every removal entry
// point reports the hook identically. A nil or empty script is a
// no-op; failure is logged at warn level like the other best-effort
// hooks.
func runBeforeRemoveHook(ctx context.Context, log *slog.Logger, path string, env map[string]string, script string, timeoutMS int) {
	if script == "" {
		return
	}
	log.InfoContext(ctx, "running hook", slog.String("hook", "before_remove"), slog.String("workspace", path))
	res, hookErr := RunHook(ctx, HookParams{
		Script:    script,
		Dir:       path,
		Env:       env,
		TimeoutMS: timeoutMS,
	})
	if hookErr != nil {
		var he *HookError
		errors.As(hookErr, &he)
		attrs := []slog.Attr{slog.String("hook", "before_remove"), slog.String("workspace", path), slog.Any("error", hookErr)}
		if he != nil && he.Output != "" {
			attrs = append(attrs, slog.String("hook_output", he.Output))
		}
		log.LogAttrs(ctx, slog.LevelWarn, "before_remove hook failed", attrs...)
		if he != nil {
			logHookLeftovers(ctx, log, "before_remove", path, he.TerminatedLeftovers)
		}
		return
	}
	logHookLeftovers(ctx, log, "before_remove", path, res.TerminatedLeftovers)
	if res.Output != "" {
		log.DebugContext(ctx, "hook completed", slog.String("hook", "before_remove"), slog.String("workspace", path), slog.String("hook_output", res.Output))
	}
}
