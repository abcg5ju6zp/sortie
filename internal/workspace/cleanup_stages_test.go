package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClassifyBeforeRemoveOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		hookErr *HookError
		sshHost string
		want    BeforeRemoveOutcomeKind
	}{
		{name: "nil error succeeds", hookErr: nil, want: BeforeRemoveSucceeded},
		{
			name:    "timeout",
			hookErr: &HookError{Op: "timeout", ExitCode: -1},
			want:    BeforeRemoveTimeout,
		},
		{
			name:    "nonzero exit is failure",
			hookErr: &HookError{Op: "run", ExitCode: 1},
			want:    BeforeRemoveFailed,
		},
		{
			name:    "ssh exit 255 with pinned host is host unreachable",
			hookErr: &HookError{Op: "run", ExitCode: 255},
			sshHost: "builder.example",
			want:    BeforeRemoveHostUnreachable,
		},
		{
			name:    "ssh exit 255 without pinned host is ordinary failure",
			hookErr: &HookError{Op: "run", ExitCode: 255},
			sshHost: "",
			want:    BeforeRemoveFailed,
		},
		{
			name:    "start failure with host is ordinary failure",
			hookErr: &HookError{Op: "start", ExitCode: -1},
			sshHost: "builder.example",
			want:    BeforeRemoveFailed,
		},
		{
			name:    "timeout wins over exit code even with host",
			hookErr: &HookError{Op: "timeout", ExitCode: -1},
			sshHost: "builder.example",
			want:    BeforeRemoveTimeout,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyBeforeRemoveOutcome(tc.hookErr, tc.sshHost); got != tc.want {
				t.Errorf("ClassifyBeforeRemoveOutcome() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestVerifyWorkspaceRemovable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "empty rejected", path: "", wantErr: true},
		{name: "relative rejected", path: "workspace/PROJ-1", wantErr: true},
		// Plain concatenation (not filepath.Join, which cleans the path).
		{name: "non canonical rejected", path: dir + "/a/../PROJ-1", wantErr: true},
		{name: "filesystem root rejected", path: string(os.PathSeparator), wantErr: true},
		{name: "absolute canonical accepted even when absent", path: filepath.Join(dir, "absent"), wantErr: false},
		{name: "existing directory accepted", path: dir, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := VerifyWorkspaceRemovable(tc.path)
			if (err != nil) != tc.wantErr {
				t.Errorf("VerifyWorkspaceRemovable(%q) err = %v, wantErr = %v", tc.path, err, tc.wantErr)
			}
		})
	}
}

func TestInspectWorkspace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	workspacePath := filepath.Join(dir, "PROJ-1")
	if err := os.Mkdir(workspacePath, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	filePath := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := InspectWorkspace(workspacePath)
	if err != nil || got != WorkspaceSafeDirectory {
		t.Fatalf("directory: got=%d err=%v", got, err)
	}
	got, err = InspectWorkspace(filePath)
	if err != nil || got != WorkspaceUnsafeEntry {
		t.Fatalf("plain file: got=%d err=%v", got, err)
	}
	got, err = InspectWorkspace(filepath.Join(dir, "absent"))
	if err != nil || got != WorkspaceAbsent {
		t.Fatalf("absent: got=%d err=%v", got, err)
	}
}

func TestRemoveWorkspaceTree(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "PROJ-1")
	if err := os.MkdirAll(filepath.Join(target, ".sortie"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, ".sortie", "scm.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := RemoveWorkspaceTree(target); err != nil {
		t.Fatalf("RemoveWorkspaceTree: %v", err)
	}
	if got, _ := InspectWorkspace(target); got != WorkspaceAbsent {
		t.Fatalf("after removal inspection = %d, want absent", got)
	}
	// Idempotent: removing an absent path is not an error.
	if err := RemoveWorkspaceTree(target); err != nil {
		t.Fatalf("RemoveWorkspaceTree absent: %v", err)
	}
}
