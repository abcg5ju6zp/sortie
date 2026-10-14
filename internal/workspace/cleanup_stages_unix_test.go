//go:build unix

package workspace

import (
	"os"
	"path/filepath"
	"testing"
)

// A pinned path that has become a symlink must classify unsafe without
// following the link, and RemoveWorkspaceTree must not be offered it by
// the cleanup engine (the engine gates on InspectWorkspace first).
func TestInspectWorkspace_SymlinkIsUnsafe(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	realDir := filepath.Join(dir, "real")
	if err := os.Mkdir(realDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	link := filepath.Join(dir, "PROJ-1")
	if err := os.Symlink(realDir, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	got, err := InspectWorkspace(link)
	if err != nil {
		t.Fatalf("InspectWorkspace symlink: %v", err)
	}
	if got != WorkspaceUnsafeEntry {
		t.Fatalf("symlink classification = %d, want unsafe", got)
	}
}
