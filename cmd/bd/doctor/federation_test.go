//go:build cgo

package doctor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProbeDoltOpen_NoDoltDir(t *testing.T) {
	tmpDir := t.TempDir()
	beadsDir := filepath.Join(tmpDir, ".beads")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write dolt backend config but do NOT create dolt/ directory.
	if err := os.WriteFile(filepath.Join(beadsDir, "metadata.json"),
		[]byte(`{"backend":"dolt"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if ProbeDoltOpen(tmpDir) {
		t.Error("ProbeDoltOpen should return false when dolt directory does not exist")
	}
}

func TestProbeDoltOpen_NoBeadsDir(t *testing.T) {
	tmpDir := t.TempDir()
	// No .beads/ at all — config cannot be loaded, defaults to dolt backend
	// but dolt/ directory doesn't exist.
	if ProbeDoltOpen(tmpDir) {
		t.Error("ProbeDoltOpen should return false when .beads directory does not exist")
	}
}

func TestProbeDoltOpen_EmptyDoltDir(t *testing.T) {
	tmpDir := t.TempDir()
	beadsDir := filepath.Join(tmpDir, ".beads")
	doltDir := filepath.Join(beadsDir, "dolt")
	if err := os.MkdirAll(doltDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(beadsDir, "metadata.json"),
		[]byte(`{"backend":"dolt"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	// Empty dolt/ directory — dolt.New() can initialize a fresh database here,
	// so ProbeDoltOpen should succeed. The probe's purpose is to catch the
	// driver panic (nil pointer in SetCrashOnFatalError), not to detect
	// missing databases.
	if !ProbeDoltOpen(tmpDir) {
		t.Error("ProbeDoltOpen should return true for empty dolt directory (fresh database creation)")
	}
}

func TestProbeDoltOpen_NonDoltBackend(t *testing.T) {
	tmpDir := t.TempDir()
	beadsDir := filepath.Join(tmpDir, ".beads")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// SQLite backend — federation checks are Dolt-only.
	if err := os.WriteFile(filepath.Join(beadsDir, "metadata.json"),
		[]byte(`{"backend":"sqlite"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if ProbeDoltOpen(tmpDir) {
		t.Error("ProbeDoltOpen should return false for non-Dolt backend")
	}
}
