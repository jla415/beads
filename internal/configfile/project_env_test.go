package configfile

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad_ProjectEnvAppliesAllowlistedValues(t *testing.T) {
	clearProjectEnvForTest(t)

	beadsDir := filepath.Join(t.TempDir(), ".beads")
	if err := os.MkdirAll(beadsDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	cfg := &Config{
		Database:       "dolt",
		DoltMode:       DoltModeServer,
		DoltServerHost: "metadata-host",
		DoltServerUser: "metadata-user",
		DoltDatabase:   "metadata-db",
	}
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("Save: %v", err)
	}

	writeProjectEnvFile(t, beadsDir, strings.Join([]string{
		"BEADS_DOLT_SERVER_HOST=env-host",
		"BEADS_DOLT_SERVER_USER=env-user",
		"BEADS_DOLT_SERVER_DATABASE=env-db",
		"BEADS_DOLT_PASSWORD=env-pass",
		"BEADS_DOLT_SERVER_TLS=true",
		"SOME_OTHER_KEY=ignored",
		"",
	}, "\n"))

	loaded, err := Load(beadsDir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("Load returned nil config")
	}

	if got := loaded.GetDoltServerHost(); got != "env-host" {
		t.Fatalf("GetDoltServerHost() = %q, want %q", got, "env-host")
	}
	if got := loaded.GetDoltServerUser(); got != "env-user" {
		t.Fatalf("GetDoltServerUser() = %q, want %q", got, "env-user")
	}
	if got := loaded.GetDoltDatabase(); got != "env-db" {
		t.Fatalf("GetDoltDatabase() = %q, want %q", got, "env-db")
	}
	if got := loaded.GetDoltServerPassword(); got != "env-pass" {
		t.Fatalf("GetDoltServerPassword() = %q, want %q", got, "env-pass")
	}
	if got := loaded.GetDoltServerTLS(); !got {
		t.Fatal("GetDoltServerTLS() = false, want true")
	}
	if got := os.Getenv("SOME_OTHER_KEY"); got != "" {
		t.Fatalf("SOME_OTHER_KEY should be ignored, got %q", got)
	}
}

func TestLoad_ProjectEnvShellEnvWins(t *testing.T) {
	clearProjectEnvForTest(t)

	beadsDir := filepath.Join(t.TempDir(), ".beads")
	if err := os.MkdirAll(beadsDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	cfg := &Config{Database: "dolt", DoltMode: DoltModeServer}
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("Save: %v", err)
	}

	writeProjectEnvFile(t, beadsDir, strings.Join([]string{
		"BEADS_DOLT_SERVER_HOST=env-host",
		"BEADS_DOLT_PASSWORD=env-pass",
		"",
	}, "\n"))

	t.Setenv("BEADS_DOLT_SERVER_HOST", "shell-host")
	t.Setenv("BEADS_DOLT_PASSWORD", "shell-pass")

	loaded, err := Load(beadsDir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if got := loaded.GetDoltServerHost(); got != "shell-host" {
		t.Fatalf("GetDoltServerHost() = %q, want %q", got, "shell-host")
	}
	if got := loaded.GetDoltServerPassword(); got != "shell-pass" {
		t.Fatalf("GetDoltServerPassword() = %q, want %q", got, "shell-pass")
	}
}

func TestLoad_ProjectEnvRedirectUsesTargetEnv(t *testing.T) {
	clearProjectEnvForTest(t)

	root := t.TempDir()
	sourceBeads := filepath.Join(root, "source", ".beads")
	targetBeads := filepath.Join(root, "target", ".beads")
	if err := os.MkdirAll(sourceBeads, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.MkdirAll(targetBeads, 0o750); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}

	cfg := &Config{
		Database:       "dolt",
		DoltMode:       DoltModeServer,
		DoltServerHost: "source-host",
		DoltDatabase:   "source-db",
	}
	if err := cfg.Save(sourceBeads); err != nil {
		t.Fatalf("Save: %v", err)
	}

	redirectTarget, err := filepath.Rel(filepath.Dir(sourceBeads), targetBeads)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceBeads, "redirect"), []byte(redirectTarget+"\n"), 0o600); err != nil {
		t.Fatalf("write redirect: %v", err)
	}

	writeProjectEnvFile(t, targetBeads, strings.Join([]string{
		"BEADS_DOLT_SERVER_HOST=target-host",
		"BEADS_DOLT_SERVER_DATABASE=target-db",
		"",
	}, "\n"))

	loaded, err := Load(sourceBeads)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.DoltServerHost != "source-host" {
		t.Fatalf("metadata host = %q, want %q", loaded.DoltServerHost, "source-host")
	}
	if got := loaded.GetDoltServerHost(); got != "target-host" {
		t.Fatalf("GetDoltServerHost() = %q, want %q", got, "target-host")
	}
	if got := loaded.GetDoltDatabase(); got != "target-db" {
		t.Fatalf("GetDoltDatabase() = %q, want %q", got, "target-db")
	}
}

func TestApplyProjectEnv_WarnsOnceOnParseFailure(t *testing.T) {
	clearProjectEnvForTest(t)

	beadsDir := filepath.Join(t.TempDir(), ".beads")
	if err := os.MkdirAll(beadsDir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeProjectEnvFile(t, beadsDir, "not valid env content\nstill bad\n")

	output := captureStderr(t, func() {
		ApplyProjectEnv(beadsDir)
		ApplyProjectEnv(beadsDir)
	})

	if count := strings.Count(output, "failed to load"); count != 1 {
		t.Fatalf("warning count = %d, want 1\noutput: %s", count, output)
	}
}

func writeProjectEnvFile(t *testing.T, beadsDir string, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(beadsDir, projectEnvFileName), []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func clearProjectEnvForTest(t *testing.T) {
	t.Helper()
	ResetProjectEnvCacheForTesting()

	saved := make(map[string]*string)
	for key := range projectEnvAllowedKeys {
		if value, ok := os.LookupEnv(key); ok {
			copy := value
			saved[key] = &copy
		} else {
			saved[key] = nil
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("Unsetenv(%s): %v", key, err)
		}
	}

	t.Cleanup(func() {
		ResetProjectEnvCacheForTesting()
		for key, value := range saved {
			if value == nil {
				_ = os.Unsetenv(key)
				continue
			}
			_ = os.Setenv(key, *value)
		}
	})
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w

	fn()

	_ = w.Close()
	os.Stderr = oldStderr

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	_ = r.Close()
	return string(data)
}
