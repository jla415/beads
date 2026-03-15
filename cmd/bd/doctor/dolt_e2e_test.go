//go:build cgo

package doctor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/dolt"
	"github.com/steveyegge/beads/internal/testutil"
)

// e2eDoctorResult mirrors the JSON output struct from cmd/bd/doctor.go.
// Kept minimal — only the fields we assert on.
type e2eDoctorResult struct {
	Path      string           `json:"path"`
	Checks    []e2eDoctorCheck `json:"checks"`
	OverallOK bool             `json:"overall_ok"`
}

type e2eDoctorCheck struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Message  string `json:"message"`
	Detail   string `json:"detail,omitempty"`
	Category string `json:"category,omitempty"`
}

// testBDPath holds the path to the bd binary built from the current branch.
// Built once via sync.Once to avoid redundant compilations across tests.
// testBDDir is cleaned up by TestMain after all tests complete.
var (
	testBDPath string
	testBDDir  string
	testBDOnce sync.Once
	testBDErr  error
)

// testSharedDB is the name of the shared database for branch-per-test isolation.
var testSharedDB string

// testSharedConn is a raw *sql.DB for branch operations in the shared database.
var testSharedConn *sql.DB

func TestMain(m *testing.M) {
	os.Exit(testMainInner(m))
}

func testMainInner(m *testing.M) int {
	os.Setenv("BEADS_TEST_MODE", "1")
	if err := testutil.EnsureDoltContainerForTestMain(); err != nil {
		fmt.Fprintf(os.Stderr, "WARN: %v, skipping Dolt tests\n", err)
	} else {
		defer testutil.TerminateDoltContainer()
		port := testutil.DoltContainerPortInt()

		// Set up shared database for branch-per-test isolation
		testSharedDB = "doctor_pkg_shared"
		db, err := testutil.SetupSharedTestDB(port, testSharedDB)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: shared DB setup failed: %v\n", err)
			return 1
		}
		testSharedConn = db
		defer db.Close()

		// Create schema + config on the shared DB and commit to main
		if err := initDoctorSharedSchema(port); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: shared schema init failed: %v\n", err)
			return 1
		}
	}

	code := m.Run()

	os.Unsetenv("BEADS_DOLT_PORT")
	os.Unsetenv("BEADS_TEST_MODE")
	if testBDDir != "" {
		os.RemoveAll(testBDDir)
	}
	return code
}

func initDoctorSharedSchema(port int) error {
	ctx := context.Background()
	cfg := &dolt.Config{
		Path:         "/tmp/doctor-shared-init",
		ServerHost:   "127.0.0.1",
		ServerPort:   port,
		Database:     testSharedDB,
		MaxOpenConns: 1,
	}
	store, err := dolt.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("New: %w", err)
	}
	defer store.Close()

	if err := store.SetConfig(ctx, "issue_prefix", "bd"); err != nil {
		return fmt.Errorf("SetConfig(issue_prefix): %w", err)
	}
	if err := store.SetConfig(ctx, "types.custom", "molecule,gate,convoy,merge-request,slot,agent,role,rig,event,message"); err != nil {
		return fmt.Errorf("SetConfig(types.custom): %w", err)
	}

	// Commit schema to main so branches get a clean snapshot
	db := store.DB()
	if _, err := db.ExecContext(ctx, "CALL DOLT_ADD('-A')"); err != nil {
		return fmt.Errorf("DOLT_ADD: %w", err)
	}
	if _, err := db.ExecContext(ctx, "CALL DOLT_COMMIT('--allow-empty', '-m', 'test: init shared schema')"); err != nil {
		return fmt.Errorf("DOLT_COMMIT: %w", err)
	}

	return nil
}

// buildTestBD compiles the bd binary from the current worktree. This ensures
// E2E tests run against the code in this branch, not the system-installed bd.
func buildTestBD(t *testing.T) string {
	t.Helper()

	testBDOnce.Do(func() {
		bdBinary := "bd-test"
		if runtime.GOOS == "windows" {
			bdBinary = "bd-test.exe"
		}

		dir, err := os.MkdirTemp("", "bd-dolt-e2e-*")
		if err != nil {
			testBDErr = err
			return
		}
		testBDDir = dir

		testBDPath = filepath.Join(dir, bdBinary)

		// Build from cmd/bd relative to the module root.
		// The test runs from cmd/bd/doctor/, so module root is ../../../
		cmd := exec.Command("go", "build", "-o", testBDPath, "./cmd/bd")

		// Find the module root by looking for go.mod
		modRoot := findModuleRoot(t)
		cmd.Dir = modRoot

		// Set CGO flags for ICU (required for Dolt backend)
		icuPrefix := icuPrefixPath()
		if icuPrefix != "" {
			cmd.Env = append(os.Environ(),
				"CGO_CFLAGS=-I"+icuPrefix+"/include",
				"CGO_CPPFLAGS=-I"+icuPrefix+"/include",
				"CGO_LDFLAGS=-L"+icuPrefix+"/lib",
			)
		} else {
			cmd.Env = os.Environ()
		}

		out, err := cmd.CombinedOutput()
		if err != nil {
			testBDErr = &buildError{output: string(out), err: err}
			return
		}
	})

	if testBDErr != nil {
		t.Skipf("skipping E2E test: failed to build bd binary: %v", testBDErr)
	}

	return testBDPath
}

type buildError struct {
	output string
	err    error
}

func (e *buildError) Error() string {
	return e.err.Error() + "\n" + e.output
}

// findModuleRoot walks up from the test's source directory to find go.mod.
func findModuleRoot(t *testing.T) string {
	t.Helper()

	// Start from the directory containing this test file.
	// runtime.Caller gives us the source file path at compile time.
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine test file location")
	}

	dir := filepath.Dir(filename)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find go.mod in any parent directory")
		}
		dir = parent
	}
}

// icuPrefixPath returns the ICU4C prefix from brew, or empty string if unavailable.
func icuPrefixPath() string {
	out, err := exec.Command("brew", "--prefix", "icu4c").Output()
	if err != nil {
		return ""
	}
	return string(out[:len(out)-1]) // trim trailing newline
}

// setupMinimalGitRepo creates a temp dir with a git repo and .beads/ directory.
// Doctor requires git context for workspace detection.
func setupMinimalGitRepo(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()

	for _, args := range [][]string{
		{"init"},
		{"config", "user.name", "test"},
		{"config", "user.email", "test@test.com"},
	} {
		cmd := exec.Command("git", append([]string{"-C", tmpDir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, out)
		}
	}

	beadsDir := filepath.Join(tmpDir, ".beads")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatalf("failed to create .beads dir: %v", err)
	}

	return tmpDir
}

// runBDDoctor executes `bd doctor <path> --json` using the branch-built binary
// and returns the parsed result, raw stdout, and any exec error. The JSON is on
// stdout even when exit code is 1 (doctor exits 1 when checks fail).
func runBDDoctor(t *testing.T, bdPath, path string) (e2eDoctorResult, string, error) {
	t.Helper()

	cmd := exec.Command(bdPath, "doctor", path, "--json")
	cmd.Env = os.Environ()

	out, execErr := cmd.Output()

	var result e2eDoctorResult
	if len(out) > 0 {
		if jsonErr := json.Unmarshal(out, &result); jsonErr != nil {
			t.Fatalf("failed to parse doctor JSON output: %v\nraw output: %s", jsonErr, out)
		}
	}

	return result, string(out), execErr
}

func TestE2E_DoctorServerHonorsProjectEnv(t *testing.T) {
	if testSharedDB == "" {
		t.Skip("shared Dolt test database not available")
	}

	bd := buildTestBD(t)
	repoPath := setupMinimalGitRepo(t)
	beadsDir := filepath.Join(repoPath, ".beads")

	cfg := &configfile.Config{
		Database:       "dolt",
		Backend:        configfile.BackendDolt,
		DoltMode:       configfile.DoltModeServer,
		DoltServerHost: "127.0.0.1",
		DoltDatabase:   "wrong_database",
	}
	if err := cfg.Save(beadsDir); err != nil {
		t.Fatalf("save metadata: %v", err)
	}

	envFile := strings.Join([]string{
		fmt.Sprintf("BEADS_DOLT_SERVER_PORT=%d", testutil.DoltContainerPortInt()),
		fmt.Sprintf("BEADS_DOLT_SERVER_DATABASE=%s", testSharedDB),
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(beadsDir, ".env"), []byte(envFile), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	cmd := exec.Command(bd, "doctor", repoPath, "--server", "--json")
	cmd.Env = filteredProjectEnvSubprocess(os.Environ())
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("bd doctor --server --json failed: %v", err)
	}

	var result e2eDoctorResult
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("failed to parse doctor JSON output: %v\nraw output: %s", err, out)
	}

	if !result.OverallOK {
		t.Fatalf("expected overall_ok=true, got false: %s", out)
	}

	for _, check := range result.Checks {
		if check.Name == "Server port" && strings.Contains(check.Message, "No Dolt server port configured") {
			t.Fatalf("expected .env to provide server port, got: %s", check.Message)
		}
	}
}

func filteredProjectEnvSubprocess(env []string) []string {
	filtered := make([]string, 0, len(env))
	for _, entry := range env {
		if strings.HasPrefix(entry, "BEADS_DOLT_") || strings.HasPrefix(entry, "DOLT_REMOTE_") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

// TestE2E_DoctorSQLiteBackend was removed: SQLite backend no longer exists.
// GetBackend() always returns "dolt" after the dolt-native cleanup (bd-yqpwy).

// TestE2E_DoctorDoltBackendNoDB was removed: the embedded Dolt driver
// auto-creates the database, so the "no DB" error scenario doesn't exist.
// (bd-yqpwy)
