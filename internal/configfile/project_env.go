package configfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/subosito/gotenv"
)

const projectEnvFileName = ".env"

var projectEnvAllowedKeys = map[string]struct{}{
	"BEADS_DOLT_PASSWORD":        {},
	"BEADS_DOLT_PORT":            {},
	"BEADS_DOLT_REMOTESAPI_PORT": {},
	"BEADS_DOLT_SERVER_DATABASE": {},
	"BEADS_DOLT_SERVER_HOST":     {},
	"BEADS_DOLT_SERVER_MODE":     {},
	"BEADS_DOLT_SERVER_PORT":     {},
	"BEADS_DOLT_SERVER_TLS":      {},
	"BEADS_DOLT_SERVER_USER":     {},
	"DOLT_REMOTE_PASSWORD":       {},
	"DOLT_REMOTE_USER":           {},
}

type projectEnvStatus struct {
	applied bool
	failed  bool
}

var (
	projectEnvMu    sync.Mutex
	projectEnvCache = make(map[string]projectEnvStatus)
)

// ApplyProjectEnv loads allowlisted Dolt connection variables from
// .beads/.env, if present, without overriding non-empty process env vars.
func ApplyProjectEnv(beadsDir string) {
	envDir := resolveProjectEnvDir(beadsDir)
	if envDir == "" {
		return
	}

	envPath := filepath.Join(envDir, projectEnvFileName)
	if _, err := os.Stat(envPath); err != nil {
		return
	}

	projectEnvMu.Lock()
	if status, ok := projectEnvCache[envPath]; ok && (status.applied || status.failed) {
		projectEnvMu.Unlock()
		return
	}

	envVars, err := gotenv.Read(envPath)
	if err != nil {
		projectEnvCache[envPath] = projectEnvStatus{failed: true}
		projectEnvMu.Unlock()
		fmt.Fprintf(os.Stderr, "Warning: failed to load %s: %v\n", envPath, err)
		return
	}

	for key, val := range envVars {
		if _, allowed := projectEnvAllowedKeys[key]; !allowed {
			continue
		}
		if os.Getenv(key) != "" {
			continue
		}
		_ = os.Setenv(key, val)
	}

	projectEnvCache[envPath] = projectEnvStatus{applied: true}
	projectEnvMu.Unlock()
}

func resolveProjectEnvDir(beadsDir string) string {
	if strings.TrimSpace(beadsDir) == "" {
		return ""
	}

	resolved := canonicalProjectEnvPath(beadsDir)
	redirectFile := filepath.Join(resolved, "redirect")
	data, err := os.ReadFile(redirectFile)
	if err != nil {
		return resolved
	}

	target := parseProjectEnvRedirectTarget(data)
	if target == "" {
		return resolved
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(resolved), target)
	}
	target = canonicalProjectEnvPath(target)

	info, err := os.Stat(target)
	if err != nil || !info.IsDir() {
		return resolved
	}

	return target
}

func canonicalProjectEnvPath(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(path)
}

func parseProjectEnvRedirectTarget(data []byte) string {
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		return line
	}
	return ""
}

// ResetProjectEnvCacheForTesting clears memoized .beads/.env load state.
func ResetProjectEnvCacheForTesting() {
	projectEnvMu.Lock()
	projectEnvCache = make(map[string]projectEnvStatus)
	projectEnvMu.Unlock()
}
