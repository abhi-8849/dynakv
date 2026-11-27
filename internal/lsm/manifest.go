package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const manifestName = "MANIFEST"

// readManifest returns the live SSTable file names, oldest first. A missing manifest means an empty database.
func readManifest(dir string) ([]string, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lsm: read manifest: %w", err)
	}
	return strings.Fields(string(b)), nil
}

// writeManifest replaces the manifest atomically: a crash leaves either the old list or the new one.
func writeManifest(dir string, names []string) error {
	tmp := filepath.Join(dir, manifestName+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("lsm: write manifest: %w", err)
	}
	if _, err := f.WriteString(strings.Join(names, "\n") + "\n"); err != nil {
		f.Close()
		return fmt.Errorf("lsm: write manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("lsm: sync manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("lsm: close manifest: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		return fmt.Errorf("lsm: install manifest: %w", err)
	}
	return nil
}
