package evidence

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const ChecksumsFile = "SHA256SUMS"

func WriteBytes(path string, body []byte) error {
	return writeAtomic(path, func(writer io.Writer) error {
		_, err := writer.Write(body)
		return err
	})
}

func WriteJSON(path string, value any) error {
	return writeAtomic(path, func(writer io.Writer) error {
		encoder := json.NewEncoder(writer)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	})
}

func ReadJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON file contains more than one value")
		}
		return err
	}
	return nil
}

func WriteJSONLines[T any](path string, records []T) error {
	return writeAtomic(path, func(writer io.Writer) error {
		encoder := json.NewEncoder(writer)
		encoder.SetEscapeHTML(false)
		for _, record := range records {
			if err := encoder.Encode(record); err != nil {
				return err
			}
		}
		return nil
	})
}

func ReadJSONLines[T any](path string) ([]T, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	decoder := json.NewDecoder(file)
	var records []T
	for {
		var record T
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
}

func GenerateChecksums(root string) error {
	artifacts, err := regularArtifacts(root)
	if err != nil {
		return err
	}

	return writeAtomic(filepath.Join(root, ChecksumsFile), func(writer io.Writer) error {
		for _, relative := range artifacts {
			digest, err := FileSHA256(filepath.Join(root, filepath.FromSlash(relative)))
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(writer, "%s  %s\n", digest, relative); err != nil {
				return err
			}
		}
		return nil
	})
}

func VerifyChecksums(root string) error {
	checksumPath := filepath.Join(root, ChecksumsFile)
	file, err := os.Open(checksumPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	listed := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < sha256.Size*2+3 || line[sha256.Size*2:sha256.Size*2+2] != "  " {
			return fmt.Errorf("invalid checksum line %q", line)
		}
		digest := line[:sha256.Size*2]
		if _, err := hex.DecodeString(digest); err != nil {
			return fmt.Errorf("invalid checksum digest for %q: %w", line, err)
		}
		relative := line[sha256.Size*2+2:]
		if err := validateRelativeArtifact(relative); err != nil {
			return err
		}
		if _, duplicate := listed[relative]; duplicate {
			return fmt.Errorf("duplicate checksum entry %q", relative)
		}
		listed[relative] = struct{}{}

		actual, err := FileSHA256(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			return fmt.Errorf("verify %s: %w", relative, err)
		}
		if actual != strings.ToLower(digest) {
			return fmt.Errorf("checksum mismatch for %s", relative)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	artifacts, err := regularArtifacts(root)
	if err != nil {
		return err
	}
	for _, relative := range artifacts {
		if _, ok := listed[relative]; !ok {
			return fmt.Errorf("artifact is not checksummed: %s", relative)
		}
	}
	if len(listed) != len(artifacts) {
		return errors.New("checksum list contains missing artifacts")
	}
	return nil
}

func FileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = file.Close()
	}()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func SQLiteSnapshotDigests(path string) (map[string]string, error) {
	components := []struct {
		name string
		path string
	}{
		{name: "database", path: path},
		{name: "wal", path: path + "-wal"},
		{name: "shm", path: path + "-shm"},
	}
	digests := make(map[string]string)
	for index, component := range components {
		info, err := os.Stat(component.path)
		switch {
		case err == nil:
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("SQLite snapshot component is not a regular file: %s", component.path)
			}
		case errors.Is(err, os.ErrNotExist) && index > 0:
			continue
		case err != nil:
			return nil, err
		}
		digest, err := FileSHA256(component.path)
		if err != nil {
			return nil, err
		}
		digests[component.name] = digest
	}
	return digests, nil
}

func regularArtifacts(root string) ([]string, error) {
	var artifacts []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact tree contains symlink: %s", path)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("artifact tree contains non-regular file: %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == ChecksumsFile {
			return nil
		}
		if err := validateRelativeArtifact(relative); err != nil {
			return err
		}
		artifacts = append(artifacts, relative)
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(artifacts)
	return artifacts, nil
}

func validateRelativeArtifact(path string) error {
	if path == "" || path == "." || filepath.IsAbs(path) || strings.Contains(path, `\`) {
		return fmt.Errorf("invalid artifact path %q", path)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	if clean != path || path == ".." || strings.HasPrefix(path, "../") {
		return fmt.Errorf("unsafe artifact path %q", path)
	}
	return nil
}

func writeAtomic(path string, write func(io.Writer) error) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()

	if err := write(temporary); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temporaryPath, 0o644); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
