//go:build !windows

package mlc

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// directoryLockGuard holds a lock on a directory and a pid file inside.  The pid file isn't part
// of the locking mechanism, it's just advisory.
type directoryLockGuard struct {
	// File handle on the directory, which we've flocked.
	f *os.File
	// The absolute path to our pid file.
	path string
}

// acquireDirectoryLock gets a lock on the directory (using flock). If
// this is not read-only, it will also write our pid to
// dirPath/leaderName for convenience.
func acquireDirectoryLock(dirPath string, pidFileName, leaderName, leaderAddr string) (*directoryLockGuard, error) {
	// Convert to absolute path so that Release still works even if we do an unbalanced
	// chdir in the meantime.
	absPidFilePath, err := filepath.Abs(filepath.Join(dirPath, pidFileName))
	if err != nil {
		return nil, fmt.Errorf("cannot get absolute path for pid lock file: %w", err)
	}
	f, err := os.Open(dirPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open directory %q: %w", dirPath, err)
	}
	opts := unix.LOCK_EX | unix.LOCK_NB

	err = unix.Flock(int(f.Fd()), opts)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("cannot acquire directory lock on %q: %w", dirPath, err)
	}

	// Overwrite a pre-existing pid file. We're the
	// only process using this directory.
	err = ioutil.WriteFile(absPidFilePath, []byte(leaderName+`|`+leaderAddr), 0666)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("cannot write pid file %q: %w", absPidFilePath, err)
	}
	return &directoryLockGuard{f, absPidFilePath}, nil
}

// Release deletes the pid file and releases our lock on the directory.
func (guard *directoryLockGuard) release() error {
	var err error
	if closeErr := guard.f.Close(); err == nil {
		err = closeErr
	}
	guard.path = ""
	guard.f = nil

	return err
}

// openDir opens a directory for syncing.
func openDir(path string) (*os.File, error) { return os.Open(path) }

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return fmt.Errorf("error while opening directory: %s: %w", dir, err)
	}

	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return fmt.Errorf("error while syncing directory: %s: %w", dir, err)
	}
	return fmt.Errorf("error while closing directory: %s: %w", dir, closeErr)
}
