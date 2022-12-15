//go:build !windows

package mlc

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/sys/unix"
)

// directoryLockGuard holds a lock on a directory and a pid file inside.  The pid file isn't part
// of the locking mechanism, it's just advisory.
type directoryLockGuard struct {
	// File handle on the directory, which we've flocked.
	d *os.File

	// File handle on the LOCK file, which we've FcntlFlocked.
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
	d, err := os.Open(dirPath)
	if err != nil {
		return nil, fmt.Errorf("cannot open directory %q: %w", dirPath, err)
	}

	opts := unix.LOCK_EX | unix.LOCK_NB
	err = unix.Flock(int(d.Fd()), opts)
	if err != nil {
		d.Close()
		return nil, fmt.Errorf("cannot acquire directory lock on %q: %w", dirPath, err)
	}

	var f *os.File
	switch {
	case fileExists(absPidFilePath):
		f, err = os.OpenFile(absPidFilePath, os.O_WRONLY, 0666)
	default:
		f, err = os.OpenFile(absPidFilePath, os.O_WRONLY|os.O_CREATE, 0666)
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("cannot open file %q: %w", absPidFilePath, err)
	}

	flockT := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: io.SeekStart,
	}
	err = unix.FcntlFlock(f.Fd(), unix.F_SETLK, &flockT)
	if err != nil {
		f.Close()
		d.Close()
		return nil, fmt.Errorf("cannot acquire file lock on %q: %w", absPidFilePath, err)
	}

	err = f.Truncate(0)
	_, seekErr := f.Seek(0, 0)
	if err != nil || seekErr != nil {
		f.Close()
		d.Close()
		return nil, fmt.Errorf("cannot truncate file lock on %q: truncate: %w; seek: %v", absPidFilePath, err, seekErr)
	}
	_, err = f.Write([]byte(leaderName + `|` + leaderAddr))
	if err != nil {
		f.Close()
		d.Close()
		return nil, fmt.Errorf("cannot write to lock file %q: %w", absPidFilePath, err)
	}
	err = f.Sync()
	if err != nil {
		f.Close()
		d.Close()
		return nil, fmt.Errorf("cannot sync lock file %q: %w", absPidFilePath, err)
	}
	return &directoryLockGuard{d, f, absPidFilePath}, nil
}

// Release deletes the pid file and releases our lock on the directory.
func (guard *directoryLockGuard) release() error {
	var errs error
	if err := guard.f.Close(); err != nil {
		errs = multierror.Append(errs, fmt.Errorf("failed to close lock file: %w", err))
	}
	if err := guard.d.Close(); err != nil {
		errs = multierror.Append(errs, fmt.Errorf("failed to close lockDir: %w", err))
	}
	guard.path = ""
	guard.d = nil
	guard.f = nil

	return errs
}
