//go:build windows

package client

import (
	"fmt"
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
)

func lockDataDir(dir string) (func() error, error) {
	f, e := os.OpenFile(filepath.Join(dir, ".instance.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	o := new(windows.Overlapped)
	if e = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, o); e != nil {
		f.Close()
		return nil, fmt.Errorf("该数据目录已被其他客户端使用: %w", e)
	}
	return func() error {
		e := windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, o)
		closeErr := f.Close()
		if e != nil {
			return e
		}
		return closeErr
	}, nil
}
