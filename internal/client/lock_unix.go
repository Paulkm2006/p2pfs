//go:build linux || darwin || freebsd

package client

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

func lockDataDir(dir string) (func() error, error) {
	f, e := os.OpenFile(filepath.Join(dir, ".instance.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("该数据目录已被其他客户端使用: %w", e)
	}
	return func() error {
		e := unix.Flock(int(f.Fd()), unix.LOCK_UN)
		closeErr := f.Close()
		if e != nil {
			return e
		}
		return closeErr
	}, nil
}
