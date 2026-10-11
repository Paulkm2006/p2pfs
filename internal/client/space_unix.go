//go:build linux || darwin || freebsd

package client

import "golang.org/x/sys/unix"

func availableBytes(path string) (uint64, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return 0, err
	}
	return s.Bavail * uint64(s.Bsize), nil
}
