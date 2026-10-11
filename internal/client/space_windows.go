//go:build windows

package client

import "golang.org/x/sys/windows"

func availableBytes(path string) (uint64, error) {
	p, e := windows.UTF16PtrFromString(path)
	if e != nil {
		return 0, e
	}
	var free, total, all uint64
	e = windows.GetDiskFreeSpaceEx(p, &free, &total, &all)
	return free, e
}
