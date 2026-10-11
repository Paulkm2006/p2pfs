//go:build !linux && !darwin && !freebsd && !windows

package client

import "errors"

func availableBytes(path string) (uint64, error) {
	return 0, errors.New("disk-space checks unsupported on this platform")
}
