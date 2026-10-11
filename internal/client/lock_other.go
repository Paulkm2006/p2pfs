//go:build !linux && !darwin && !freebsd && !windows

package client

import "errors"

func lockDataDir(string) (func() error, error) {
	return nil, errors.New("exclusive client directory locks unsupported on this platform")
}
