package main

import "testing"

func TestHTTPRequiresTLSOrExplicitLoopback(t *testing.T) {
	for _, tt := range []struct {
		addr, cert, key string
		insecure, ok    bool
	}{{"0.0.0.0:8080", "", "", false, false}, {"127.0.0.1:8080", "", "", false, false}, {"127.0.0.1:8080", "", "", true, true}, {"[::1]:8080", "", "", true, true}, {"localhost:8080", "", "", true, false}, {"0.0.0.0:8080", "", "", true, false}, {"0.0.0.0:8443", "cert.pem", "key.pem", false, true}} {
		if got := validateHTTP(tt.addr, tt.cert, tt.key, tt.insecure) == nil; got != tt.ok {
			t.Errorf("%+v: allowed=%v", tt, got)
		}
	}
}
