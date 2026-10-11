// Package testnet provides explicit network fixtures for integration tests. It
// never discovers host interfaces. The sockets, UDP, ICE, DTLS and SCTP remain
// real, so tests work in CI containers that disallow netlink enumeration.
package testnet

import (
	"github.com/pion/transport/v5"
	"github.com/pion/transport/v5/stdnet"
	"net"
)

type loopback struct{ *stdnet.Net }

// Loopback returns a network with a single explicitly specified loopback
// interface. Pass it to transport.WithNetwork only in controlled local tests.
func Loopback() transport.Net { return &loopback{Net: &stdnet.Net{}} }
func (n *loopback) Interfaces() ([]*transport.Interface, error) {
	i := transport.NewInterface(net.Interface{Index: 1, MTU: 65536, Name: "lo", Flags: net.FlagUp | net.FlagLoopback})
	i.AddAddress(&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)})
	return []*transport.Interface{i}, nil
}
func (n *loopback) InterfaceByIndex(index int) (*transport.Interface, error) {
	if index != 1 {
		return nil, transport.ErrInterfaceNotFound
	}
	interfaces, _ := n.Interfaces()
	return interfaces[0], nil
}
func (n *loopback) InterfaceByName(name string) (*transport.Interface, error) {
	if name != "lo" {
		return nil, transport.ErrInterfaceNotFound
	}
	interfaces, _ := n.Interfaces()
	return interfaces[0], nil
}
