// Package remote implements a FakeDNS engine backed by a single remote allocator.
package remote

import (
	"context"
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/app/dns/fakedns/command"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/dns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type pools struct {
	ranges []netip.Prefix
}

// Engine shares mappings, not a replica or a cache: reverse lookups always reach
// the authority, so an evicted address cannot retain a stale local domain.
type Engine struct {
	client    command.FakeDNSServiceClient
	conn      *grpc.ClientConn
	ctx       context.Context
	cancel    context.CancelFunc
	timeout   time.Duration
	pools     atomic.Pointer[pools]
	closeOnce sync.Once
	closeErr  error
}

var _ dns.FakeDNSEngineRev0 = (*Engine)(nil)
var _ dns.FakeDNSPoolProvider = (*Engine)(nil)

// New connects directly to a trusted API endpoint. Protect nonlocal API access
// with an authenticated transport, just as for the other Xray control services.
// No traffic is sent until Start; the connection is shared by all mapping calls.
func New(ctx context.Context, address string, timeout uint32) (*Engine, error) {
	if address == "" {
		return nil, fmt.Errorf("remote FakeDNS requires an address")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	if timeout == 0 {
		timeout = 2000
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Engine{
		client: command.NewFakeDNSServiceClient(conn), conn: conn,
		ctx: ctx, cancel: cancel, timeout: time.Duration(timeout) * time.Millisecond,
	}, nil
}

func (*Engine) Type() interface{} { return (*dns.FakeDNSEngine)(nil) }

func (e *Engine) Start() error {
	ctx, cancel := context.WithTimeout(e.ctx, e.timeout)
	defer cancel()
	response, err := e.client.GetPools(ctx, &command.GetPoolsRequest{})
	if err != nil {
		return fmt.Errorf("get remote FakeDNS pools: %w", err)
	}
	p := &pools{}
	for _, cidr := range response.IpPools {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil || prefix.Addr().Is4In6() {
			return fmt.Errorf("invalid remote FakeDNS pool: %q", cidr)
		}
		p.ranges = append(p.ranges, prefix.Masked())
	}
	if len(p.ranges) == 0 {
		return fmt.Errorf("remote FakeDNS has no pools")
	}
	e.pools.Store(p)
	return nil
}

func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		e.cancel()
		e.closeErr = e.conn.Close()
	})
	return e.closeErr
}

func (e *Engine) GetFakeIPRanges() []string {
	var ranges []string
	if p := e.pools.Load(); p != nil {
		for _, prefix := range p.ranges {
			ranges = append(ranges, prefix.String())
		}
	}
	return ranges
}

func (e *Engine) IsIPInIPPool(ip net.Address) bool {
	if ip == nil || !ip.Family().IsIP() {
		return false
	}
	addr, ok := netip.AddrFromSlice(ip.IP())
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if p := e.pools.Load(); p != nil {
		for _, prefix := range p.ranges {
			if prefix.Contains(addr) {
				return true
			}
		}
	}
	return false
}

func (e *Engine) GetFakeIPForDomain(domain string) []net.Address {
	return e.GetFakeIPForDomain3(domain, true, true)
}

func (e *Engine) GetFakeIPForDomain3(domain string, ipv4, ipv6 bool) []net.Address {
	if e.pools.Load() == nil || (!ipv4 && !ipv6) {
		return nil
	}
	ctx, cancel := context.WithTimeout(e.ctx, e.timeout)
	defer cancel()
	response, err := e.client.Allocate(ctx, &command.AllocateRequest{Domain: domain, Ipv4: ipv4, Ipv6: ipv6})
	if err != nil {
		return nil
	}
	var ips []net.Address
	for _, raw := range response.Ips {
		if len(raw) != 4 && len(raw) != 16 {
			return nil
		}
		ip := net.IPAddress(raw)
		if !e.IsIPInIPPool(ip) || (ip.Family().IsIPv4() && !ipv4) || (ip.Family().IsIPv6() && !ipv6) {
			return nil
		}
		ips = append(ips, ip)
	}
	return ips
}

func (e *Engine) GetDomainFromFakeDNS(ip net.Address) string {
	if !e.IsIPInIPPool(ip) {
		return ""
	}
	ctx, cancel := context.WithTimeout(e.ctx, e.timeout)
	defer cancel()
	response, err := e.client.ReverseLookup(ctx, &command.ReverseLookupRequest{Ip: ip.IP()})
	if err != nil {
		return ""
	}
	return response.Domain
}
