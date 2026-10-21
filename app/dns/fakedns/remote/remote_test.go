package remote_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/commander"
	"github.com/xtls/xray-core/app/dns/fakedns"
	"github.com/xtls/xray-core/app/dns/fakedns/command"
	"github.com/xtls/xray-core/app/dns/fakedns/remote"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func authority(t *testing.T) (string, dns.FakeDNSEngineRev0, func()) {
	t.Helper()
	s, err := core.New(&core.Config{App: []*serial.TypedMessage{serial.ToTypedMessage(&fakedns.FakeDnsPoolMulti{Pools: []*fakedns.FakeDnsPool{
		{IpPool: "198.18.0.0/24", LruSize: 4}, {IpPool: "2001:2::/120", LruSize: 4},
	}})}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	object, err := core.CreateObject(s, &command.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	object.(commander.Service).Register(server)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve(l)
	t.Cleanup(server.Stop)
	return l.Addr().String(), s.GetFeature((*dns.FakeDNSEngine)(nil)).(dns.FakeDNSEngineRev0), server.Stop
}

func client(t *testing.T, address string) *remote.Engine {
	t.Helper()
	c, err := remote.New(context.Background(), address, 500)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestMappingSharedWithLocalAndIndependentCores(t *testing.T) {
	address, local, _ := authority(t)
	a, b := client(t, address), client(t, address)
	ips := a.GetFakeIPForDomain("shared.example")
	if len(ips) != 2 {
		t.Fatal(ips)
	}
	for i, ip := range ips {
		if b.GetDomainFromFakeDNS(ip) != "shared.example" || local.GetDomainFromFakeDNS(ip) != "shared.example" {
			t.Fatal("mapping not shared")
		}
		if got := b.GetFakeIPForDomain("shared.example"); got[i].String() != ip.String() {
			t.Fatal("allocators diverged")
		}
	}
	if len(a.GetFakeIPForDomain3("v4.example", true, false)) != 1 || len(a.GetFakeIPForDomain3("v6.example", false, true)) != 1 {
		t.Fatal("family selection ignored")
	}
	if got := a.GetFakeIPForDomain3("none.example", false, false); len(got) != 0 {
		t.Fatal(got)
	}
	// Replacing a consumer does not retire or reinitialize the authoritative LRU.
	a.Close()
	replacement := client(t, address)
	for _, ip := range ips {
		if replacement.GetDomainFromFakeDNS(ip) != "shared.example" {
			t.Fatal("consumer restart lost mapping")
		}
	}
	// No reverse cache: an evicted mapping disappears from all clients.
	for i := 0; i < 8; i++ {
		local.GetFakeIPForDomain(fmt.Sprintf("evict%d.example", i))
	}
	for _, ip := range ips {
		if local.GetDomainFromFakeDNS(ip) == "shared.example" || b.GetDomainFromFakeDNS(ip) == "shared.example" {
			t.Fatal("stale reverse mapping")
		}
	}
}

func TestRemoteConfiguredAsCoreFeature(t *testing.T) {
	address, local, _ := authority(t)
	s, err := core.New(&core.Config{App: []*serial.TypedMessage{serial.ToTypedMessage(&fakedns.FakeDnsPoolMulti{Remote: &fakedns.RemoteConfig{Address: address, Timeout: 500}})}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	engine := s.GetFeature((*dns.FakeDNSEngine)(nil)).(dns.FakeDNSEngineRev0)
	ips := engine.GetFakeIPForDomain("embedded.example")
	if len(ips) != 2 || local.GetDomainFromFakeDNS(ips[0]) != "embedded.example" {
		t.Fatal(ips)
	}
	_, err = core.New(&core.Config{App: []*serial.TypedMessage{serial.ToTypedMessage(&fakedns.FakeDnsPoolMulti{Remote: &fakedns.RemoteConfig{Address: address}, Pools: []*fakedns.FakeDnsPool{{IpPool: "198.18.0.0/24", LruSize: 4}}})}})
	if err == nil {
		t.Fatal("accepted local and remote allocators together")
	}
}

func TestRemoteConcurrentAllocation(t *testing.T) {
	address, _, _ := authority(t)
	a, b := client(t, address), client(t, address)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			for j := 0; j < 30; j++ {
				ips := a.GetFakeIPForDomain("concurrent.example")
				if len(ips) != 2 {
					t.Error(ips)
					return
				}
				if b.GetDomainFromFakeDNS(ips[0]) != "concurrent.example" {
					t.Error("mapping lost")
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestRemoteFailureDoesNotAllocateLocally(t *testing.T) {
	address, _, stop := authority(t)
	c := client(t, address)
	ips := c.GetFakeIPForDomain("before.example")
	stop()
	start := time.Now()
	if got := c.GetFakeIPForDomain("after.example"); len(got) != 0 {
		t.Fatal("allocated locally after RPC failure")
	}
	if got := c.GetDomainFromFakeDNS(ips[0]); got != "" {
		t.Fatal("used stale reverse cache")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("unbounded request")
	}
	c.Close()
	if got := c.GetFakeIPForDomain("closed.example"); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestServiceRejectsMalformedRequests(t *testing.T) {
	address, _, _ := authority(t)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := command.NewFakeDNSServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, req := range []*command.AllocateRequest{{Domain: "", Ipv4: true}, {Domain: "bad\n.example", Ipv4: true}, {Domain: "okay.example"}} {
		if _, err := c.Allocate(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%v: %v", req, err)
		}
	}
	if _, err := c.ReverseLookup(ctx, &command.ReverseLookupRequest{Ip: []byte{1, 2, 3}}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if resp, err := c.ReverseLookup(ctx, &command.ReverseLookupRequest{Ip: []byte{127, 0, 0, 1}}); err != nil || resp.Domain != "" {
		t.Fatal(resp, err)
	}
}
