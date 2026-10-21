package remote_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/commander"
	"github.com/xtls/xray-core/app/dns/fakedns"
	"github.com/xtls/xray-core/app/dns/fakedns/command"
	"github.com/xtls/xray-core/app/dns/fakedns/remote"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type controlledService struct {
	command.UnimplementedFakeDNSServiceServer
	ranges       []string
	addresses    [][]byte
	entered      chan struct{}
	exited       chan struct{}
	startEntered chan struct{}
	domain       string
	rpcErr       error
}

func (s *controlledService) GetPools(ctx context.Context, _ *command.GetPoolsRequest) (*command.GetPoolsResponse, error) {
	if s.startEntered != nil {
		close(s.startEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &command.GetPoolsResponse{IpPools: s.ranges}, nil
}

func (s *controlledService) Allocate(ctx context.Context, _ *command.AllocateRequest) (*command.AllocateResponse, error) {
	if s.entered != nil {
		close(s.entered)
		if s.exited != nil {
			defer close(s.exited)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if s.rpcErr != nil {
		return nil, s.rpcErr
	}
	return &command.AllocateResponse{Ips: s.addresses}, nil
}

func (s *controlledService) ReverseLookup(context.Context, *command.ReverseLookupRequest) (*command.ReverseLookupResponse, error) {
	if s.rpcErr != nil {
		return nil, s.rpcErr
	}
	return &command.ReverseLookupResponse{Domain: s.domain}, nil
}

func controlledAddress(t *testing.T, s *controlledService) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	command.RegisterFakeDNSServiceServer(g, s)
	go g.Serve(l)
	t.Cleanup(g.Stop)
	return l.Addr().String()
}

func forwardedAddress(t *testing.T, address string) string {
	t.Helper()
	s, err := core.New(&core.Config{App: []*serial.TypedMessage{serial.ToTypedMessage(&fakedns.FakeDnsPoolMulti{Remote: &fakedns.RemoteConfig{Address: address, Timeout: 5000}})}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	obj, err := core.CreateObject(s, &command.Config{})
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	obj.(commander.Service).Register(g)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(l)
	t.Cleanup(g.Stop)
	return l.Addr().String()
}

func TestForwardedRequestPreservesCancellation(t *testing.T) {
	backend := &controlledService{ranges: []string{"198.18.0.0/24"}, entered: make(chan struct{}), exited: make(chan struct{})}
	address := forwardedAddress(t, controlledAddress(t, backend))
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := command.NewFakeDNSServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.Allocate(ctx, &command.AllocateRequest{Domain: "pending.example", Ipv4: true})
		done <- err
	}()
	select {
	case <-backend.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("backend request did not start")
	}
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	select {
	case <-backend.exited:
	case <-time.After(time.Second):
		t.Fatal("forwarder discarded caller cancellation")
	}
}

func TestForwardedRequestPreservesBackendFailure(t *testing.T) {
	backend := &controlledService{ranges: []string{"198.18.0.0/24"}, rpcErr: status.Error(codes.Unavailable, "authority unavailable")}
	address := forwardedAddress(t, controlledAddress(t, backend))
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := command.NewFakeDNSServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Allocate(ctx, &command.AllocateRequest{Domain: "down.example", Ipv4: true}); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if _, err := c.ReverseLookup(ctx, &command.ReverseLookupRequest{Ip: []byte{198, 18, 0, 1}}); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}

func TestCloseCancelsStartup(t *testing.T) {
	backend := &controlledService{startEntered: make(chan struct{})}
	c, err := remote.New(context.Background(), controlledAddress(t, backend), 5000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	started := make(chan error, 1)
	go func() { started <- c.Start() }()
	select {
	case <-backend.startEntered:
	case <-time.After(time.Second):
		t.Fatal("startup did not reach backend")
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close blocked behind startup")
	}
	if err := <-started; err == nil {
		t.Fatal("canceled startup succeeded")
	}
	if ranges := c.GetFakeIPRanges(); len(ranges) != 0 {
		t.Fatal("published canceled startup", ranges)
	}
	if err := c.Start(); err == nil {
		t.Fatal("closed engine restarted")
	}
}

func TestRejectMalformedReverseDomain(t *testing.T) {
	for _, domain := range []string{"bad\n.example", "bad..example"} {
		c := client(t, controlledAddress(t, &controlledService{ranges: []string{"198.18.0.0/24"}, domain: domain}))
		ip := xnet.IPAddress([]byte{198, 18, 0, 1})
		if name := c.GetDomainFromFakeDNS(ip); name != "" {
			t.Fatal("accepted malformed domain", name)
		}
		if _, err := c.ReverseLookup(context.Background(), ip); err == nil {
			t.Fatal("missing malformed-response error")
		}
	}
}

func TestRejectMalformedRemotePoolsAndAddresses(t *testing.T) {
	for _, ranges := range [][]string{nil, {"invalid"}, {"::ffff:198.18.0.0/112"}} {
		s := &controlledService{ranges: ranges}
		c, err := remote.New(context.Background(), controlledAddress(t, s), 500)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Start(); err == nil {
			t.Fatal("accepted invalid ranges", ranges)
		}
		c.Close()
	}
	for _, raw := range [][]byte{{1, 2, 3}, {127, 0, 0, 1}, {0x20, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}} {
		c := client(t, controlledAddress(t, &controlledService{ranges: []string{"198.18.0.0/24"}, addresses: [][]byte{raw}}))
		if ips := c.GetFakeIPForDomain3("bad.example", true, false); len(ips) != 0 {
			t.Fatal("accepted invalid addresses", ips)
		}
	}
}

func TestCloseCancelsInFlightRPC(t *testing.T) {
	s := &controlledService{ranges: []string{"198.18.0.0/24"}, entered: make(chan struct{})}
	c := client(t, controlledAddress(t, s))
	done := make(chan struct{})
	go func() { defer close(done); c.GetFakeIPForDomain("pending.example") }()
	<-s.entered
	c.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RPC did not cancel on Close")
	}
	if err := c.Close(); err != nil {
		t.Fatal("Close is not idempotent", err)
	}
}
