package remote_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dns/fakedns/command"
	"github.com/xtls/xray-core/app/dns/fakedns/remote"
	"google.golang.org/grpc"
)

type controlledService struct {
	command.UnimplementedFakeDNSServiceServer
	ranges    []string
	addresses [][]byte
	entered   chan struct{}
}

func (s *controlledService) GetPools(context.Context, *command.GetPoolsRequest) (*command.GetPoolsResponse, error) {
	return &command.GetPoolsResponse{IpPools: s.ranges}, nil
}

func (s *controlledService) Allocate(ctx context.Context, _ *command.AllocateRequest) (*command.AllocateResponse, error) {
	if s.entered != nil {
		close(s.entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &command.AllocateResponse{Ips: s.addresses}, nil
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
