package command

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestMappingsRejectNonlocalPeerBeforeAccess(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr:      &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 12345},
		LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080},
	})
	// A missing engine makes premature access visible; all methods must deny
	// before inspecting requests, reporting pools or mutating the allocator.
	s := new(service)
	if _, err := s.GetPools(ctx, nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if _, err := s.Allocate(ctx, nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if _, err := s.ReverseLookup(ctx, nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
}
