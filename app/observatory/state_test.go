package observatory

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestObservationStateRestoresAllGroupsAndFreshFailuresReplaceIt(t *testing.T) {
	old := &Observer{status: []*OutboundStatus{
		{OutboundTag: "main-member", Alive: true, Delay: 10, LastTryTime: 20, LastSeenTime: 20},
		{OutboundTag: "routed-member", Alive: true, Delay: 30, LastTryTime: 40, LastSeenTime: 40},
		{OutboundTag: "removed", Alive: false, LastErrorReason: "failed"},
	}}
	data, err := old.SnapshotObservation()
	if err != nil {
		t.Fatal(err)
	}
	next := new(Observer)
	if err := next.RestoreObservation(data, []string{"main-member", "routed-member"}); err != nil {
		t.Fatal(err)
	}
	report, _ := next.GetObservation(context.Background())
	if got := report.(*ObservationResult).Status; len(got) != 2 || !proto.Equal(got[1], old.status[1]) {
		t.Fatalf("routed group's full state was not restored: %v", got)
	}
	next.updateStatusForResult("routed-member", &ProbeResult{Alive: false, LastErrorReason: "fresh failure"})
	report, _ = next.GetObservation(context.Background())
	if report.(*ObservationResult).Status[1].Alive {
		t.Fatal("cached success hid fresh failure")
	}
	// Consumers cannot mutate the live report or the saved state.
	report.(*ObservationResult).Status[0].Alive = false
	again, _ := next.GetObservation(context.Background())
	if !again.(*ObservationResult).Status[0].Alive {
		t.Fatal("report shares mutable state")
	}
	if err := next.RestoreObservation([]byte{255}, nil); err == nil {
		t.Fatal("invalid state accepted")
	}
}

func TestObservationSnapshotsConcurrentWithProbeUpdates(t *testing.T) {
	o := new(Observer)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for range 100 {
			o.updateStatusForResult("routed", &ProbeResult{Alive: true, Delay: 10})
		}
	}()
	go func() {
		defer workers.Done()
		for range 100 {
			if _, err := o.SnapshotObservation(); err != nil {
				t.Error(err)
			}
			_, _ = o.GetObservation(context.Background())
		}
	}()
	workers.Wait()
}
