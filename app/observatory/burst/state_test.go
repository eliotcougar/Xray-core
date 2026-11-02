package burst

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func stateObserver() *Observer {
	return &Observer{hp: NewHealthPing(context.Background(), nil, &HealthPingConfig{
		SamplingCount: 3, Interval: int64(time.Minute),
	})}
}

func TestBurstStatePreservesSampleWindowsAndFreshResultsReplaceThem(t *testing.T) {
	old := stateObserver()
	for _, tag := range []string{"main", "routed", "removed"} {
		old.hp.PutResult(tag, 10*time.Millisecond)
		old.hp.PutResult(tag, rttFailed)
		old.hp.PutResult(tag, 30*time.Millisecond)
	}
	state, err := old.SnapshotObservation()
	if err != nil {
		t.Fatal(err)
	}
	next := stateObserver()
	if err := next.RestoreObservation(state, []string{"main", "routed"}); err != nil {
		t.Fatal(err)
	}
	if len(next.hp.Results) != 2 {
		t.Fatal("removed outbound was restored")
	}
	oldWindow, newWindow := old.hp.Results["routed"], next.hp.Results["routed"]
	if oldWindow.idx != newWindow.idx || oldWindow.cap != newWindow.cap || oldWindow.validity != newWindow.validity {
		t.Fatal("RTT window metadata was not restored")
	}
	for index, sample := range oldWindow.rtts {
		if !sample.time.Equal(newWindow.rtts[index].time) || sample.value != newWindow.rtts[index].value {
			t.Fatal("RTT samples were not restored")
		}
	}
	if !reflect.DeepEqual(old.hp.Results["routed"].Get(), next.hp.Results["routed"].Get()) {
		t.Fatal("restored statistics differ")
	}
	next.hp.PutResult("routed", rttFailed)
	if got := next.hp.Results["routed"].Get(); got.All != 1 || got.Fail != 1 {
		t.Fatalf("fresh failure did not replace cached window: %+v", got)
	}
	if got := next.hp.Results["main"].Get(); got.All != 3 {
		t.Fatal("another group's window was changed")
	}
	if err := next.RestoreObservation([]byte("invalid"), nil); err == nil {
		t.Fatal("invalid state accepted")
	}
}

func TestBurstRestorationDoesNotRenewExpiredSamples(t *testing.T) {
	o := stateObserver()
	o.hp.PutResult("routed", time.Millisecond)
	o.hp.Results["routed"].rtts[0].time = time.Now().Add(-time.Hour)
	state, _ := o.SnapshotObservation()
	next := stateObserver()
	if err := next.RestoreObservation(state, []string{"routed"}); err != nil {
		t.Fatal(err)
	}
	if got := next.hp.Results["routed"].Get(); got.All != 0 {
		t.Fatal("expired history became fresh")
	}
}
