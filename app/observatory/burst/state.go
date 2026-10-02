package burst

import (
	"encoding/json"
	"slices"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

type sampleState struct {
	Time  time.Time
	Value time.Duration
}

type windowState struct {
	Index    int
	Capacity int
	Validity time.Duration
	Samples  []sampleState
}

// SnapshotObservation preserves the complete RTT windows, including failed and
// untested samples. Original timestamps keep their existing expiration behavior.
func (o *Observer) SnapshotObservation() ([]byte, error) {
	o.hp.access.Lock()
	defer o.hp.access.Unlock()
	state := make(map[string]windowState, len(o.hp.Results))
	for tag, result := range o.hp.Results {
		window := windowState{Index: result.idx, Capacity: result.cap, Validity: result.validity}
		for _, sample := range result.rtts {
			window.Samples = append(window.Samples, sampleState{Time: sample.time, Value: sample.value})
		}
		state[tag] = window
	}
	return json.Marshal(state)
}

func (o *Observer) RestoreObservation(data []byte, allowed []string) error {
	if o.finished != nil {
		return errors.New("observation state must be restored before Start")
	}
	var state map[string]windowState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	results := make(map[string]*HealthPingRTTS)
	restored := make(map[string]bool)
	for tag, window := range state {
		if !slices.Contains(allowed, tag) {
			continue
		}
		if window.Capacity != o.hp.Settings.SamplingCount ||
			len(window.Samples) != window.Capacity || window.Index < 0 || window.Index >= window.Capacity ||
			window.Validity != o.hp.Settings.Interval*time.Duration(window.Capacity)*2 {
			return errors.New("invalid restored RTT window")
		}
		result := NewHealthPingResult(window.Capacity, window.Validity)
		result.idx = window.Index
		for _, sample := range window.Samples {
			result.rtts = append(result.rtts, &pingRTT{time: sample.Time, value: sample.Value})
		}
		results[tag] = result
		restored[tag] = true
	}
	o.hp.access.Lock()
	defer o.hp.access.Unlock()
	o.hp.Results = results
	o.hp.restored = restored
	return nil
}
