package extension

import (
	"context"

	"github.com/xtls/xray-core/features"
	"google.golang.org/protobuf/proto"
)

type Observatory interface {
	features.Feature

	GetObservation(ctx context.Context) (proto.Message, error)
}

type BurstObservatory interface {
	Observatory
	Check(tag []string)
}

// StatefulObservatory carries probe history between instances on the same network.
// RestoreObservation must be called before Start. Only allowed outbound tags are restored.
type StatefulObservatory interface {
	Observatory
	SnapshotObservation() ([]byte, error)
	RestoreObservation(state []byte, allowed []string) error
}

func ObservatoryType() interface{} {
	return (*Observatory)(nil)
}
