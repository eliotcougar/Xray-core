package core

import "testing"

type stateTestFeature struct{ id int }

func (*stateTestFeature) Type() interface{} { return (*stateTestFeature)(nil) }
func (*stateTestFeature) Start() error      { return nil }
func (*stateTestFeature) Close() error      { return nil }

func TestGetFeaturesIncludesAllImplementationsWithoutChangingFirstSelection(t *testing.T) {
	s := new(Instance)
	first, second := &stateTestFeature{id: 1}, &stateTestFeature{id: 2}
	if err := s.AddFeature(first); err != nil {
		t.Fatal(err)
	}
	if err := s.AddFeature(second); err != nil {
		t.Fatal(err)
	}
	all := s.GetFeatures(first.Type())
	if len(all) != 2 || all[0] != first || all[1] != second {
		t.Fatal("not all features returned")
	}
	all[0] = nil
	if s.GetFeature(first.Type()) != first {
		t.Fatal("first selection changed")
	}
}
