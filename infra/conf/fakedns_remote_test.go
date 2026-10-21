package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/infra/conf"
)

func TestRemoteFakeDNSConfig(t *testing.T) {
	var cfg conf.FakeDNSConfig
	if err := json.Unmarshal([]byte(`{"remote":{"address":"unix:///run/xray/fakedns.sock","timeout":1200}}`), &cfg); err != nil {
		t.Fatal(err)
	}
	built, err := cfg.Build()
	if err != nil || built.Remote.Address != "unix:///run/xray/fakedns.sock" || built.Remote.Timeout != 1200 || len(built.Pools) != 0 {
		t.Fatal(built, err)
	}
	encoded, err := json.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &cfg); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(`{"Remote":{"Address":"127.0.0.1:8080","Timeout":1200}}`), &cfg); err != nil {
		t.Fatal("remote config ignored normal case-insensitive JSON decoding", err)
	}
	if built, err := cfg.Build(); err != nil || built.Remote == nil || built.Remote.Address != "127.0.0.1:8080" {
		t.Fatal(built, err)
	}
	for _, text := range []string{`{"remote":null}`, `{"remote":{}}`, `{"remote":{"address":"127.0.0.1:1"},"ipPool":"198.18.0.0/15"}`} {
		if err := json.Unmarshal([]byte(text), &cfg); err == nil {
			t.Fatal("accepted", text)
		}
	}
	if err := json.Unmarshal([]byte(`{"ipPool":"198.18.0.0/15","poolSize":65535}`), &cfg); err != nil {
		t.Fatal(err)
	}
	built, err = cfg.Build()
	if err != nil || built.Remote != nil || len(built.Pools) != 1 {
		t.Fatal("previous remote value leaked into local config", built, err)
	}
	if err := json.Unmarshal([]byte(`[null]`), &cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Build(); err == nil {
		t.Fatal("null pool should produce an error")
	}
}

func TestFakeDNSServiceConfig(t *testing.T) {
	config, err := (&conf.APIConfig{Tag: "api", Services: []string{"FakeDNSService"}}).Build()
	if err != nil || len(config.Service) != 1 {
		t.Fatal(config, err)
	}
}
