package agent

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func TestDevinBackendConstructs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	b, err := New("devin", Config{Logger: logger})
	if err != nil {
		t.Fatalf("New(devin): %v", err)
	}
	if b == nil {
		t.Fatal("New(devin) returned nil backend")
	}
}

func TestDevinLaunchHeader(t *testing.T) {
	if got := LaunchHeader("devin"); got == "" {
		t.Error("LaunchHeader(devin) returned empty string")
	}
}

func TestDevinIsSupported(t *testing.T) {
	if !IsSupportedType("devin") {
		t.Error("IsSupportedType(devin) = false, want true")
	}
}

func TestParseDevinModelsList(t *testing.T) {
	input := `Claude Sonnet 5 (claude-sonnet-5)
  claude-sonnet-5-medium               Claude Sonnet 5 Medium  [1M context, ...]
  claude-sonnet-5-low                  Claude Sonnet 5 Low  [1M context, ...]

SWE-1.7 (swe-1.7)
  swe-1-7                              SWE-1.7 Max  [262K context, Free]
  swe-1-7-medium                       SWE-1.7 Medium  [262K context, Free]
`
	models, err := parseDevinModelsList(input)
	if err != nil {
		t.Fatalf("parseDevinModelsList: %v", err)
	}
	if len(models) != 4 {
		t.Fatalf("expected 4 models, got %d: %+v", len(models), models)
	}
	want := map[string]string{
		"claude-sonnet-5-medium": "Claude Sonnet 5 Medium",
		"claude-sonnet-5-low":    "Claude Sonnet 5 Low",
		"swe-1-7":                "SWE-1.7 Max",
		"swe-1-7-medium":         "SWE-1.7 Medium",
	}
	for _, m := range models {
		wantLabel, ok := want[m.ID]
		if !ok {
			t.Fatalf("unexpected model %q", m.ID)
		}
		if m.Label != wantLabel {
			t.Errorf("model %q: label = %q, want %q", m.ID, m.Label, wantLabel)
		}
		wantProvider := "anthropic"
		if m.ID == "swe-1-7" || m.ID == "swe-1-7-medium" {
			wantProvider = "cognition"
		}
		if m.Provider != wantProvider {
			t.Errorf("model %q: provider = %q, want %q", m.ID, m.Provider, wantProvider)
		}
	}
}

func TestDiscoverDevinModelsFallsBackWhenMissing(t *testing.T) {
	// Point at a command that does not exist so discovery falls back to the
	// static catalog.
	runtimeCmd := Command{Path: "/dev/null/not-devin"}
	models, err := discoverDevinModels(context.Background(), runtimeCmd)
	if err != nil {
		t.Fatalf("discoverDevinModels: %v", err)
	}
	if len(models) == 0 {
		t.Fatal("expected non-empty static fallback catalog")
	}
	seen := map[string]bool{}
	for _, m := range models {
		seen[m.ID] = true
	}
	if !seen["swe-1-7"] {
		t.Error("static fallback missing swe-1-7")
	}
}

func TestDevinStaticModelsAreSensible(t *testing.T) {
	models := devinStaticModels()
	if len(models) == 0 {
		t.Fatal("devinStaticModels returned no models")
	}
	var defaultFound bool
	for _, m := range models {
		if m.Default && m.ID == "swe-1-7" {
			defaultFound = true
		}
	}
	if !defaultFound {
		t.Error("expected swe-1-7 to be the default static model")
	}
}
