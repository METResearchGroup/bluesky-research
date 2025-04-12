package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

type BenchmarkResult struct {
	DurationMs     int     `json:"duration_ms"`
	MemoryMb       float64 `json:"memory_mb"`
	ProcessedCount int     `json:"processed_count"`
	RecordCount    int     `json:"record_count"`
	ApiCalls       int     `json:"api_calls"`
	RateLimited    int     `json:"rate_limited"`
	SuccessRate    float64 `json:"success_rate"`
}

func TestBackfillSync(t *testing.T) {
	// Create temporary file with test DIDs
	dids := []string{
		"did:plc:test1",
		"did:plc:test2",
	}
	tempDidsFile, err := ioutil.TempFile("", "dids.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempDidsFile.Name())

	for _, did := range dids {
		if _, err := tempDidsFile.WriteString(did + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	tempDidsFile.Close()

	// Create temporary file for output
	tempOutputFile, err := ioutil.TempFile("", "results.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempOutputFile.Name())
	tempOutputFile.Close()

	// Build the Go binary
	cmdBuild := exec.Command("go", "build", "-o", "backfill_sync")
	cmdBuild.Dir = "." // assuming current directory is go_sync
	if out, err := cmdBuild.CombinedOutput(); err != nil {
		t.Fatalf("failed to build binary: %v, output: %s", err, string(out))
	}

	// Run the binary
	cmdRun := exec.Command("./backfill_sync", "--input", tempDidsFile.Name(), "--output", tempOutputFile.Name(), "--concurrency", "2", "--batch-size", "1", "--timeout", "10", "--rate-limit", "100", "--verbose")
	cmdRun.Dir = "."
	if out, err := cmdRun.CombinedOutput(); err != nil {
		t.Fatalf("binary execution failed: %v, output: %s", err, string(out))
	}

	// Read output JSON file
	outputBytes, err := ioutil.ReadFile(tempOutputFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	var result BenchmarkResult
	if err := json.Unmarshal(outputBytes, &result); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	// Validate that processed count matches the number of DIDs provided
	if result.ProcessedCount != len(dids) {
		t.Errorf("expected processed_count %d, got %d", len(dids), result.ProcessedCount)
	}
}
