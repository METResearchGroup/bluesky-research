package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"bluesky/sync/backfill"
)

// Options holds configuration for the backfill process
type Options struct {
	InputFile   string
	OutputFile  string
	BatchSize   int
	Concurrency int
	Verbose     bool
	TimeoutSecs int
	RateLimit   int
	RateWindow  int
	StartDate   string
	EndDate     string
}

func main() {
	// Parse command line arguments
	opts := parseArgs()

	// Initialize logger
	logger := log.New(os.Stdout, "[GO-SYNC] ", log.LstdFlags)
	if opts.Verbose {
		logger.Printf("Starting backfill with options: %+v", opts)
	}

	// Read DIDs from file
	dids, err := loadDIDsFromFile(opts.InputFile)
	if err != nil {
		logger.Fatalf("Failed to load DIDs: %v", err)
	}
	logger.Printf("Loaded %d DIDs from %s", len(dids), opts.InputFile)

	// Configure and run backfill
	config := backfill.Config{
		Concurrency: opts.Concurrency,
		BatchSize:   opts.BatchSize,
		Timeout:     time.Duration(opts.TimeoutSecs) * time.Second,
		RateLimit:   opts.RateLimit,
		RateWindow:  opts.RateWindow,
		StartDate:   opts.StartDate,
		EndDate:     opts.EndDate,
		OutputFile:  opts.OutputFile,
		Logger:      logger,
	}

	// Measure execution time
	startTime := time.Now()

	// Run the backfill
	result, err := backfill.Run(dids, config)
	if err != nil {
		logger.Fatalf("Backfill failed: %v", err)
	}

	duration := time.Since(startTime)

	// Report results
	logger.Printf("Backfill completed in %v", duration)
	logger.Printf("Processed %d DIDs", result.ProcessedCount)
	logger.Printf("Success rate: %.2f%%", result.SuccessRate*100)
	logger.Printf("Records processed: %d", result.RecordCount)
	logger.Printf("Average throughput: %.2f DIDs/sec", float64(result.ProcessedCount)/duration.Seconds())

	// Output results as JSON if requested
	if opts.OutputFile != "" {
		resultData := map[string]interface{}{
			"duration_ms":     duration.Milliseconds(),
			"processed_count": result.ProcessedCount,
			"success_rate":    result.SuccessRate,
			"record_count":    result.RecordCount,
			"dids_per_second": float64(result.ProcessedCount) / duration.Seconds(),
			"memory_mb":       result.PeakMemoryMB,
			"api_calls":       result.ApiCalls,
			"rate_limited":    result.RateLimitedCalls,
		}

		resultJSON, err := json.MarshalIndent(resultData, "", "  ")
		if err != nil {
			logger.Printf("Failed to marshal results: %v", err)
		} else {
			if err := ioutil.WriteFile(opts.OutputFile, resultJSON, 0644); err != nil {
				logger.Printf("Failed to write results: %v", err)
			} else {
				logger.Printf("Results written to %s", opts.OutputFile)
			}
		}
	}
}

func parseArgs() Options {
	// Define and parse command-line flags
	inputFile := flag.String("input", "", "Path to file containing list of DIDs (one per line)")
	outputFile := flag.String("output", "", "Path to output file for results (JSON)")
	batchSize := flag.Int("batch-size", 50, "Number of DIDs to process in each batch")
	concurrency := flag.Int("concurrency", runtime.NumCPU(), "Maximum number of concurrent requests")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")
	timeoutSecs := flag.Int("timeout", 30, "Timeout for HTTP requests in seconds")
	rateLimit := flag.Int("rate-limit", 3000, "Rate limit (requests per window)")
	rateWindow := flag.Int("rate-window", 300, "Rate limit window in seconds")
	startDate := flag.String("start-date", "", "Start date for backfill (YYYY-MM-DD)")
	endDate := flag.String("end-date", "", "End date for backfill (YYYY-MM-DD)")

	flag.Parse()

	if *inputFile == "" {
		fmt.Println("Error: Input file is required")
		flag.Usage()
		os.Exit(1)
	}

	return Options{
		InputFile:   *inputFile,
		OutputFile:  *outputFile,
		BatchSize:   *batchSize,
		Concurrency: *concurrency,
		Verbose:     *verbose,
		TimeoutSecs: *timeoutSecs,
		RateLimit:   *rateLimit,
		RateWindow:  *rateWindow,
		StartDate:   *startDate,
		EndDate:     *endDate,
	}
}

func loadDIDsFromFile(filename string) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	var dids []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			dids = append(dids, line)
		}
	}

	return dids, nil
}
