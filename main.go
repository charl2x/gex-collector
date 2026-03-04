package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"unsafe"

	_ "time/tzdata" // Embed timezone data for Windows

	"gex-collector/internal/api"
	"gex-collector/internal/database"
	"gex-collector/internal/utils"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procCreateMutexW = kernel32.NewProc("CreateMutexW")
	procGetLastError = kernel32.NewProc("GetLastError")
)

const ERROR_ALREADY_EXISTS = 183

// createNamedMutex creates a Windows named mutex.
// Returns the handle and true if we own it, or 0 and false if it already exists.
func createNamedMutex(name string) (syscall.Handle, bool) {
	namePtr, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return 0, false
	}

	handle, _, _ := procCreateMutexW.Call(
		0,                          // lpMutexAttributes
		1,                          // bInitialOwner = TRUE
		uintptr(unsafe.Pointer(namePtr)),
	)

	if handle == 0 {
		return 0, false
	}

	// Check if mutex already existed
	lastErr, _, _ := procGetLastError.Call()
	if lastErr == ERROR_ALREADY_EXISTS {
		syscall.CloseHandle(syscall.Handle(handle))
		return 0, false
	}

	return syscall.Handle(handle), true
}

func main() {
	// CLI flags
	apiKey := flag.String("api-key", "", "GEXBot API key (required)")
	ticker := flag.String("ticker", "ES_SPX", "Ticker symbol")
	folder := flag.String("folder", "", "Data directory (contains 'Tickers MM.DD.YYYY/' subdirs)")
	tiersStr := flag.String("tiers", "state", "Comma-separated tiers: classic,state,orderflow")
	collectAll := flag.Bool("collect-all", true, "Collect all endpoints (true) or chart-only (false)")
	refreshMs := flag.Int("refresh", 0, "Polling interval in ms (0=auto by priority)")
	priority := flag.Int("priority", 0, "0=High/1s, 1=Medium/5s, 2=Low/30s")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	// Validate required flags
	if *apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: --api-key is required")
		flag.Usage()
		os.Exit(1)
	}
	if *folder == "" {
		fmt.Fprintln(os.Stderr, "Error: --folder is required")
		flag.Usage()
		os.Exit(1)
	}

	utils.SetVerbose(*verbose)

	// Parse tiers
	tiers := strings.Split(*tiersStr, ",")
	for i, t := range tiers {
		tiers[i] = strings.TrimSpace(t)
	}

	// Named mutex: one process per ticker
	mutexName := fmt.Sprintf("Global\\GexCollector_%s", *ticker)
	mutexHandle, owned := createNamedMutex(mutexName)
	if !owned {
		// Another process is already collecting this ticker
		utils.LogAlways("Collector for %s is already running (mutex: %s). Exiting.", *ticker, mutexName)
		os.Exit(0)
	}
	defer syscall.CloseHandle(mutexHandle)

	utils.LogAlways("=== GEX Collector started ===")
	utils.LogAlways("Ticker: %s", *ticker)
	utils.LogAlways("Folder: %s", *folder)
	utils.LogAlways("Tiers: %s", formatTiers(tiers))
	utils.LogAlways("Collect all: %v", *collectAll)
	utils.LogAlways("Mutex acquired: %s", mutexName)

	// Resolve polling interval
	interval := resolveInterval(*refreshMs, *priority)
	utils.LogAlways("Polling interval: %v", interval)

	// Initialize components
	client := api.NewClient(*apiKey)

	// Data directory is "Tickers" inside the folder
	// Writer will create "Tickers MM.DD.YYYY/" subdirectories
	dataDir := *folder + "\\Tickers"
	writer := database.NewDataWriter(dataDir)

	// Create and start collector
	collector := NewCollector(client, writer, *ticker, tiers, *collectAll, interval)
	collector.Start()

	// Wait for SIGTERM / Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	utils.LogAlways("Received signal %v, shutting down...", sig)

	// Graceful shutdown
	collector.Stop()
	if err := writer.Close(); err != nil {
		utils.LogAlways("Warning: error closing writer: %v", err)
	}
	client.Close()

	utils.LogAlways("=== GEX Collector stopped ===")
}
