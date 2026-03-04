# GEX Collector for Sierra Chart

Display real-time GEX (Gamma Exposure) levels directly on your Sierra Chart — no external app needed. Just add the study, enter your API key, and the collector runs automatically in the background.

## Overview

This project combines two components:

1. **`gex-collector.exe`** — A headless Go service that polls the [GEXBot](https://www.gexbot.com) API and stores data in SQLite databases.
2. **`GEX.cpp`** — A Sierra Chart ACSIL study that auto-launches the collector and displays GEX levels as horizontal lines on the chart.

When you add the study to a chart, it automatically starts the collector process for the configured ticker. A Windows named mutex ensures only one collector runs per ticker, even across multiple charts.

## Displayed Levels

| Subgraph | Description | Color |
|----------|-------------|-------|
| LongGamma | Major long gamma level | Cyan |
| ShortGamma | Major short gamma level | Purple |
| MajorPositive | Major positive level | Green |
| MajorNegative | Major negative level | Red |
| ZeroGamma | Zero gamma level | Orange |
| NetGex | Net GEX volume (hidden by default) | Green |

## Requirements

- [Sierra Chart](https://www.sierrachart.com) (64-bit)
- A [GEXBot](https://www.gexbot.com) API key (Classic, State, or Orderflow subscription)
- Visual Studio Build Tools 2022+ with C++ workload (for compiling the study)

## Installation

### 1. Place the collector executable

Download `gex-collector.exe` from the [`bin/`](bin/) folder and place it somewhere on your system, for example:

```
C:\Users\<you>\Documents\TRADING\OPTIONS\gex-collector.exe
```

This is also where the SQLite databases will be stored (in `Tickers MM.DD.YYYY/` subdirectories).

### 2. Compile the Sierra Chart study

1. Copy `sierra-chart/GEX.cpp` to your Sierra Chart `ACS_Source` folder:
   ```
   C:\SierraChart\ACS_Source\GEX.cpp
   ```

2. Make sure `sqlite3.c` and `sqlite3.h` are present in `ACS_Source`. If not, download them from [sqlite.org](https://www.sqlite.org/download.html) (amalgamation).

3. In Sierra Chart, go to **Analysis > Build Custom Studies DLL**.

4. Set **Additional Compiler Parameters** to:
   ```
   "C:\SierraChart\ACS_Source\sqlite3.c" /I"C:\SierraChart\ACS_Source"
   ```

5. Click **Build**.

### 3. Add the study to a chart

1. Open a chart for your instrument (e.g. ES)
2. Go to **Analysis > Studies**
3. Search for **GEX MAJORS** and add it
4. Configure the study inputs (see below)

## Study Inputs

| Input | Name | Default | Description |
|-------|------|---------|-------------|
| In:1 | FOLDER | `%USERPROFILE%\Documents\MAIN\TRADING\OPTIONS` | Root data directory. The collector creates `Tickers MM.DD.YYYY/` subdirectories here. |
| In:2 | TICKER | `ES_SPX` | Ticker symbol matching the GEXBot API (e.g. `ES_SPX`, `SPY`, `QQQ`, `NQ_NDX`). |
| In:3 | NB DAYS | `2` | Number of historical days to load from SQLite databases. |
| In:4 | Max age (sec) | `120` | Maximum age (in seconds) of the latest data point before levels are considered stale. |
| In:5 | API Key | *(empty)* | Your GEXBot API key. **Required** for the collector to start. |
| In:6 | Subscription | `State` | Dropdown menu — your GEXBot subscription tier. Tiers are cumulative: **Classic** collects classic data; **State** collects classic + state; **Orderflow** collects all. |
| In:7 | Refresh (sec) | `1` | Polling interval in seconds. How often the collector fetches new data from the API. |
| In:8 | EXE Path | *(auto)* | Full path to `gex-collector.exe`. |

## How It Works

1. When the study loads and an API key is provided, it checks for a Windows named mutex (`Global\GexCollector_<TICKER>`).
2. If no collector is running for that ticker, the study launches `gex-collector.exe` as a hidden background process with the configured parameters.
3. The collector polls the GEXBot API at the configured interval and writes data to SQLite databases in `<FOLDER>\Tickers MM.DD.YYYY\<TICKER>.db`.
4. The study reads the SQLite databases and draws GEX levels as horizontal lines on the chart, updating in real-time.

## Building from Source

If you want to build the collector yourself instead of using the pre-built binary:

### Prerequisites

- [Go](https://go.dev/dl/) 1.21+

### Build

```bash
cd gex-collector
GOOS=windows GOARCH=amd64 go build -o bin/gex-collector.exe .
```

### Collector CLI Flags

```
--api-key     GEXBot API key (required)
--ticker      Ticker symbol (default: ES_SPX)
--folder      Data directory (default: "")
--tiers       Comma-separated tiers: classic,state,orderflow (default: state)
--collect-all Collect all endpoints (default: true)
--refresh     Polling interval in ms (default: 0 = auto by priority)
--priority    0=High/1s, 1=Medium/5s, 2=Low/30s (default: 0)
--verbose     Enable verbose logging (default: false)
```

These flags are passed automatically by the study via `CreateProcess` — you don't need to run the collector manually.

## Data Storage

Data is stored in SQLite databases with WAL mode enabled for concurrent read/write:

```
<FOLDER>\
  Tickers 03.04.2026\
    ES_SPX.db
    SPY.db
  Tickers 03.03.2026\
    ES_SPX.db
```

Each database contains a `ticker_data` table with scalar fields (spot, gamma levels, positions) and a gzip-compressed `profiles_blob` column for profile data.

## License

MIT
