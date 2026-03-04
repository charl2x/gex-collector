#include "sierrachart.h"
#include "sqlite3.h"
#include <vector>
#include <string>
#include <oleauto.h>
#include <algorithm>

#pragma comment(lib, "OleAut32.lib")


SCDLLName("GEX_TERMINAL")

struct GammaRow
{
    double ts; // SCDateTime (GetAsDouble) dans le fuseau du chart
    float LG, SG, MP, MN, Z, NV;
    bool ValidLG, ValidSG, ValidMP, ValidMN, ValidZ, ValidNV;
};

struct GammaData
{
    std::vector<GammaRow> Rows;

    sqlite3* TodayDB = nullptr;
    double LastRawTs = 0.0;

    std::string LastFolder;
    std::string LastTicker;
    int LastDays = -1;

    SCDateTime LastSystemDay;
    bool Initialized = false;

    // Cache par bougie (pour coherence historique vs temps reel)
    int LastBarIndex = -1;
    bool HasCachedRow = false;
    GammaRow CachedRow = {};

    // Collector auto-launch state
    int LastLaunchAttemptBarIndex = -1;
    SCDateTime LastLaunchAttemptTime;
    bool CollectorLaunched = false;
};

static bool FileExists(const std::string& p)
{
    FILE* f = fopen(p.c_str(), "rb");
    if (f) { fclose(f); return true; }
    return false;
}

static std::string FormatDateSuffix(const SCDateTime& d)
{
    SYSTEMTIME st = {};
    VariantTimeToSystemTime((DATE)d.GetAsDouble(), &st);
    char buf[32];
    sprintf_s(buf, "%02d.%02d.%04d", st.wMonth, st.wDay, st.wYear);
    return buf;
}

static std::string GetDefaultFolder()
{
    char profile[MAX_PATH] = {};
    DWORD len = GetEnvironmentVariableA("USERPROFILE", profile, MAX_PATH);
    if (len == 0 || len >= MAX_PATH)
        return "";
    return std::string(profile) + "\\Documents";
}

static void ResetAll(GammaData* d)
{
    d->Rows.clear();
    d->LastRawTs = 0.0;
    d->Initialized = false;

    d->LastBarIndex = -1;
    d->HasCachedRow = false;
    d->CachedRow = {};

    if (d->TodayDB)
    {
        sqlite3_close(d->TodayDB);
        d->TodayDB = nullptr;
    }
}

static void SortRowsByTs(GammaData* d)
{
    if (!d) return;

    std::sort(d->Rows.begin(), d->Rows.end(),
        [](const GammaRow& a, const GammaRow& b)
        {
            return a.ts < b.ts;
        });
}

static float SafeColumnFloat(sqlite3_stmt* st, int col, bool& valid)
{
    if (sqlite3_column_type(st, col) == SQLITE_NULL)
    {
        valid = false;
        return 0.0f;
    }
    valid = true;
    return (float)sqlite3_column_double(st, col);
}

static void LoadIncremental(sqlite3* db, const SCDateTime& TimeScaleAdjustment, GammaData* d, SCStudyInterfaceRef sc)
{
    if (!db)
        return;

    std::string q =
        "SELECT timestamp, major_long_gamma, major_short_gamma, "
        "major_positive, major_negative, zero_gamma, net_gex_vol "
        "FROM ticker_data ";

    bool hasFilter = (d->LastRawTs > 0.0);
    if (hasFilter)
        q += "WHERE timestamp > ? ";

    q += "ORDER BY timestamp";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, q.c_str(), -1, &st, nullptr) != SQLITE_OK)
    {
        SCString msg;
        msg.Format("GEX: SQL prepare error: %s", sqlite3_errmsg(db));
        sc.AddMessageToLog(msg, 1);
        return;
    }

    if (hasFilter)
        sqlite3_bind_double(st, 1, d->LastRawTs);

    int count = 0;
    while (sqlite3_step(st) == SQLITE_ROW)
    {
        double raw = sqlite3_column_double(st, 0); // Unix UTC seconds

        // Unix seconds (UTC) -> SCDateTime (UTC)
        SCDateTime utcTime = SCDateTime(raw / 86400.0 + 25569.0);

        // UTC -> Chart TimeZone via sc.TimeScaleAdjustment
        SCDateTime chartTime = utcTime + TimeScaleAdjustment;

        GammaRow r;
        r.ts = chartTime.GetAsDouble();
        r.LG = SafeColumnFloat(st, 1, r.ValidLG);
        r.SG = SafeColumnFloat(st, 2, r.ValidSG);
        r.MP = SafeColumnFloat(st, 3, r.ValidMP);
        r.MN = SafeColumnFloat(st, 4, r.ValidMN);
        r.Z  = SafeColumnFloat(st, 5, r.ValidZ);
        r.NV = SafeColumnFloat(st, 6, r.ValidNV);

        if (!d->Rows.empty() && r.ts <= d->Rows.back().ts)
        {
            if (r.ts == d->Rows.back().ts)
            {
                d->LastRawTs = raw;
                continue;
            }
        }

        d->Rows.push_back(r);
        d->LastRawTs = raw;
        count++;
    }

    sqlite3_finalize(st);

    if (count > 0)
    {
        SCString msg;
        msg.Format("GEX: Loaded %d new rows (total=%d)", count, (int)d->Rows.size());
        sc.AddMessageToLog(msg, 0);
    }
}

static bool FindLastInBar(
    const std::vector<GammaRow>& rows,
    double barStart,
    double barEnd,
    double maxAgeDays,
    GammaRow& out)
{
    if (rows.empty())
        return false;

    auto it = std::lower_bound(
        rows.begin(),
        rows.end(),
        barEnd,
        [](const GammaRow& r, double t) { return r.ts < t; });

    if (it == rows.begin())
        return false;

    --it;

    if (it->ts < barStart)
        return false;

    if ((barEnd - it->ts) <= maxAgeDays)
    {
        out = *it;
        return true;
    }

    return false;
}

static void EnsureLoaded(
    SCStudyInterfaceRef sc,
    GammaData* d,
    const std::string& folder,
    const std::string& ticker,
    int days)
{
    SCDateTime systemDay = sc.CurrentSystemDateTime.GetDate();

    bool paramsChanged =
        folder != d->LastFolder ||
        ticker != d->LastTicker ||
        days != d->LastDays;

    bool dayChanged =
        d->LastSystemDay != 0 &&
        systemDay != d->LastSystemDay;

    if (!d->Initialized || paramsChanged)
    {
        ResetAll(d);

        d->LastFolder = folder;
        d->LastTicker = ticker;
        d->LastDays = days;
        d->LastSystemDay = systemDay;

        // Chargement historique (jours precedents)
        int loadedDays = 0;
        SCDateTime day = systemDay;

        int safety = 0;
        while (loadedDays < days - 1 && safety < 60)
        {
            safety++;
            day = SCDateTime(day.GetAsDouble() - 1);

            int dow = day.GetDayOfWeek();
            if (dow == SATURDAY || dow == SUNDAY)
                continue;

            std::string path =
                folder + "\\Tickers " +
                FormatDateSuffix(day) + "\\" +
                ticker + ".db";

            if (!FileExists(path))
                continue;

            sqlite3* db = nullptr;
            if (sqlite3_open_v2(path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) == SQLITE_OK)
            {
                LoadIncremental(db, sc.TimeScaleAdjustment, d, sc);
                sqlite3_close(db);
            }
            else
            {
                SCString msg;
                msg.Format("GEX: Failed to open historical DB: %s", path.c_str());
                sc.AddMessageToLog(msg, 1);
                if (db) sqlite3_close(db);
            }

            loadedDays++;
        }

        // Sort once after all historical days are loaded
        SortRowsByTs(d);

        d->Initialized = true;

        SCString msg;
        msg.Format("GEX: Initialized - Rows loaded=%d, HistDays=%d", (int)d->Rows.size(), loadedDays);
        sc.AddMessageToLog(msg, 0);
    }

    if (dayChanged)
    {
        if (d->TodayDB)
        {
            sqlite3_close(d->TodayDB);
            d->TodayDB = nullptr;
        }

        // Purge rows older than the configured window
        double cutoff = (systemDay.GetAsDouble() - days);
        auto newEnd = std::remove_if(d->Rows.begin(), d->Rows.end(),
            [cutoff](const GammaRow& r) { return r.ts < cutoff; });
        int purged = (int)(d->Rows.end() - newEnd);
        d->Rows.erase(newEnd, d->Rows.end());

        if (purged > 0)
        {
            SCString msg;
            msg.Format("GEX: Day changed - purged %d old rows, %d remaining", purged, (int)d->Rows.size());
            sc.AddMessageToLog(msg, 0);
        }

        d->LastRawTs = 0.0;
        d->LastSystemDay = systemDay;

        // Reset cache bougie
        d->LastBarIndex = -1;
        d->HasCachedRow = false;
        d->CachedRow = {};
    }

    // Ouvrir la DB du jour si dispo
    std::string todayPath =
        folder + "\\Tickers " +
        FormatDateSuffix(systemDay) + "\\" +
        ticker + ".db";

    if (!d->TodayDB && FileExists(todayPath))
    {
        sqlite3* tmp = nullptr;
        int rc = sqlite3_open_v2(todayPath.c_str(), &tmp, SQLITE_OPEN_READONLY, nullptr);

        if (rc == SQLITE_OK)
        {
            d->TodayDB = tmp;
            SCString msg;
            msg.Format("GEX: TodayDB opened: %s", todayPath.c_str());
            sc.AddMessageToLog(msg, 0);
        }
        else
        {
            SCString msg;
            msg.Format("GEX: Failed to open TodayDB: %s (rc=%d)", todayPath.c_str(), rc);
            sc.AddMessageToLog(msg, 1);
            if (tmp)
                sqlite3_close(tmp);

            d->TodayDB = nullptr;
        }
    }

}

// ============================================================
// Collector auto-launch helpers
// ============================================================

// Check if a collector process is already running for this ticker
// by probing the named mutex "Global\GexCollector_{ticker}"
static bool IsCollectorRunning(const char* ticker)
{
    std::string name = "Global\\GexCollector_" + std::string(ticker);
    HANDLE h = OpenMutexA(SYNCHRONIZE, FALSE, name.c_str());
    if (h)
    {
        CloseHandle(h);
        return true;
    }
    return false;
}

// Map subscription dropdown index to tiers string
// 0=Classic, 1=State, 2=Orderflow, 3=Classic+State, 4=State+Orderflow, 5=Classic+State+Orderflow
static std::string GetTiersFromIndex(int index)
{
    // Hierarchical: Classic < State (includes Classic) < Orderflow (includes State + Classic)
    switch (index)
    {
    case 0: return "classic";
    case 1: return "classic,state";
    case 2: return "classic,state,orderflow";
    default: return "classic,state";
    }
}

// Launch gex-collector.exe with all parameters via CreateProcess
static bool LaunchCollector(
    SCStudyInterfaceRef sc,
    const std::string& exePath,
    const std::string& apiKey,
    const std::string& ticker,
    const std::string& folder,
    const std::string& tiers,
    bool collectAll,
    int refreshMs,
    int priority)
{
    if (!FileExists(exePath))
    {
        SCString msg;
        msg.Format("GEX: Collector exe not found: %s", exePath.c_str());
        sc.AddMessageToLog(msg, 1);
        return false;
    }

    // Build command line
    // Note: API key may contain special chars, so we quote it
    char cmdLine[4096];
    sprintf_s(cmdLine, sizeof(cmdLine),
        "\"%s\" --api-key=\"%s\" --ticker=%s --folder=\"%s\" --tiers=%s --collect-all=%s --refresh=%d --priority=%d",
        exePath.c_str(),
        apiKey.c_str(),
        ticker.c_str(),
        folder.c_str(),
        tiers.c_str(),
        collectAll ? "true" : "false",
        refreshMs,
        priority);

    STARTUPINFOA si = {};
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESHOWWINDOW;
    si.wShowWindow = SW_HIDE;

    PROCESS_INFORMATION pi = {};

    BOOL ok = CreateProcessA(
        NULL,           // lpApplicationName (use cmdLine)
        cmdLine,        // lpCommandLine
        NULL,           // lpProcessAttributes
        NULL,           // lpThreadAttributes
        FALSE,          // bInheritHandles
        CREATE_NO_WINDOW | DETACHED_PROCESS,
        NULL,           // lpEnvironment
        NULL,           // lpCurrentDirectory
        &si,
        &pi);

    if (!ok)
    {
        SCString msg;
        msg.Format("GEX: Failed to launch collector (error=%d): %s", GetLastError(), exePath.c_str());
        sc.AddMessageToLog(msg, 1);
        return false;
    }

    // Close process/thread handles (we don't need to track the process)
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);

    SCString msg;
    msg.Format("GEX: Collector launched for %s (PID=%d)", ticker.c_str(), pi.dwProcessId);
    sc.AddMessageToLog(msg, 0);

    return true;
}


SCSFExport scsf_GEX_MAJORS(SCStudyInterfaceRef sc)
{
    SCSubgraphRef LG = sc.Subgraph[0];
    SCSubgraphRef SG = sc.Subgraph[1];
    SCSubgraphRef MP = sc.Subgraph[2];
    SCSubgraphRef MN = sc.Subgraph[3];
    SCSubgraphRef Z = sc.Subgraph[4];
    SCSubgraphRef NV = sc.Subgraph[5];

    // Original inputs
    SCInputRef Folder      = sc.Input[0];
    SCInputRef Ticker      = sc.Input[1];
    SCInputRef Days        = sc.Input[2];
    SCInputRef MaxAgeSec   = sc.Input[3];

    // Collector inputs
    SCInputRef ApiKey       = sc.Input[4];
    SCInputRef Subscription = sc.Input[5];
    SCInputRef RefreshMs    = sc.Input[6];
    SCInputRef ExePath      = sc.Input[7];

    if (sc.SetDefaults)
    {
        sc.GraphName = "GEX MAJORS                                                                       ";
        sc.AutoLoop = 0;
        sc.GraphRegion = 0;

        LG.Name = "LongGamma";  LG.DrawStyle = DRAWSTYLE_DASH; LG.PrimaryColor = RGB(0, 255, 255); LG.LineWidth = 6;
        SG.Name = "ShortGamma"; SG.DrawStyle = DRAWSTYLE_DASH; SG.PrimaryColor = RGB(174, 74, 213); SG.LineWidth = 6;
        MP.Name = "MajorPositive"; MP.DrawStyle = DRAWSTYLE_DASH; MP.PrimaryColor = RGB(0, 255, 0); MP.LineWidth = 5;
        MN.Name = "MajorNegative"; MN.DrawStyle = DRAWSTYLE_DASH; MN.PrimaryColor = RGB(255, 0, 0); MN.LineWidth = 5;
        Z.Name = "ZeroGamma"; Z.DrawStyle = DRAWSTYLE_DASH; Z.PrimaryColor = RGB(252, 177, 3); Z.LineWidth = 5;
        NV.Name = "NetGex"; NV.DrawStyle = DRAWSTYLE_IGNORE; NV.PrimaryColor = RGB(0, 159, 0); NV.LineWidth = 5;

        Folder.Name = "FOLDER";
        Folder.SetString(GetDefaultFolder().c_str());

        Ticker.Name = "TICKER";
        Ticker.SetString("ES_SPX");

        Days.Name = "NB DAYS";
        Days.SetInt(2);

        MaxAgeSec.Name = "Max age (sec)";
        MaxAgeSec.SetInt(120);

        ApiKey.Name = "API Key";
        ApiKey.SetString("");

        Subscription.Name = "Subscription";
        Subscription.SetCustomInputStrings("Classic;State;Orderflow");
        Subscription.SetIntWithoutTypeChange(1); // Default: State

        RefreshMs.Name = "Refresh (sec)";
        RefreshMs.SetInt(1);

        ExePath.Name = "EXE Path";
        ExePath.SetString((GetDefaultFolder() + "\\gex-collector.exe").c_str());

        return;
    }

    if (sc.LastCallToFunction)
    {
        GammaData* d0 = (GammaData*)sc.GetPersistentPointer(0);
        if (d0)
        {
            if (d0->TodayDB) sqlite3_close(d0->TodayDB);
            delete d0;
            sc.SetPersistentPointer(0, nullptr);
        }
        return;
    }

    GammaData* d = (GammaData*)sc.GetPersistentPointer(0);
    if (!d)
    {
        d = new GammaData();
        sc.SetPersistentPointer(0, d);
    }

    // ---- Collector auto-launch ----
    std::string apiKeyStr = ApiKey.GetString();
    std::string tickerStr = Ticker.GetString();
    std::string folderStr = Folder.GetString();

    if (!apiKeyStr.empty() && !d->CollectorLaunched)
    {
        // Cooldown: 30 seconds between launch attempts
        SCDateTime now = sc.CurrentSystemDateTime;
        double elapsed = (now.GetAsDouble() - d->LastLaunchAttemptTime.GetAsDouble()) * 86400.0;
        bool cooldownOk = (d->LastLaunchAttemptTime.GetAsDouble() == 0.0 || elapsed >= 30.0);

        if (cooldownOk)
        {
            d->LastLaunchAttemptTime = now;

            if (!IsCollectorRunning(tickerStr.c_str()))
            {
                std::string exePath = ExePath.GetString();
                std::string tiers = GetTiersFromIndex(Subscription.GetIndex());
                int refreshSec = RefreshMs.GetInt();
                int refreshMs = refreshSec * 1000;

                if (LaunchCollector(sc, exePath, apiKeyStr, tickerStr, folderStr, tiers, true, refreshMs, 0))
                {
                    d->CollectorLaunched = true;
                }
            }
            else
            {
                // Already running (another chart or external process)
                d->CollectorLaunched = true;
                SCString msg;
                msg.Format("GEX: Collector already running for %s", tickerStr.c_str());
                sc.AddMessageToLog(msg, 0);
            }
        }
    }

    // ---- SQLite data loading (unchanged) ----
    EnsureLoaded(sc, d,
        folderStr.c_str(),
        tickerStr.c_str(),
        Days.GetInt());

    // Lecture DB immediate (sans Refresh)
    if (d->TodayDB)
    {
        LoadIncremental(d->TodayDB, sc.TimeScaleAdjustment, d, sc);
        SortRowsByTs(d);
    }


    const double maxAgeDays = MaxAgeSec.GetInt() / 86400.0;

    int start = sc.UpdateStartIndex;
    if (start < 0) start = 0;

    for (int i = start; i < sc.ArraySize; i++)
    {
        double barStart = sc.BaseDateTimeIn[i].GetAsDouble();
        double barEnd = sc.GetEndingDateTimeForBarIndex(i).GetAsDouble();

        // nouvelle bougie -> reset cache
        if (i != d->LastBarIndex)
        {
            d->LastBarIndex = i;
            d->HasCachedRow = false;
        }

        // capter UNE fois la valeur valide pour cette bougie
        if (!d->HasCachedRow)
        {
            GammaRow r;
            if (FindLastInBar(d->Rows, barStart, barEnd, maxAgeDays, r))
            {
                d->CachedRow = r;
                d->HasCachedRow = true;
            }
        }

        // afficher si on a capture
        if (d->HasCachedRow)
        {
            if (d->CachedRow.ValidLG) LG[i] = d->CachedRow.LG;
            if (d->CachedRow.ValidSG) SG[i] = d->CachedRow.SG;
            if (d->CachedRow.ValidMP) MP[i] = d->CachedRow.MP;
            if (d->CachedRow.ValidMN) MN[i] = d->CachedRow.MN;
            if (d->CachedRow.ValidZ)  Z[i]  = d->CachedRow.Z;
            if (d->CachedRow.ValidNV) NV[i] = d->CachedRow.NV;
        }
    }
}
