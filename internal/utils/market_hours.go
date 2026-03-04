package utils

import (
	"log"
	"time"
)

// MARKET_TIMEZONE represents US Eastern Time
var MARKET_TIMEZONE *time.Location

func init() {
	var err error
	MARKET_TIMEZONE, err = time.LoadLocation("America/New_York")
	if err != nil {
		log.Printf("WARNING: Failed to load America/New_York timezone: %v - falling back to UTC", err)
		MARKET_TIMEZONE = time.UTC
	}
}

// GetMarketTimezone returns the market timezone (Eastern Time)
func GetMarketTimezone() *time.Location {
	return MARKET_TIMEZONE
}

// NowMarketTime returns current time in market timezone
func NowMarketTime() time.Time {
	return time.Now().In(MARKET_TIMEZONE)
}

// IsMarketOpen checks if the US stock market is currently open
func IsMarketOpen() bool {
	now := NowMarketTime()
	weekday := now.Weekday()
	if weekday == time.Saturday || weekday == time.Sunday {
		return false
	}
	marketOpen := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, MARKET_TIMEZONE)
	marketClose := time.Date(now.Year(), now.Month(), now.Day(), 16, 0, 0, 0, MARKET_TIMEZONE)
	return (now.After(marketOpen) || now.Equal(marketOpen)) && (now.Before(marketClose) || now.Equal(marketClose))
}

// IsWeekend checks if a date falls on a weekend
func IsWeekend(date time.Time) bool {
	date = date.In(MARKET_TIMEZONE)
	weekday := date.Weekday()
	return weekday == time.Saturday || weekday == time.Sunday
}

// GetLastTradingDay returns the last trading day (Friday) if weekend
func GetLastTradingDay(date time.Time) time.Time {
	date = date.In(MARKET_TIMEZONE)
	weekday := date.Weekday()
	if weekday == time.Saturday {
		return date.AddDate(0, 0, -1)
	} else if weekday == time.Sunday {
		return date.AddDate(0, 0, -2)
	}
	return date
}

// GetMarketDate returns the current market date in Eastern Time
// Date rolls over at 8:30 AM ET
func GetMarketDate() time.Time {
	now := NowMarketTime()
	rolloverTime := time.Date(now.Year(), now.Month(), now.Day(), 8, 30, 0, 0, MARKET_TIMEZONE)
	if now.Before(rolloverTime) {
		return now.AddDate(0, 0, -1)
	}
	return now
}
