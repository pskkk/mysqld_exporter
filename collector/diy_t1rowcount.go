package collector

import (
	"context"
	"database/sql"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

type DiyScrape struct {
}

func (DiyScrape) Name() string {
	return ""
}

func (DiyScrape) Help() string {
	return ""
}

func (DiyScrape) Version() float64 {
	return 1.0
}

func (DiyScrape) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	return nil
}

var _ Scraper = DiyScrape{}
