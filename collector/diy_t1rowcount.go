package collector

import (
	"context"
	"database/sql"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

var t1rowcountDesc = prometheus.NewDesc(
	prometheus.BuildFQName(namespace, "diy", "t1_row_count"),
	"is t1 row count",
	[]string{"NewDesc", "VariablesLabel"}, nil,
)

type DiyScrape struct {
}

func (DiyScrape) Name() string {
	return "t1_row_count"
}

func (DiyScrape) Help() string {
	return "Collect prom.t1 row count"
}

func (DiyScrape) Version() float64 {
	return 1.0
}

func (DiyScrape) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	sqlStr := "SELECT count(1) FROM prom.t1"

	rows, err := db.QueryContext(ctx, sqlStr)

	if err != nil {
		return err
	}

	var ret int

	for rows.Next() {
		rows.Scan(&ret)
		ch <- prometheus.MustNewConstMetric(t1rowcountDesc, prometheus.GaugeValue, float64(ret), "labelValue_IN_mustNewConstMetric_1", "labelValue_IN_mustNewConstMetric_2")
	}

	return nil
}

var _ Scraper = DiyScrape{}
