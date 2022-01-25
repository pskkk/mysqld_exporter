package collector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/mysqld_exporter/tools"
)

const (
	// 获得并传递最大的阻塞时间
	blockedTrxCountQuery = `
	SELECT 
		ifnull(max(tmp.summary_wait_secs),0)
	FROM 
		(SELECT 
			r.trx_wait_started AS summary_wait_started,  
			timestampdiff(SECOND,r.trx_wait_started,now()) AS summary_wait_secs, 
			rl.lock_table AS summary_locked_table, 
			rl.lock_index AS summary_locked_index, 
			rl.lock_type AS summary_locked_type,
		
			b.trx_id AS blocking_trx_id, 
			b.trx_mysql_thread_id AS blocking_pid, 
			bl.lock_id AS blocking_lock_id, 
			bl.lock_mode AS blocking_lock_mode, 
			b.trx_started AS blocking_trx_started, 
			timediff( now(), b.trx_started ) AS blocking_trx_age, 
			b.trx_rows_locked AS blocking_trx_rows_locked, 
			b.trx_rows_modified AS blocking_trx_rows_modified, 
			tmp.sql_text as blocking_sql, 
		
			r.trx_id AS waiting_trx_id,
			r.trx_mysql_thread_id AS waiting_pid,
			rl.lock_id AS waiting_lock_id, 
			rl.lock_mode AS waiting_lock_mode, 
			r.trx_started AS waiting_trx_started, 
			timediff( now(), r.trx_started ) AS waiting_trx_age, 
			r.trx_rows_locked AS waiting_trx_rows_locked, 
			r.trx_rows_modified AS waiting_trx_rows_modified, 
			sys.format_statement ( r.trx_query ) AS waiting_query, 
				
			
			concat( 'KILL QUERY ', b.trx_mysql_thread_id,";" ) AS sql_kill_blocking_query, 
			concat( 'KILL ', b.trx_mysql_thread_id,";" ) AS sql_kill_blocking_connection  
		FROM 
			information_schema.innodb_lock_waits w 
			JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id 
			JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id 
			JOIN information_schema.innodb_locks bl ON bl.lock_id = w.blocking_lock_id 
			JOIN information_schema.innodb_locks rl ON rl.lock_id = w.requested_lock_id 
			JOIN (  SELECT t.trx_started,t.trx_id AS trx_id, e.sql_text FROM information_schema.innodb_trx t, performance_schema.events_statements_current e, performance_schema.threads c  WHERE t.trx_mysql_thread_id = c.processlist_id AND e.thread_id = c.thread_id AND t.trx_mysql_thread_id <> connection_id()) as tmp ON b.trx_id = tmp.trx_id
		) AS tmp
`
	blockedTrxInfoQuery = `
	SELECT 
		r.trx_wait_started AS summary_wait_started,  
		timestampdiff(SECOND,r.trx_wait_started,now()) AS summary_wait_secs, 
		rl.lock_table AS summary_locked_table, 
		rl.lock_index AS summary_locked_index, 
		rl.lock_type AS summary_locked_type,
	
		b.trx_id AS blocking_trx_id, 
		b.trx_mysql_thread_id AS blocking_pid, 
		bl.lock_id AS blocking_lock_id, 
		bl.lock_mode AS blocking_lock_mode, 
		b.trx_started AS blocking_trx_started, 
		timediff( now(), b.trx_started ) AS blocking_trx_age, 
		b.trx_rows_locked AS blocking_trx_rows_locked, 
		b.trx_rows_modified AS blocking_trx_rows_modified, 
		tmp.sql_text as blocking_sql, 
	
		r.trx_id AS waiting_trx_id,
		r.trx_mysql_thread_id AS waiting_pid,
		rl.lock_id AS waiting_lock_id, 
		rl.lock_mode AS waiting_lock_mode, 
		r.trx_started AS waiting_trx_started, 
		timediff( now(), r.trx_started ) AS waiting_trx_age, 
		r.trx_rows_locked AS waiting_trx_rows_locked, 
		r.trx_rows_modified AS waiting_trx_rows_modified, 
		sys.format_statement ( r.trx_query ) AS waiting_query, 
			
		
		concat( 'KILL QUERY ', b.trx_mysql_thread_id,";" ) AS sql_kill_blocking_query, 
		concat( 'KILL ', b.trx_mysql_thread_id,";" ) AS sql_kill_blocking_connection  
	FROM 
		information_schema.innodb_lock_waits w 
		JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id 
		JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id 
		JOIN information_schema.innodb_locks bl ON bl.lock_id = w.blocking_lock_id 
		JOIN information_schema.innodb_locks rl ON rl.lock_id = w.requested_lock_id 
		JOIN (  SELECT t.trx_started,t.trx_id AS trx_id, e.sql_text FROM information_schema.innodb_trx t, performance_schema.events_statements_current e, performance_schema.threads c  WHERE t.trx_mysql_thread_id = c.processlist_id AND e.thread_id = c.thread_id AND t.trx_mysql_thread_id <> connection_id()) as tmp ON b.trx_id = tmp.trx_id
	ORDER BY r.trx_wait_started
`
)

var blockedTrxDesc = prometheus.NewDesc(
	prometheus.BuildFQName(namespace, "diy", "blocked_trx_count"),
	"Query Blocking And Blocked Transcation Informations In Database",
	nil, nil,
)

type DiyScrapeBlockedTrx struct {
}

func (DiyScrapeBlockedTrx) Name() string {
	return "diy_blocked_status"
}

func (DiyScrapeBlockedTrx) Help() string {
	return "Query Blocked Transcations Infos"
}

func (DiyScrapeBlockedTrx) Version() float64 {
	return 1.0
}

func (DiyScrapeBlockedTrx) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	var blockedTrxMaxSecs int
	fmt.Println("BLocked --------------->>>>", ctx)
	if err := db.QueryRowContext(ctx, blockedTrxCountQuery).Scan(&blockedTrxMaxSecs); err != nil {
		logger.Log(err.Error())
		return err
	}

	if blockedTrxMaxSecs > 0 {
		var blockedTrxInfo struct {
			summaryWaitStartTime    string
			summaryWaitSecs         int
			summaryLockedTable      string
			summaryLockedIndex      string
			summaryLockType         string
			blokingTrxID            string
			blockingPID             string
			blockingLockID          string
			blockingLockMode        string
			blockingTrxStartTime    string
			blockingTrxAge          string
			blockingTrxRowsLocked   int
			blockingTrxRowsModified int
			blockingSQL             string
			waitingTrxID            string
			waitingPID              string
			waitingLockID           string
			waitingLockMode         string
			waitingTrxStartTime     string
			waitingTrxAge           string
			waitingTrxRowsLocked    int
			waitingTrxRowsModified  int
			waitingSQL              string
			killBlockingQuery       string
			killBlockingThread      string
		}

		blockedStatusRows, err := db.QueryContext(ctx, blockedTrxInfoQuery)
		defer blockedStatusRows.Close()

		if err != nil {
			logger.Log(err.Error())
		}

		for blockedStatusRows.Next() {
			blockedStatusRows.Scan(&blockedTrxInfo.summaryWaitStartTime,
				&blockedTrxInfo.summaryWaitSecs,
				&blockedTrxInfo.summaryLockedTable,
				&blockedTrxInfo.summaryLockedIndex,
				&blockedTrxInfo.summaryLockType,
				&blockedTrxInfo.blokingTrxID,
				&blockedTrxInfo.blockingPID,
				&blockedTrxInfo.blockingLockID,
				&blockedTrxInfo.blockingLockMode,
				&blockedTrxInfo.blockingTrxStartTime,
				&blockedTrxInfo.blockingTrxAge,
				&blockedTrxInfo.blockingTrxRowsLocked,
				&blockedTrxInfo.blockingTrxRowsModified,
				&blockedTrxInfo.blockingSQL,
				&blockedTrxInfo.waitingTrxID,
				&blockedTrxInfo.waitingPID,
				&blockedTrxInfo.waitingLockID,
				&blockedTrxInfo.waitingLockMode,
				&blockedTrxInfo.waitingTrxStartTime,
				&blockedTrxInfo.waitingTrxAge,
				&blockedTrxInfo.waitingTrxRowsLocked,
				&blockedTrxInfo.waitingTrxRowsModified,
				&blockedTrxInfo.waitingSQL,
				&blockedTrxInfo.killBlockingQuery,
				&blockedTrxInfo.killBlockingThread)
			blockInfoMsg := fmt.Sprintf(`
                出现_阻塞
-----------------------------------------
                  报告 
阻塞时长(秒)	: 				%d
加锁表		: 				%s
加锁索引		: 				%s
锁类型		: 				%s
-----------------------------------------
			   阻塞事务信息
事务ID		:				%s	
PID			:				%s
锁ID		:				%s
锁类型		:				%s
事务开始时间	:				%s
事务运行时长	:            	%s
事务持锁行数	:               %d
事务修改行数	:               %d
当前SQL语句	:               %s
-----------------------------------------
			   等待事务信息
事务ID		:            	%s
PID			:               %s
锁ID		:               %s
锁类型		:               %s
事务开始时间	:               %s
事务运行时长	:               %s
事务持锁行数	:               %d
事务修改行数	:               %d
当前SQL语句	:               %s
-----------------------------------------
			   处理语句
kill_Query	: 				%s
kill_Thread	:				%s
`, blockedTrxInfo.summaryWaitSecs,
				blockedTrxInfo.summaryLockedTable,
				blockedTrxInfo.summaryLockedIndex,
				blockedTrxInfo.summaryLockType,
				blockedTrxInfo.blokingTrxID,
				blockedTrxInfo.blockingPID,
				blockedTrxInfo.blockingLockID,
				blockedTrxInfo.blockingLockMode,
				blockedTrxInfo.blockingTrxStartTime,
				blockedTrxInfo.blockingTrxAge,
				blockedTrxInfo.blockingTrxRowsLocked,
				blockedTrxInfo.blockingTrxRowsModified,
				blockedTrxInfo.blockingSQL,
				blockedTrxInfo.waitingTrxID,
				blockedTrxInfo.waitingPID,
				blockedTrxInfo.waitingLockID,
				blockedTrxInfo.waitingLockMode,
				blockedTrxInfo.waitingTrxStartTime,
				blockedTrxInfo.waitingTrxAge,
				blockedTrxInfo.waitingTrxRowsLocked,
				blockedTrxInfo.waitingTrxRowsModified,
				blockedTrxInfo.waitingSQL,
				blockedTrxInfo.killBlockingQuery,
				blockedTrxInfo.killBlockingThread)
			tools.SendReport2Users(blockInfoMsg)
			fmt.Println(blockedTrxInfo)
		}
	}
	ch <- prometheus.MustNewConstMetric(blockedTrxDesc, prometheus.GaugeValue, float64(blockedTrxMaxSecs))

	return nil
}

var _ Scraper = DiyScrapeBlockedTrx{}
