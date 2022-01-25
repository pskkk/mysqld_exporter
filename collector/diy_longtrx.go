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
	// longTrxCountQuery 统计大于10秒未提交的事务数量
	longTrxCountQuery = `
	SELECT 
		count(t.trx_id) 
	FROM 
		information_schema.innodb_trx AS t 
	WHERE 
		TIMESTAMPDIFF( SECOND, t.trx_started, NOW()) > 10
		AND 
		t.trx_mysql_thread_id <> connection_id()
`
	// longTrxInfoQuery 大于10秒未提交的事务的具体信息，写入阿里云日志服务
	longTrxInfoQuery = `
	SELECT 
		t.trx_mysql_thread_id AS connection_id, 
		t.trx_id AS trx_id, 
		t.trx_state AS trx_state, 
		t.trx_started AS trx_started, 
		ifnull( t.trx_requested_lock_id, "nil" ) AS trx_requested_lock_id, 
		ifnull( t.trx_operation_state, "nil" ) AS trx_operation_state, 
		t.trx_tables_in_use AS trx_tables_in_use, 
		t.trx_tables_locked AS trx_tables_locked, 
		t.trx_rows_locked AS trx_rows_locked, 
		t.trx_isolation_level AS trx_isolation_level, 
		t.trx_is_read_only AS trx_is_read_only, 
		e.event_name AS event_name, 
		now(), 
		TIMESTAMPDIFF( SECOND, t.trx_started, NOW()) AS timecost, 
		ifnull( t.trx_query, "nil" ) AS last_query,
		ifnull( e.sql_text, "nil" ) AS last_sqltext, 
		concat( "KILL ", t.trx_mysql_thread_id, ";" ) AS killthread, 
		concat( "KILL QUERY ", t.trx_mysql_thread_id, ";" ) AS killquery  
	FROM 
		information_schema.innodb_trx t, 
		performance_schema.events_statements_current e, 
		performance_schema.threads c  
	WHERE 
		t.trx_mysql_thread_id = c.processlist_id  
		AND 
		e.thread_id = c.thread_id 
		AND 
		t.trx_mysql_thread_id <> connection_id()
		AND
		TIMESTAMPDIFF( SECOND, t.trx_started, NOW()) > 10
	ORDER BY 
		timecost DESC
`
)

var longTrxDesc = prometheus.NewDesc(
	prometheus.BuildFQName(namespace, "diy", "long_trx_count"),
	"Query How Many Long Transcation In Database(Include not commit & blocked trx)",
	nil, nil,
)

type DiyScrapeLongTrx struct {
}

func (DiyScrapeLongTrx) Name() string {
	return "diy_long_trx_count"
}

func (DiyScrapeLongTrx) Help() string {
	return "Query How Many Long Transcation In Database(Include not commit & blocked trx)"
}

func (DiyScrapeLongTrx) Version() float64 {
	return 1.0
}

func (DiyScrapeLongTrx) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	var ltrxCounter int
	fmt.Println("Long Trx --------------->>>>", ctx.Value("hostname"))

	if err := db.QueryRowContext(ctx, longTrxCountQuery).Scan(&ltrxCounter); err != nil {
		logger.Log(err.Error()) // 输出到阿里云?,若输出到阿里云则需要区分日志内容
		return err
	}

	// 如果存在长事务，则获取长事务具体信息
	if ltrxCounter > 0 {
		var longTrxInfo struct {
			trxThreadID       string // 事务的线程ID
			trxID             string // 事务ID
			trxState          string // 事务当前状态
			trxStartTime      string // 事务启动时间
			trxRequestLockID  string // 事务所需的锁ID,若没有则为 nil
			trxOperationState string // 事务当前操作的状态
			trxTableInUse     int    // 事务打开的表数量
			trxTablesLocked   int    // 事务持有行锁的表数量，并不是表锁
			trxRowsLocked     int    // 事务持有的行锁数量(可能包含delete标志的待回收行)
			trxIsolationLevel string // 事务隔离级别
			trxIsReadOnly     int    // set session transaction read only; 事务内只能进行只读操作
			eventName         string // event 名称
			nowTime           string // 当前时间
			timecost          int    // 事务开始至今的时间，单位 秒
			sqlQuery          string // 当前正在执行的Query 内容,若没有则为 nil
			sqlText           string // 执行的最新SQL内容(sqlQuery 为nil , sqlText 不为nil ，则说明事务当前均执行完，但未结束，若sqlQuery与sqlText 均为nil ，则可能query未正常结束)
			killThread        string // 杀线程语句
			killQuery         string // 杀查询语句
		}

		longTrxInfoRows, err := db.QueryContext(ctx, longTrxInfoQuery)
		defer longTrxInfoRows.Close()

		if err != nil {
			logger.Log(err.Error()) // 输出到阿里云?,若输出到阿里云则需要区分日志内容;此处报错 只记录，而不是 return err 退出 采集器
		}
		for longTrxInfoRows.Next() { // 长事务可能不止一个
			longTrxInfoRows.Scan(&longTrxInfo.trxThreadID,
				&longTrxInfo.trxID,
				&longTrxInfo.trxState,
				&longTrxInfo.trxStartTime,
				&longTrxInfo.trxRequestLockID,
				&longTrxInfo.trxOperationState,
				&longTrxInfo.trxTableInUse,
				&longTrxInfo.trxTablesLocked,
				&longTrxInfo.trxRowsLocked,
				&longTrxInfo.trxIsolationLevel,
				&longTrxInfo.trxIsReadOnly,
				&longTrxInfo.eventName,
				&longTrxInfo.nowTime,
				&longTrxInfo.timecost,
				&longTrxInfo.sqlQuery,
				&longTrxInfo.sqlText,
				&longTrxInfo.killThread,
				&longTrxInfo.killQuery)
			longTrxInfoMsg := fmt.Sprintf(`
				出现_长事务
-----------------------------------------
线程ID				:		%s
事务ID             	:		%s
状态             	:       %s
启动时间           	:       %s
所需锁ID          	:       %s
操作状态           	:       %s
打开表				:       %d
持有行锁的表数量		:       %d
持有行锁数量         	:       %d
隔离级别           	:       %s
是否为只读事务      	:       %d
event名称        	:       %s
当前时间           	:       %s
运行时间(秒)			:       %d
当前执行SQL        	:       %s
上一个执行完的SQL		:       %s
------------------------------------------
			     处理语句
Kill_Thread         :       %s
Kill_Query          :       %s
`, longTrxInfo.trxThreadID,
				longTrxInfo.trxID,
				longTrxInfo.trxState,
				longTrxInfo.trxStartTime,
				longTrxInfo.trxRequestLockID,
				longTrxInfo.trxOperationState,
				longTrxInfo.trxTableInUse,
				longTrxInfo.trxTablesLocked,
				longTrxInfo.trxRowsLocked,
				longTrxInfo.trxIsolationLevel,
				longTrxInfo.trxIsReadOnly,
				longTrxInfo.eventName,
				longTrxInfo.nowTime,
				longTrxInfo.timecost,
				longTrxInfo.sqlQuery,
				longTrxInfo.sqlText,
				longTrxInfo.killThread,
				longTrxInfo.killQuery)
			tools.SendReport2Users(longTrxInfoMsg)
			fmt.Println(longTrxInfo) // 输出阿里云日志

		}
		fmt.Println("--------------------------------")
	}

	ch <- prometheus.MustNewConstMetric(longTrxDesc, prometheus.GaugeValue, float64(ltrxCounter))

	return nil
}

var _ Scraper = DiyScrapeLongTrx{}
