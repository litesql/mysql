package extension

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/mysql/replication"
)

type ReplicationType int

const (
	ReplicationTypeBoth ReplicationType = iota
	ReplicationTypeDataOnly
	ReplicationTypeHistoryOnly
)

type subscriptionItem struct {
	*replication.Subscription
	replicationType ReplicationType
}

type SubscriptionVirtualTable struct {
	virtualTableName   string
	conn               *sqlite.Conn
	subscriptions      []*subscriptionItem
	useNamespace       bool
	stmtMu             sync.Mutex
	loadPositionStmt   *sqlite.Stmt
	updatePositionStmt *sqlite.Stmt
	historyStmt        *sqlite.Stmt
	dumpExecutionPath  string
	dumpDB             string
	dumpTables         []string
	mu                 sync.Mutex
	logger             *slog.Logger
	loggerCloser       io.Closer
}

func NewSubscriptionVirtualTable(virtualTableName string, conn *sqlite.Conn,
	positionTrackerTable string, useNamespace bool, dumpExecutionPath string,
	dumpDB string, dumpTables []string, loggerDef string) (*SubscriptionVirtualTable, error) {
	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}
	loadStmt, _, err := conn.Prepare("SELECT position FROM " + positionTrackerTable + " WHERE localhost = ?")
	if err != nil {
		return nil, fmt.Errorf("preparing position loader statement: %w", err)
	}
	updateStmt, _, err := conn.Prepare("REPLACE INTO " + positionTrackerTable + "(localhost, position, server_time) VALUES(?, ?, ?)")
	if err != nil {
		return nil, fmt.Errorf("preparing position tracker statement: %w", err)
	}
	historyStmt, _, err := conn.Prepare("INSERT INTO mysql_history(localhost, position, server_time, changeset) VALUES(?, ?, ?, ?)")
	if err != nil {
		return nil, fmt.Errorf("preparing history statement: %w", err)
	}

	return &SubscriptionVirtualTable{
		virtualTableName:   virtualTableName,
		conn:               conn,
		useNamespace:       useNamespace,
		loadPositionStmt:   loadStmt,
		updatePositionStmt: updateStmt,
		historyStmt:        historyStmt,
		dumpExecutionPath:  dumpExecutionPath,
		dumpDB:             dumpDB,
		dumpTables:         dumpTables,
		logger:             logger,
		loggerCloser:       loggerCloser,
		subscriptions:      make([]*subscriptionItem, 0),
	}, nil
}

func (vt *SubscriptionVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{EstimatedCost: 1000000}, nil
}

func (vt *SubscriptionVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newSubscriptionsCursor(vt.subscriptions), nil
}

func (vt *SubscriptionVirtualTable) Disconnect() error {
	var err error
	for _, subscription := range vt.subscriptions {
		subscription.Stop()
	}
	err = errors.Join(err, vt.loadPositionStmt.Finalize(), vt.updatePositionStmt.Finalize(), vt.historyStmt.Finalize())
	if vt.loggerCloser != nil {
		err = errors.Join(err, vt.loggerCloser.Close())
	}
	return err
}

func (vt *SubscriptionVirtualTable) Destroy() error {
	return nil
}

func (vt *SubscriptionVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	if len(values) < 1 || values[0].Text() == "" {
		return 0, fmt.Errorf("connect is required")
	}
	dsn := values[0].Text()
	localhost := "sqlite"
	if len(values) > 1 && values[1].Text() != "" {
		localhost = values[1].Text()
	}
	var includeTables string
	if len(values) > 2 {
		includeTables = values[2].Text()
	}

	var excludeTables string
	if len(values) > 3 {
		excludeTables = values[3].Text()
	}

	cfg := replication.Config{
		DSN:                dsn,
		Localhost:          localhost,
		IncludeTablesRegex: includeTables,
		ExcludeTablesRegex: excludeTables,
		DumpExecutionPath:  vt.dumpExecutionPath,
		DumpDB:             vt.dumpDB,
		DumpTables:         vt.dumpTables,
	}

	vt.mu.Lock()
	defer vt.mu.Unlock()
	if vt.contains(localhost) {
		return 0, fmt.Errorf("already using the %q localhost", localhost)
	}

	var replType ReplicationType
	if len(values) > 4 && values[4].Type() == sqlite.SQLITE_INTEGER {
		replType = ReplicationType(values[4].Int())
		if replType < ReplicationTypeBoth || replType > ReplicationTypeHistoryOnly {
			return 0, fmt.Errorf("invalid replication type %d. Both = 0, DataOnly = 1, HistoryOnly = 2", replType)
		}
	}
	subscription, err := replication.Subscribe(cfg, vt.handler(localhost, replType), vt.useNamespace)
	if err != nil {
		return 0, err
	}
	vt.subscriptions = append(vt.subscriptions, &subscriptionItem{
		Subscription:    subscription,
		replicationType: replType,
	})

	go func() {
		err := subscription.Start(vt.logger, vt.loader(localhost), false)
		if err != nil {
			vt.logger.Error("failed to subscribe", "error", err)
		}
	}()

	return 1, nil
}

func (vt *SubscriptionVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q are not supported", vt.virtualTableName)
}

func (vt *SubscriptionVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("REPLACE operations on %q are not supported", vt.virtualTableName)
}

func (vt *SubscriptionVirtualTable) Delete(v sqlite.Value) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	index := v.Int()
	// slices are 0 based
	index--

	if index >= 0 && index < len(vt.subscriptions) {
		rh := vt.subscriptions[index]
		rh.Stop()
		vt.subscriptions = slices.Delete(vt.subscriptions, index, index+1)
	}

	return nil
}

func (vt *SubscriptionVirtualTable) contains(localhost string) bool {
	for _, rh := range vt.subscriptions {
		if rh.Localhost() == localhost {
			return true
		}
	}
	return false
}

func (vt *SubscriptionVirtualTable) loader(localhost string) replication.CheckpointLoader {
	return func() (mysql.Position, error) {
		err := vt.loadPositionStmt.Reset()
		if err != nil {
			return mysql.Position{}, err
		}
		vt.loadPositionStmt.BindText(1, localhost)
		hasRow, err := vt.loadPositionStmt.Step()
		if err != nil {
			return mysql.Position{}, fmt.Errorf("loading last position for localhost %q: %w", localhost, err)
		}
		if hasRow {
			positionStr := vt.loadPositionStmt.ColumnText(0)
			var position mysql.Position
			err := json.Unmarshal([]byte(positionStr), &position)
			if err != nil {
				return mysql.Position{}, fmt.Errorf("parsing last position %q for localhost %q: %w", positionStr, localhost, err)
			}
			vt.logger.Info("loaded last saved position", "localhost", localhost, "position", position)
			return position, nil
		}
		return mysql.Position{}, fmt.Errorf("no checkpoint found")

	}
}

func (vt *SubscriptionVirtualTable) handler(localhost string, replType ReplicationType) replication.HandleChanges {
	return func(changeset []replication.Change, currentPosition mysql.Position) error {
		vt.stmtMu.Lock()
		defer vt.stmtMu.Unlock()

		vt.logger.Debug("applying changeset", "localhost", localhost, "current_position", currentPosition)
		err := vt.conn.Exec("BEGIN IMMEDIATE", nil)
		if err != nil {
			return err
		}
		defer vt.conn.Exec("ROLLBACK", nil)

		var serverTime time.Time
		if replType == ReplicationTypeBoth || replType == ReplicationTypeDataOnly {
			for _, change := range changeset {
				serverTime = change.ServerTime
				tableName := change.Table
				if vt.useNamespace {
					if change.Schema == "db" {
						change.Schema = "main"
					}
					tableName = fmt.Sprintf("%s.%s", change.Schema, change.Table)
				}
				var sql string
				switch change.Kind {
				case "INSERT":
					sql = fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES (%s)", tableName, strings.Join(change.ColumnNames, ", "), placeholders(len(change.ColumnValues)))
					err = vt.conn.Exec(sql, nil, change.ColumnValues...)
				case "UPDATE":
					setClause := make([]string, len(change.ColumnNames))
					for i, col := range change.ColumnNames {
						setClause[i] = fmt.Sprintf("%s = ?", col)
					}
					if len(change.OldKeys.KeyNames) == 0 {
						return fmt.Errorf("missing old keys for update on table %s.%s", change.Schema, change.Table)
					}
					var args []any
					args = append(args, change.ColumnValues...)
					whereClause := make([]string, len(change.OldKeys.KeyNames))
					for i, col := range change.OldKeys.KeyNames {
						if change.OldKeys.KeyValues[i] == nil {
							whereClause[i] = fmt.Sprintf("%s IS NULL", col)
						} else {
							args = append(args, change.OldKeys.KeyValues[i])
							whereClause[i] = fmt.Sprintf("%s = ?", col)
						}
					}
					sql = fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", tableName, strings.Join(setClause, ", "), strings.Join(whereClause, " AND "))
					err = vt.conn.Exec(sql, nil, args...)
				case "DELETE":
					whereClause := make([]string, len(change.ColumnNames))
					var args []any
					for i, col := range change.ColumnNames {
						if change.ColumnValues[i] == nil {
							whereClause[i] = fmt.Sprintf("%s IS NULL", col)
						} else {
							args = append(args, change.ColumnValues[i])
							whereClause[i] = fmt.Sprintf("%s = ?", col)
						}
					}
					sql = fmt.Sprintf("DELETE FROM `%s` WHERE %s", tableName, strings.Join(whereClause, " AND "))
					err = vt.conn.Exec(sql, nil, args...)
				case "SQL":
					err = vt.conn.Exec(change.SQL, nil)
				default:
					continue
				}
				if err != nil {
					vt.logger.Error("failed to exec statement", "sql", sql, "error", err)
					return fmt.Errorf("failed to exec %q: %w", sql, err)
				}
			}
		}
		err = vt.updatePositionStmt.Reset()
		if err != nil {
			vt.logger.Error("failed to reset position tracker statement", "localhost", localhost, "position", currentPosition, "error", err)
			return err
		}
		vt.updatePositionStmt.BindText(1, localhost)
		positionJson, _ := json.Marshal(currentPosition)
		vt.updatePositionStmt.BindText(2, string(positionJson))
		vt.updatePositionStmt.BindText(3, serverTime.Format(time.RFC3339))

		_, err = vt.updatePositionStmt.Step()
		if err != nil {
			vt.logger.Error("failed to update position tracker", "localhost", localhost, "position", currentPosition, "error", err)
			return fmt.Errorf("updating position tracker: %w", err)
		}

		if replType == ReplicationTypeBoth || replType == ReplicationTypeHistoryOnly {
			err = vt.historyStmt.Reset()
			if err != nil {
				vt.logger.Error("failed to reset history statement", "localhost", localhost, "position", currentPosition, "error", err)
				return err
			}
			changesetJson, err := json.Marshal(changeset)
			if err != nil {
				vt.logger.Error("failed to marshal changeset for history record", "localhost", localhost, "position", currentPosition, "error", err)
				return fmt.Errorf("marshaling changeset for history record: %w", err)
			}
			vt.historyStmt.BindText(1, localhost)
			vt.historyStmt.BindText(2, currentPosition.String())
			vt.historyStmt.BindText(3, serverTime.Format(time.RFC3339))
			vt.historyStmt.BindText(4, string(changesetJson))

			_, err = vt.historyStmt.Step()
			if err != nil {
				vt.logger.Error("failed to insert history record", "localhost", localhost, "position", currentPosition, "error", err)
				return fmt.Errorf("inserting history record: %w", err)
			}
		}

		err = vt.conn.Exec("COMMIT", nil)
		if err != nil {
			vt.logger.Error("commit changeset", "position", currentPosition, "error", err)
			return err
		}

		return nil
	}
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	var b strings.Builder
	for i := range n {
		b.WriteString(fmt.Sprintf("?%d,", i+1))
	}
	return strings.TrimRight(b.String(), ",")
}

type subscriptionsCursor struct {
	data    []*subscriptionItem
	current *subscriptionItem // current row that the cursor points to
	rowid   int64             // current rowid .. negative for EOF
}

func newSubscriptionsCursor(data []*subscriptionItem) *subscriptionsCursor {
	slices.SortFunc(data, func(a, b *subscriptionItem) int {
		return cmp.Compare(a.Localhost(), b.Localhost())
	})
	return &subscriptionsCursor{
		data: data,
	}
}

func (c *subscriptionsCursor) Next() error {
	// EOF
	if c.rowid < 0 || int(c.rowid) >= len(c.data) {
		c.rowid = -1
		return sqlite.SQLITE_OK
	}
	// slices are zero based
	c.current = c.data[c.rowid]
	c.rowid += 1

	return sqlite.SQLITE_OK
}

func (c *subscriptionsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		ctx.ResultText(c.current.DSN())
	case 1:
		ctx.ResultText(c.current.Localhost())
	case 2:
		ctx.ResultText(c.current.IncludeTables())
	case 3:
		ctx.ResultText(c.current.ExcludeTables())
	case 4:
		ctx.ResultInt(int(c.current.replicationType))
	default:
		return fmt.Errorf("invalid column index %d", i)
	}
	return nil
}

func (c *subscriptionsCursor) Filter(int, string, ...sqlite.Value) error {
	c.rowid = 0
	return c.Next()
}

func (c *subscriptionsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *subscriptionsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *subscriptionsCursor) Close() error {
	return nil
}
