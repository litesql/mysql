package extension

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/walterwanderley/sqlite"

	"github.com/litesql/mysql/config"
)

type SubscriptionModule struct {
}

func (m *SubscriptionModule) Connect(conn *sqlite.Conn, args []string, declare func(string) error) (sqlite.VirtualTable, error) {
	virtualTableName := args[2]
	if virtualTableName == "" {
		virtualTableName = config.DefaultSubscriptionVTabName
	}

	var (
		useNamespace         bool
		positionTrackerTable string
		dumpExecutionPath    string
		dumpDB               string
		dumpTables           []string

		logger string
		err    error
	)
	if len(args) > 3 {
		for _, opt := range args[3:] {
			k, v, ok := strings.Cut(opt, "=")
			if !ok {
				return nil, fmt.Errorf("invalid option: %q", opt)
			}
			k = strings.TrimSpace(k)
			v = sanitizeOptionValue(v)

			switch strings.ToLower(k) {
			case config.UseNamespace:
				b, err := strconv.ParseBool(v)
				if err != nil {
					return nil, fmt.Errorf("invalid %q option: %w", k, err)
				}
				useNamespace = b
			case config.PositionTrackerTable:
				positionTrackerTable = v
			case config.DumpExecutionPath:
				dumpExecutionPath = v
			case config.DumpDB:
				dumpDB = v
			case config.DumpTables:
				dumpTables = strings.Split(v, ",")
			case config.Logger:
				logger = v
			}
		}
	}

	if positionTrackerTable == "" {
		positionTrackerTable = config.DefaultPositionTrackerTabName
	}

	err = conn.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s(
		   	localhost TEXT PRIMARY KEY,
		   	position TEXT,
		   	server_time TEXT
		   )`, positionTrackerTable), nil)
	if err != nil {
		return nil, fmt.Errorf("creating %q table: %w", positionTrackerTable, err)
	}

	historyTable := "mysql_history"
	err = conn.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s(
		   	seq INTEGER PRIMARY KEY AUTOINCREMENT,
		   	localhost TEXT,
		   	position TEXT,
		   	server_time TEXT,
		   	changeset JSONB
		   )`, historyTable), nil)
	if err != nil {
		return nil, fmt.Errorf("creating %q table: %w", positionTrackerTable, err)
	}

	vtab, err := NewSubscriptionVirtualTable(virtualTableName, conn, positionTrackerTable, useNamespace, dumpExecutionPath, dumpDB, dumpTables, logger)
	if err != nil {
		return nil, err
	}
	return vtab, declare("CREATE TABLE x(connect TEXT, localhost TEXT, include_tables TEXT, exclude_tables TEXT, type INTEGER)")
}

func sanitizeOptionValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, "'")
	v = strings.TrimSuffix(v, "'")
	v = strings.TrimPrefix(v, "\"")
	v = strings.TrimSuffix(v, "\"")
	return os.ExpandEnv(v)
}
