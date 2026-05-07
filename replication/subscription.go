package replication

import (
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

type HandleChanges func(changeset []Change, currentPosition mysql.Position) error
type CheckpointLoader func() (mysql.Position, error)

type Config struct {
	DSN                string
	IncludeTablesRegex string
	ExcludeTablesRegex string
	Localhost          string
	DumpExecutionPath  string
	DumpDB             string
	DumpTables         []string
	Timeout            time.Duration
}

type Subscription struct {
	cfg          Config
	canalCfg     *canal.Config
	canal        *canal.Canal
	eventHandler canal.EventHandler
	stopped      bool
}

func Subscribe(cfg Config, handler HandleChanges, useNamespace bool) (*Subscription, error) {
	dsn, err := url.Parse(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %w", err)
	}
	if dsn.Scheme != "mysql" && dsn.Scheme != "mariadb" {
		return nil, fmt.Errorf("invalid DSN scheme: %s", dsn.Scheme)
	}

	canalCfg := canal.NewDefaultConfig()
	canalCfg.Flavor = dsn.Scheme
	canalCfg.Addr = dsn.Host
	canalCfg.User = dsn.User.Username()
	canalCfg.Password, _ = dsn.User.Password()
	if cfg.Localhost == "" {
		cfg.Localhost = "sqlite"
	}
	canalCfg.Localhost = cfg.Localhost
	if cfg.IncludeTablesRegex != "" {
		canalCfg.IncludeTableRegex = []string{cfg.IncludeTablesRegex}
	}
	if cfg.ExcludeTablesRegex != "" {
		canalCfg.ExcludeTableRegex = []string{cfg.ExcludeTablesRegex}
	}
	canalCfg.Dump.ExecutionPath = cfg.DumpExecutionPath
	canalCfg.Dump.TableDB = cfg.DumpDB
	canalCfg.Dump.Tables = cfg.DumpTables

	eventHandler := eventHandler{
		handleFn:     handler,
		useNamespace: useNamespace,
		changes:      make([]Change, 0),
		relations:    make(map[string]struct{}),
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}

	return &Subscription{
		cfg:          cfg,
		canalCfg:     canalCfg,
		eventHandler: &eventHandler,
	}, nil
}

func (s *Subscription) DSN() string {
	return s.cfg.DSN
}

func (s *Subscription) Localhost() string {
	return s.cfg.Localhost
}

func (s *Subscription) IncludeTables() string {
	return s.cfg.IncludeTablesRegex
}

func (s *Subscription) ExcludeTables() string {
	return s.cfg.ExcludeTablesRegex
}

func (s *Subscription) DumpExecutionPath() string {
	return s.cfg.DumpExecutionPath
}

func (s *Subscription) Start(logger *slog.Logger, loadCheckpoint CheckpointLoader, retry bool) error {
	if s.stopped == true {
		return nil
	}
	canal, err := canal.NewCanal(s.canalCfg)
	if err != nil {
		if retry {
			logger.Warn("failed to initialize canal, will retry", "timeout", s.cfg.Timeout, "error", err)
			time.Sleep(s.cfg.Timeout)
			return s.Start(logger, loadCheckpoint, retry)
		}
		return fmt.Errorf("failed to initialize Canal: %w", err)
	}
	canal.SetEventHandler(s.eventHandler)
	s.canal = canal

	currentPosition, err := loadCheckpoint()
	if err != nil {
		logger.Warn("failed to load previous position, starting from beginning", "error", err)
		err := s.canal.Run()
		if err != nil {
			if retry {
				logger.Warn("subscription error, will retry", "timeout", s.cfg.Timeout, "error", err)
				time.Sleep(s.cfg.Timeout)
				return s.Start(logger, loadCheckpoint, retry)
			}
			return err
		}
	}

	logger.Info("Starting replication", "position", currentPosition)
	err = s.canal.RunFrom(currentPosition)
	if err != nil {
		if retry {
			logger.Warn("subscription error, will retry", "timeout", s.cfg.Timeout, "error", err)
			time.Sleep(s.cfg.Timeout)
			return s.Start(logger, loadCheckpoint, retry)
		}
		return err
	}
	return nil
}

func (s *Subscription) Stop() {
	if s.canal != nil {
		s.stopped = true
		s.canal.Close()
	}
}

type eventHandler struct {
	canal.DummyEventHandler
	handleFn     HandleChanges
	useNamespace bool
	changes      []Change
	relations    map[string]struct{}
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {

	fullname := e.Table.Name
	if h.useNamespace {
		fullname = e.Table.String()
	}
	if _, ok := h.relations[fullname]; !ok {
		h.relations[fullname] = struct{}{}
		h.changes = append(h.changes, createTableDDL(fullname, e.Table)...)
	}

	var change Change
	if e.Header != nil {
		change.ServerTime = time.Unix(int64(e.Header.Timestamp), 0)
	}
	change.Schema = e.Table.Schema
	change.Table = e.Table.Name
	for _, col := range e.Table.Columns {
		change.ColumnNames = append(change.ColumnNames, col.Name)
	}
	change.Kind = strings.ToUpper(e.Action)
	switch e.Action {
	case canal.UpdateAction:
		change.OldKeys.KeyNames = change.ColumnNames
		oldKeys := true
		var newValues []any
		for _, row := range e.Rows {
			if oldKeys {
				change.OldKeys.KeyValues = row
				oldKeys = false
				continue
			}
			newValues = row
			oldKeys = true
			h.changes = append(h.changes, Change{
				ServerTime:   change.ServerTime,
				Kind:         change.Kind,
				Schema:       change.Schema,
				Table:        change.Table,
				ColumnNames:  change.ColumnNames,
				ColumnValues: newValues,
				OldKeys:      change.OldKeys,
			})
		}

	default:
		for _, row := range e.Rows {
			h.changes = append(h.changes, Change{
				ServerTime:   change.ServerTime,
				Kind:         change.Kind,
				Schema:       change.Schema,
				Table:        change.Table,
				ColumnNames:  change.ColumnNames,
				ColumnValues: row,
				OldKeys:      change.OldKeys,
			})
		}
	}

	return nil
}

func (h *eventHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	err := h.handleFn(h.changes, nextPos)
	h.changes = make([]Change, 0)
	return err
}

func (h *eventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, idset mysql.GTIDSet, b bool) error {
	if len(h.changes) == 0 {
		return nil
	}
	return h.handleFn(h.changes, pos)
}

func createTableDDL(fullname string, table *schema.Table) []Change {
	var changeset []Change
	var buf strings.Builder
	buf.WriteString("CREATE TABLE IF NOT EXISTS ")
	fmt.Fprintf(&buf, "`%s`(\n", fullname)
	for i, col := range table.Columns {
		if i > 0 {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(&buf, "\t%s %s", col.Name, toSqliteType(col.Type))
	}
	if len(table.PKColumns) > 0 {
		buf.WriteString(",\n")
		fmt.Fprintf(&buf, "\tPRIMARY KEY(")
		for i, pkIndex := range table.PKColumns {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(table.Columns[pkIndex].Name)
		}
		buf.WriteString(")\n")
	}
	buf.WriteString(")")

	changeset = append(changeset, Change{
		Kind: "SQL",
		SQL:  buf.String(),
	})

	for _, index := range table.Indexes {
		var buf strings.Builder
		fmt.Fprintf(&buf, "CREATE INDEX IF NOT EXISTS `%s`\n", index.Name)
		fmt.Fprintf(&buf, "\tON `%s`(%s)", fullname, strings.Join(index.Columns, ", "))
		changeset = append(changeset, Change{
			Kind: "SQL",
			SQL:  buf.String(),
		})
	}

	return changeset
}

func toSqliteType(typ int) string {
	switch typ {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		return "INTEGER"
	case schema.TYPE_FLOAT, schema.TYPE_DECIMAL:
		return "REAL"
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		return "DATETIME"
	case schema.TYPE_BIT, schema.TYPE_BINARY, schema.TYPE_POINT:
		return "BLOB"
	default:
		return "TEXT"
	}
}
