package extension

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/litesql/mysql/replication"
	"github.com/walterwanderley/sqlite"
)

type Undo struct {
}

func (m *Undo) Args() int {
	return 4
}

func (m *Undo) Deterministic() bool {
	return false
}

func (m *Undo) Apply(ctx *sqlite.Context, values ...sqlite.Value) {
	conn := ctx.GetConnection()
	dsn := values[0].Text()
	localhost := values[1].Text()
	var changeset []replication.Change
	if values[2].Type() == sqlite.SQLITE_INTEGER {
		startSeq := values[2].Int()
		if startSeq == 0 {
			conn.Exec("SELECT max(seq) FROM mysql_history WHERE localhost = ?", func(stmt *sqlite.Stmt) error {
				startSeq = int(stmt.GetInt64("seq"))
				return nil
			}, localhost)
		}
		var err error
		changeset, err = m.changeSet(conn, "SELECT changeset FROM mysql_history WHERE localhost = ? AND seq >= ? ORDER BY seq ASC", localhost, startSeq)
		if err != nil {
			ctx.ResultError(err)
			return
		}
	} else if values[2].Type() == sqlite.SQLITE_TEXT {
		timeAgo, err := time.ParseDuration(values[2].Text())
		if err != nil {
			ctx.ResultError(err)
			return
		}
		cutoff := time.Now().Add(-timeAgo)
		changeset, err = m.changeSet(conn, "SELECT changeset FROM mysql_history WHERE localhost = ? AND timestamp >= ? ORDER BY seq ASC", localhost, cutoff)
		if err != nil {
			ctx.ResultError(err)
			return
		}
	}
	if values[3].Text() != "" {
		//pattern tableName.columnName=value
		tableName, col, _ := strings.Cut(values[3].Text(), ".")
		var value string
		if col != "" {
			col, value, _ = strings.Cut(col, "=")
		}
		var filtered []replication.Change
		for _, change := range changeset {
			if strings.EqualFold(change.Table, tableName) {
				if col != "" {
					for i, column := range change.ColumnNames {
						if strings.EqualFold(column, col) {
							if strings.EqualFold(fmt.Sprint(change.ColumnValues[i]), value) {
								filtered = append(filtered, change)
								break
							}
							if len(change.OldKeys.KeyValues) > i && strings.EqualFold(fmt.Sprint(change.OldKeys.KeyValues[i]), value) {
								filtered = append(filtered, change)
								break
							}
						}
					}
					continue
				}
				filtered = append(filtered, change)
			}
		}
		changeset = filtered
	}
	if len(changeset) == 0 {
		ctx.ResultText(`{"status": "no changes to revert"}`)
		return
	}

	err := replication.RevertChangeSet(dsn, changeset)
	if err != nil {
		ctx.ResultError(err)
		return
	}
	ctx.ResultText(`{"status": "successfully reverted changes"}`)
}

func (m *Undo) changeSet(conn *sqlite.Conn, query string, args ...interface{}) ([]replication.Change, error) {
	list := make([]replication.Change, 0)
	conn.Exec(query, func(stmt *sqlite.Stmt) error {
		var changes []replication.Change
		reader := stmt.GetReader("changeset")
		err := json.NewDecoder(reader).Decode(&changes)
		if err != nil {
			return err
		}
		list = append(list, changes...)
		return nil
	}, args...)

	return list, nil
}
