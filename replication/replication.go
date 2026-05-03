package replication

import (
	"net/url"
	"slices"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"
)

type commandAndParams struct {
	SQL    string
	Params []any
}

func RevertChangeSet(dsn string, changeset []Change) error {
	u, err := url.Parse(dsn)
	if err != nil {
		return err
	}
	pass, _ := u.User.Password()
	conn, err := client.Connect(u.Host, u.User.Username(), pass, strings.TrimPrefix(u.Path, "/"))
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.Begin(); err != nil {
		return err
	}
	defer conn.Rollback()

	commands := make([]commandAndParams, 0, len(changeset))
	for _, change := range changeset {
		sql, params := change.RevertSQL()
		if sql == "" {
			continue
		}
		commands = append(commands, commandAndParams{
			SQL:    sql,
			Params: params,
		})
	}
	slices.Reverse(commands)
	for _, cmd := range commands {
		_, err := conn.Execute(cmd.SQL, cmd.Params...)
		if err != nil {
			return err
		}
	}
	return conn.Commit()
}
