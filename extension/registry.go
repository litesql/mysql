package extension

import (
	"github.com/walterwanderley/sqlite"

	"github.com/litesql/mysql/config"
)

func registerFunc(api *sqlite.ExtensionApi) (sqlite.ErrorCode, error) {
	if err := api.CreateModule(config.DefaultSubscriptionVTabName, &SubscriptionModule{}, sqlite.ReadOnly(false)); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("mysql_info", &Info{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}
	if err := api.CreateFunction("mysql_undo", &Undo{}); err != nil {
		return sqlite.SQLITE_ERROR, err
	}

	return sqlite.SQLITE_OK, nil
}
