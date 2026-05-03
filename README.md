# SQLite extension to replicate data from MySQL or MariaDB

## Installation

Download the `mysql` extension from the [releases page](https://github.com/litesql/mysql/releases).

If you want to build it yourself, install Go 1.25+ and enable CGO:

```sh
go build -ldflags="-s -w" -buildmode=c-shared -o mysql.so
```

Use:
- `.so` on Linux
- `.dylib` on macOS
- `.dll` on Windows

## Basic usage

### Prepare MySQL

1. Set the parameter `--binlog-format=ROW` when starting mysqld server

### Prepare SQLite

Load the extension:

```sh
sqlite3 example.db
.load ./mysql
SELECT mysql_info();
```

Start replication:


2. Start replication by inserting into `mysql_sub`:

```sql
INSERT INTO mysql_sub(connect, localhost, include_tables) 
VALUES(
    'mysql://root:password@127.0.0.1:3306', 
    'sqlite', 
    '^db.*'
);
```

### Revert MySQL Committed Transactions

The extension stores changes in `mysql_history`. Undo completed MySQL transactions in reverse order.

**Syntax:**

```
mysql_undo(<dsn>, <localhost>, <startSeq>, <filter>)
```

**Example:**

```sql
SELECT mysql_undo(
  'mysql://root:password@127.0.0.1:3306',
  'sqlite',
  0,
  ''
);
```

`startSeq` is the sequence number in `mysql_history`. Use `0` to revert the most recent transaction only.

You can also specify a time duration to undo transactions from that point up to now:

```sql
SELECT mysql_undo(
  'mysql://root:password@127.0.0.1:3306',
  'sqlite',
  '5m',
  '' 
);
```

The fourth parameter filters which entities to revert. You can filter by table name or by table name with a specific column value.

**Syntax:**
```
tableName[.column=value]
```

**Examples:**

1. Revert all changes from the past 5 minutes on the `users` table:

```sql
SELECT mysql_undo(
    'mysql://root:password@127.0.0.1:3306',
    'sqlite',
    '5m',
    'users' 
);
```

2. Revert all changes from the past 5 minutes on the `users` table where `id=42`:

```sql
SELECT mysql_undo(
    'mysql://root:password@127.0.0.1:3306',
    'sqlite',
    '5m',
    'users.id=42' 
);
```

## Configure replication type

| Type | Description |
|------|-------------|
| 0 | Both: data and history are recorded in SQLite |
| 1 | DataOnly: only data changes are recorded |
| 2 | HistoryOnly: only history is stored |

Example: 
To skip history logging for a subscription, set `type` when inserting into `mysql_sub`:

```sql
INSERT INTO mysql_sub(connect, localhost, include_tables, type)
VALUES(
    'mysql://root:password@127.0.0.1:3306',
    'sqlite',
    '^db.*',
    1
);
```

## Configuration

Configure replication parameters on the virtual table:

Param | Description | Default |
--- | --- | --- |
use_namespace | Keep schema/namespace instead of using the main database | false |
position_tracker_table | Table for replication position checkpoints | mysql_sub_stat |
dump_bin | mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc... If not set, ignore using mysqldump. | |
dump_db  | Restrict databases on dump | |
dump_tables | Restrict dump tables |  |
logger | Log destination: `stdout`, `stderr`, or `file:/path/to/log.txt` | |

### Debugging

Enable logging with:

```sh
export SQLITE_MYSQL_LOG=1
```

Logs will print to stderr.