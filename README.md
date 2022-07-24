# sqlwrapper

A tiny wrapper for golang's `database/sql` package that provides transaction management and retry functionality.

> The retry wrapper uses golang's built-in retry and exponential backoff strategy.

## Usage

> go get github.com/yang-tk/go-sqlwrapper

### Initialization

Initialize using default configurations

```go
func SetupDB() {
    db, err := sql.Open("mysql", "dsn string...")
	w := sqlwrapper.New(db)
}	
```

Or initialize using a custom retry options

```go
func SetupDB() {
    db, err := sql.Open("mysql", "dsn string...")
    retryOpts := sqlwrapper.RetryOptions{
        MaxRetries:   3,                               // max retry attempts
        InitialDelay: time.Duration(5) * time.Second,  // 5 secs delay after each iteration
        Factor:       2,                               // multiply factor
        MaxDelay:     time.Duration(60) * time.Second, // max 60 secs delay
        Jitter:       func() bool { return true },     // disable jitter
    }
	w := sqlwrapper.NewWithOptions(db, retryOpts) 
}	
```

### DB Operations

The package has implemented the following `*sql.DB` operations with retry wrapper.

```code
Exec(query string, args ...any) (sql.Result, error)
ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
Query(query string, args ...any) (*sql.Rows, error)
QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
QueryRow(query string, args ...any) *sql.Row
QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
```

To use

```go
func Insert() error {
	db, err := sql.Open("mysql", "dsn string...")
	if err != nil {
		return err
	}
	
	w := sqlwrapper.New(db)
	// same as *sql.DB.Exec but with retry support
	err = w.Exec(`INSERT INTO table_name (name) VALUES (?)`, "column value")
	if err != nil {
		return err
	}

	return nil
}
```

### Transactions

```go
func BatchHandler() error {
	db, err := sql.Open("mysql", "dsn string...")
	if err != nil {
		return err
	}

	w := sqlwrapper.New(db)
	err = w.TransactWithRetry(func(tx *sql.Tx) error {
		var e error
		// e will be bubbled up to err and to determine if the transaction should roll back or commit
		_, e = tx.Exec(`INSERT INTO table_name (name) VALUES (?)`, "column value 1")
		if e != nil {
			return e
		}

        _, e = tx.Exec(`INSERT INTO table_name (name) VALUES (?)`, "column value 2")
        if e != nil {
            return e
        }

		return nil
	})

	// bubble up err from the transaction
	return err
}
```

### Custom Retry Options Per Request

If you want to use different retry policy per handler/request, the following methods are supported:

```code
ExecWithOptions(retryOpts RetryOptions, query string, args ...any) (sql.Result, error)
ExecContextWithOptions(ctx context.Context, retryOpts RetryOptions, query string, args ...any) (sql.Result, error)
QueryWithOptions(retryOpts RetryOptions, query string, args ...any) (*sql.Rows, error)
QueryContextWithOptions(ctx context.Context, retryOpts RetryOptions, query string, args ...any) (*sql.Rows, error)
QueryRowWithOptions(retryOpts RetryOptions, query string, args ...any)
QueryRowContextWithOptions(ctx context.Context, retryOpts RetryOptions, query string, args ...any) *sql.Row
TransactWithRetryOptions(retryOpts RetryOptions, fn func(*sql.Tx) error) error
TransactContextWithRetryOptions(ctx context.Context, txOpts *sql.TxOptions, retryOpts RetryOptions, fn func(*sql.Tx) error) error
```

