package main

import (
	"context"
	"database/sql"
	"gopkg.in/retry.v1"
	"time"
)

// DefaultMaxRetries represents the default max attempts for retry and is used for the default SQLWrapper
var DefaultMaxRetries = 3

// DefaultInitialDelay represents the default initial delay after each iteration and is used for the default SQLWrapper
var DefaultInitialDelay = time.Duration(5) * time.Second

// DefaultFactor represents the default multiply factor for the backoff and is used for the default SQLWrapper
var DefaultFactor = float64(2)

// DefaultMaxDelay represents the maximum delay after each iteration and is used for the default SQLWrapper
var DefaultMaxDelay = time.Duration(60) * time.Second

// DefaultJitter represents whether the jitter will be added for each retry interval and is used for the default SQLWrapper
var DefaultJitter = true

// RetryOptions represents the retry options that can be configured
//
// MaxRetries holds the maximum number of attempts allowed
// InitialDelay represents the initial delay for each iteration
// Factor will be multiplied to the delay after each iteration
// MaxDelay represents the maximum delay after each iteration
// Jitter determines if jitter will be added to the interval
//
// Example:
//
// 		j := false
//		var retryOptions = sqlwrapper.RetryOptions{
//					MaxRetries:   3,
//					InitialDelay: time.Duration(3) * time.Second,
//					Factor:       2,
//					MaxDelay:     time.Duration(60) * time.Secondy,
//					Jitter:       &j,
//		}
type RetryOptions struct {
	MaxRetries   int
	InitialDelay time.Duration
	Factor       float64
	MaxDelay     time.Duration
	Jitter       *bool
}

// SQLWrapper wraps the built-in *sql.DB along with retry options
type SQLWrapper struct {
	// db is not exported but can be unwrapped using UnWrap method to access the original *sql.DB client
	db *sql.DB

	// retryOpts is not exported. To use different retry on each request, use TransactWithRetryOptions method
	retryOpts RetryOptions
}

// New builds a new SQLWrapper with default configurations
//
// Example:
// 		db, err := sql.Open("mysql", "datasource url")
//		w := sqlwrapper.New(db)
func New(db *sql.DB) *SQLWrapper {
	return &SQLWrapper{
		db: db,
		retryOpts: RetryOptions{
			MaxRetries:   DefaultMaxRetries,
			InitialDelay: DefaultInitialDelay,
			Factor:       DefaultFactor,
			MaxDelay:     DefaultMaxDelay,
			Jitter:       &DefaultJitter,
		},
	}
}

// NewWithOptions builds new SQLWrapper using provided options, if any of the attribute was not provided,
// default value will be used.
//
// Example:
//      jitter := false
//		var retryOptions = sqlwrapper.RetryOptions{
//					MaxRetries:   3,
//					InitialDelay: time.Duration(5) * time.Second,
//					Factor:       2,
//					MaxDelay:     time.Duration(60) * time.Second,
//					Jitter:       &jitter,
//		}
//		w := sqlwrapper.NewWithOptions(db, retryOptions)
func NewWithOptions(db *sql.DB, retryOpts RetryOptions) *SQLWrapper {
	if retryOpts.MaxRetries == 0 {
		retryOpts.MaxRetries = DefaultMaxRetries
	}
	if retryOpts.InitialDelay == 0 {
		retryOpts.InitialDelay = DefaultInitialDelay
	}
	if retryOpts.Factor == 0 {
		retryOpts.Factor = DefaultFactor
	}
	if retryOpts.MaxDelay == 0 {
		retryOpts.MaxDelay = DefaultMaxDelay
	}
	if retryOpts.Jitter == nil {
		retryOpts.Jitter = &DefaultJitter
	}

	return &SQLWrapper{
		db:        db,
		retryOpts: retryOpts,
	}
}

// Retry wraps gopkg.in/retry that retrys a function using exponential backoff strategy.
func Retry(retryOpts RetryOptions, fn func() error) error {
	backoff := retry.Exponential{
		Initial:  retryOpts.InitialDelay,
		Factor:   retryOpts.Factor,
		MaxDelay: retryOpts.MaxDelay,
		Jitter:   *retryOpts.Jitter,
	}
	strategy := retry.LimitCount(retryOpts.MaxRetries, backoff)

	var err error
	for a := retry.Start(strategy, nil); a.Next(); {
		err = fn()
		// if the error returned is nil from the caller's function, retry will be terminated
		if err == nil {
			break
		}
	}
	return err
}

// Exec has same functionality as built-in *sql.DB.Exec() along with retry functionality
//
// Example:
//
// 		w := sqlwrapper.New(db)
//		err = w.Exec(`INSERT INTO table_name (attr_name) VALUES ($1)`, "Test attr")
func (w *SQLWrapper) Exec(query string, args ...any) (sql.Result, error) {
	return w.ExecContext(context.Background(), query, args...)
}

// ExecContext has same functionality as built-in *sql.DB.ExecContext() along with retry functionality
//
// Example:
// 		ctx := context.Background()
// 		w := sqlwrapper.New(db)
//		err = w.ExecContext(ctx, `INSERT INTO table_name (attr_name) VALUES ($1)`, "Test attr")
func (w *SQLWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return w.ExecContextWithOptions(ctx, w.retryOpts, query, args...)
}

// ExecWithOptions executes a query using provided retry options
func (w *SQLWrapper) ExecWithOptions(retryOpts RetryOptions, query string, args ...any) (sql.Result, error) {
	return w.ExecContextWithOptions(context.Background(), retryOpts, query, args...)
}

// ExecContextWithOptions executes a query with context and custom retry options
func (w *SQLWrapper) ExecContextWithOptions(ctx context.Context, retryOpts RetryOptions, query string, args ...any) (sql.Result, error) {
	var res sql.Result
	var e error
	err := Retry(retryOpts, func() error {
		res, e = w.db.ExecContext(ctx, query, args...)
		return e
	})
	return res, err
}

// Query has same functionality as built-in *sql.DB.Query along with retry functionality
//
// Example:
// 		w := sqlwrapper.New(db)
//		rows, err = w.QueryContext(`SELECT * FROM table_name WHERE attr_name = $1`, "Test attr")
func (w *SQLWrapper) Query(query string, args ...any) (*sql.Rows, error) {
	return w.QueryContext(context.Background(), query, args...)
}

// QueryContext has same functionality as built-in *sql.DB.QuerContext along with retry functionality
// Example:
// 		ctx := context.Background()
// 		w := sqlwrapper.New(db)
//		rows, err = w.QueryContext(ctx, `SELECT * FROM table_name WHERE attr_name = $1`, "Test attr")
func (w *SQLWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return w.QueryContextWithOptions(ctx, w.retryOpts, query, args...)
}

// QueryWithOptions execute a query using custom retry options that return rows
func (w *SQLWrapper) QueryWithOptions(retryOpts RetryOptions, query string, args ...any) (*sql.Rows, error) {
	return w.QueryContextWithOptions(context.Background(), retryOpts, query, args...)
}

// QueryContextWithOptions execute a query using custom retry options that return rows
func (w *SQLWrapper) QueryContextWithOptions(ctx context.Context, retryOpts RetryOptions, query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	var e error
	err := Retry(retryOpts, func() error {
		rows, e = w.db.QueryContext(ctx, query, args...)
		return e
	})
	return rows, err
}

// QueryRow has same functionality as built-in *sql.DB.QueryRow along with retry functionality
//
// Example:
//		var u User // a user struct
// 		w := sqlwrapper.New(db)
//		err = w.QueryRow(`SELECT * FROM user_table WHERE user_id = $1`, "1").Scan(&s.username)
func (w *SQLWrapper) QueryRow(query string, args ...any) *sql.Row {
	return w.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext has same functionality as built-in *sql.DB.QueryRowContext along with retry functionality
//
// Notice that *sql.DB.QueryRowContext() has 3 error checks (from golang doc):
//		switch {
//				case err == sql.ErrNoRows:
//				case err != nil:
//				default:
//				// ...safe to use
//		}
//
// If there are no matching row returned, the retry will be terminated.
// The only case the retry will keep running is when the error is neither sql.ErrNoRows or no errors happened.
// E.g. An invalid sql query or args during the execution.
//
// Example:
//		var u User // a user struct
// 		w := sqlwrapper.New(db)
//		err = w.QueryRowContext(ctx, `SELECT * FROM user_table WHERE user_id = $1`, "1").Scan(&s.username)
func (w *SQLWrapper) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return w.QueryRowContextWithOptions(ctx, w.retryOpts, query, args...)
}

func (w *SQLWrapper) QueryRowWithOptions(retryOpts RetryOptions, query string, args ...any) *sql.Row {
	return w.QueryRowContextWithOptions(context.Background(), retryOpts, query, args...)
}

func (w *SQLWrapper) QueryRowContextWithOptions(ctx context.Context, retryOpts RetryOptions, query string, args ...any) *sql.Row {
	var res *sql.Row
	var e error
	// Nothing need to be returned because QueryRowContext always returns a non-nil *sql.Row value
	// which will be re-assigned to res
	_ = Retry(retryOpts, func() error {
		// *sql.DB.QueryRowContext always returns a non-nil *sql.Row type
		// Need to unwrap the value to get the error
		res = w.db.QueryRowContext(ctx, query, args...)
		e = res.Err()
		return e
	})
	return res
}

// Transact is a wrapper for *sql.DB transaction management, the error returned from *sql.Tx is being used to
// determine whether the transaction being rolled-back or commited.
//
// Example:
// 		w := sqlwrapper.New(db)
//		err = w.Transact(func(tx *sql.Tx) error {
//				_, err = tx.Exec(`INSERT INTO movie_test (name) VALUES ($1)`, "Test 10")
//				if err != nil { return err }
//
//				var u User
//				err = tx.QueryRow(`SELECT * FROM movie_test WHERE name = $1`, "Test 12").Scan(&u.name)
//				if err != nil { return err }
//
//				return nil
//		})
func (w *SQLWrapper) Transact(fn func(*sql.Tx) error) error {
	return w.TransactContext(context.Background(), nil, fn)
}

// TransactContext provides same functionality as Transact along with provided context and transaction context
// options.
func (w *SQLWrapper) TransactContext(ctx context.Context, txOpts *sql.TxOptions, fn func(*sql.Tx) error) (err error) {
	tx, err := w.db.BeginTx(ctx, txOpts)
	if err != nil {
		return
	}

	defer func() {
		// in case there's a panic,recover the panic and rollback the transaction first
		// and then re-panic
		if p := recover(); p != nil {
			err := tx.Rollback()
			if err != nil {
				return
			}
			panic(p)
		} else if err != nil {
			err := tx.Rollback()
			if err != nil {
				return
			}
		} else {
			err = tx.Commit()
		}
	}()
	err = fn(tx)

	// bubble up the error to Retry wrapper to determine if retry should continue
	return err
}

// TransactWithRetry wraps transaction with retry functionality
//
// Example:
// 		w := sqlwrapper.New(db)
//		err = w.TransactWithRetry(func(tx *sql.Tx) error {
//				_, err = tx.Exec(`INSERT INTO movie_test (name) VALUES ($1)`, "Test 10")
//				if err != nil { return err }
//
//				var u User
//				err = tx.QueryRow(`SELECT * FROM movie_test WHERE name = $1`, "Test 12").Scan(&u.name)
//				if err != nil { return err }
//
//				return nil
//		})
func (w *SQLWrapper) TransactWithRetry(fn func(*sql.Tx) error) error {
	return w.TransactContextWithRetry(context.Background(), nil, fn)
}

// TransactContextWithRetry provides sames functionality as TransactWithRetry along with context and transaction
// context options
func (w *SQLWrapper) TransactContextWithRetry(ctx context.Context, txOpts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	return w.TransactContextWithRetryOptions(ctx, txOpts, w.retryOpts, fn)
}

// TransactWithRetryOptions provides same functionality as TransactWithRetry but using the provided
// retry options
//
// Example:
// 		w := sqlwrapper.New(db)
//		var newRetryOpts = sqlwrapper.RetryOptions{
//					MaxRetries:   5,
//					InitialDelay: time.Duration(10) * time.Second,
//					Factor:       3,
//					MaxDelay:     time.Duration(120) * time.Second,
//					Jitter:       func() bool { return false },
//		}
//
//		err = w.TransactWithRetryOptions(newRetryOpts, func(tx *sql.Tx) error {
//				_, err = tx.Exec(`INSERT INTO movie_test (name) VALUES ($1)`, "Test 10")
//				if err != nil { return err }
//
//				var u User
//				err = tx.QueryRow(`SELECT * FROM movie_test WHERE name = $1`, "Test 12").Scan(&u.name)
//				if err != nil { return err }
//
//				return nil
//		})
func (w *SQLWrapper) TransactWithRetryOptions(retryOpts RetryOptions, fn func(*sql.Tx) error) error {
	return w.TransactContextWithRetryOptions(context.Background(), nil, retryOpts, fn)
}

// TransactContextWithRetryOptions provides same functionality as TransactWithRetryOptions along with context and
// transaction context options
func (w *SQLWrapper) TransactContextWithRetryOptions(ctx context.Context, txOpts *sql.TxOptions, retryOpts RetryOptions, fn func(*sql.Tx) error) error {
	err := Retry(retryOpts, func() error {
		e := w.TransactContext(ctx, txOpts, fn)
		return e
	})
	return err
}

// UnWrap returns the built-in *sql.DB that passed in during construction
func (w *SQLWrapper) UnWrap() *sql.DB {
	return w.db
}
