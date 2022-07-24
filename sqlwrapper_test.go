package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
	"time"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Test sqlwraper package")
}

var _ = Describe("sqlwrapper", func() {
	Context("Retry", func() {
		It("should retry MaxRetries times if the function cannot succeed", func() {
			cnt := 0
			err := Retry(retryOpts, func() error {
				// return error every time
				e := retryTestHelper(&cnt, false)
				return e
			})

			Expect(err.Error()).To(Equal(ErrRunningFunction.Error()))
			Expect(cnt).To(Equal(retryOpts.MaxRetries))
		})

		It("should retry and succeed if the function was succeed", func() {
			cnt := 0
			successOnAttempt := 2
			err := Retry(retryOpts, func() error {
				// return error on the first 2 calls
				e := retryTestHelper(&cnt, cnt == successOnAttempt)
				return e
			})

			Expect(err).To(BeNil())
			Expect(cnt).To(Equal(successOnAttempt))
		})
	})

	Context("Construct", func() {
		It("should use default retry options when using default construction .New()", func() {
			db, _, _ := sqlmock.New()
			w := New(db)

			Expect(w.retryOpts.MaxRetries).To(Equal(DefaultMaxRetries))
			Expect(w.retryOpts.InitialDelay).To(Equal(DefaultInitialDelay))
			Expect(w.retryOpts.MaxDelay).To(Equal(DefaultMaxDelay))
			Expect(w.retryOpts.Factor).To(Equal(DefaultFactor))
			Expect(*w.retryOpts.Jitter).To(Equal(DefaultJitter))
		})

		It("should use provided retry options when using .NewWithOptions()", func() {
			db, _, _ := sqlmock.New()
			w := NewWithOptions(db, retryOpts)

			Expect(w.retryOpts.MaxRetries).To(Equal(retryOpts.MaxRetries))
			Expect(w.retryOpts.InitialDelay).To(Equal(retryOpts.InitialDelay))
			Expect(w.retryOpts.MaxDelay).To(Equal(retryOpts.MaxDelay))
			Expect(w.retryOpts.Factor).To(Equal(retryOpts.Factor))
			Expect(*w.retryOpts.Jitter).To(Equal(*retryOpts.Jitter))
		})

		It("should use default value if provided any provided retry option is nil", func() {
			db, _, _ := sqlmock.New()
			nilRetryOpts := RetryOptions{}
			w := NewWithOptions(db, nilRetryOpts)

			Expect(w.retryOpts.MaxRetries).To(Equal(DefaultMaxRetries))
			Expect(w.retryOpts.InitialDelay).To(Equal(DefaultInitialDelay))
			Expect(w.retryOpts.MaxDelay).To(Equal(DefaultMaxDelay))
			Expect(w.retryOpts.Factor).To(Equal(DefaultFactor))
			Expect(*w.retryOpts.Jitter).To(Equal(DefaultJitter))
		})
	})

	Context("Transact", func() {
		It("should commit the transaction if no errors during the transaction", func() {
			db, mock, _ := sqlmock.New()

			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()

			w := NewWithOptions(db, retryOpts)
			err := w.Transact(func(tx *sql.Tx) error {
				_, e := tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)
				if e != nil {
					return e
				}

				return nil
			})

			Expect(err).To(BeNil())
			Expect(mock.ExpectationsWereMet()).To(BeNil())
		})

		It("should not commit the transaction if there's an error during executation", func() {
			db, mock, _ := sqlmock.New()

			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(2).WillReturnError(ErrRunningFunction)
			mock.ExpectRollback()

			w := NewWithOptions(db, retryOpts)
			err := w.Transact(func(tx *sql.Tx) error {
				// successful executation
				_, e := tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)
				if e != nil {
					return e
				}

				// failed executation
				_, e = tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 2)
				if e != nil {
					return e
				}

				return nil
			})

			Expect(err).To(Equal(ErrRunningFunction))
			Expect(mock.ExpectationsWereMet()).To(BeNil())
		})

		It("should recover the panic and rollback the transaction and finally re-panic", func() {
			defer func() {
				if p := recover(); p == nil {
					fmt.Println("recover panic for unit tests")
				}
			}()

			db, mock, _ := sqlmock.New()

			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectRollback()

			w := NewWithOptions(db, retryOpts)
			err := w.Transact(func(tx *sql.Tx) error {
				_, e := tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)
				if e != nil {
					return e
				}
				panic(ErrRunningFunction)
			})

			Expect(mock.ExpectationsWereMet()).To(BeNil())
			Expect(err).To(Equal(ErrRunningFunction))
		})

		It("should rollback if there's an error happened with tx.Begin()", func() {
			db, mock, _ := sqlmock.New()

			mock.ExpectBegin().WillReturnError(ErrRunningFunction)

			w := NewWithOptions(db, retryOpts)
			err := w.Transact(func(tx *sql.Tx) error {
				return nil
			})

			Expect(err).To(Equal(ErrRunningFunction))
			Expect(mock.ExpectationsWereMet()).To(BeNil())
		})
	})

	Context("TransactWithRetry", func() {
		It("should bubble up the error after retry MaxRetries times", func() {
			db, mock, _ := sqlmock.New()

			for i := 0; i < DefaultMaxRetries; i++ {
				mock.ExpectBegin()
				mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnError(ErrRunningFunction)
				mock.ExpectRollback()
			}

			cnt := 0
			w := NewWithOptions(db, retryOpts)
			err := w.TransactWithRetry(func(tx *sql.Tx) error {
				cnt++
				_, e := tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)
				if e != nil {
					return e
				}

				return nil
			})

			Expect(mock.ExpectationsWereMet()).To(BeNil())
			Expect(err.Error()).To(Equal(ErrRunningFunction.Error()))
			Expect(cnt).To(Equal(DefaultMaxRetries))
		})

		It("should retry and succeed", func() {
			db, mock, _ := sqlmock.New()

			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnError(ErrRunningFunction)
			mock.ExpectRollback()
			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()

			successOnAttempt := 2
			cnt := 0
			w := NewWithOptions(db, retryOpts)
			err := w.TransactWithRetry(func(tx *sql.Tx) error {
				cnt++
				_, e := tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)
				if e != nil {
					return e
				}
				return nil
			})

			Expect(mock.ExpectationsWereMet()).To(BeNil())
			Expect(cnt).To(Equal(successOnAttempt))
			Expect(err).To(BeNil())
		})

		It("should retry and succeed (with multiple statements)", func() {
			db, mock, _ := sqlmock.New()

			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(2).WillReturnError(ErrRunningFunction)
			mock.ExpectRollback()
			mock.ExpectBegin()
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(`INSERT INTO table_name`).WithArgs(2).WillReturnResult(sqlmock.NewResult(2, 1))
			mock.ExpectCommit()

			successOnAttempt := 2
			cnt := 0
			w := NewWithOptions(db, retryOpts)
			err := w.TransactWithRetry(func(tx *sql.Tx) error {
				cnt++
				_, e := tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)
				if e != nil {
					return e
				}

				_, e = tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 2)
				if e != nil {
					return e
				}

				return nil
			})

			Expect(mock.ExpectationsWereMet()).To(BeNil())
			Expect(cnt).To(Equal(successOnAttempt))
			Expect(err).To(BeNil())
		})
	})

	Context("TransactWithRetryOptions", func() {
		It("should using provided retry options", func() {
			jitter := false
			customRetryOpts := RetryOptions{
				MaxRetries:   5,
				InitialDelay: 100,
				MaxDelay:     200,
				Factor:       2,
				Jitter:       &jitter,
			}

			db, mock, _ := sqlmock.New()

			for i := 0; i < customRetryOpts.MaxRetries; i++ {
				mock.ExpectBegin()
				mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnError(ErrRunningFunction)
				mock.ExpectRollback()
			}

			cnt := 0
			w := NewWithOptions(db, retryOpts)
			err := w.TransactWithRetryOptions(customRetryOpts, func(tx *sql.Tx) error {
				cnt++
				_, e := tx.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)
				if e != nil {
					return e
				}

				return nil
			})

			Expect(err).To(Equal(ErrRunningFunction))
			Expect(cnt).To(Equal(customRetryOpts.MaxRetries))
			Expect(mock.ExpectationsWereMet()).To(BeNil())
		})
	})

	Context("Exec", func() {
		It("should run default retry for Exec", func() {
			db, mock, _ := sqlmock.New()
			w := NewWithOptions(db, retryOpts)

			for i := 0; i < DefaultMaxRetries; i++ {
				mock.ExpectExec(`INSERT INTO table_name`).WithArgs(1).WillReturnError(ErrRunningFunction)
			}

			res, err := w.Exec(`INSERT INTO table_name (name) VALUES ($1)`, 1)

			Expect(mock.ExpectationsWereMet()).To(BeNil())
			Expect(err).To(Equal(ErrRunningFunction))
			Expect(res).To(BeNil())
		})
	})

	Context("Query", func() {
		It("should run default retry for Query", func() {
			db, mock, _ := sqlmock.New()
			w := NewWithOptions(db, retryOpts)

			for i := 0; i < DefaultMaxRetries; i++ {
				mock.ExpectQuery(`SELECT (.+) FROM test_mock WHERE attr_name = ?`).
					WithArgs(1).
					WillReturnError(ErrRunningFunction)
			}

			rows, err := w.Query(`SELECT * FROM test_mock WHERE attr_name = $1`, 1)

			Expect(mock.ExpectationsWereMet()).To(BeNil())
			Expect(err).To(Equal(ErrRunningFunction))
			Expect(rows).To(BeNil())
		})
	})

	Context("QueryRow", func() {
		It("should run default retry for QueryRow", func() {
			db, mock, _ := sqlmock.New()
			w := NewWithOptions(db, retryOpts)

			for i := 0; i < DefaultMaxRetries; i++ {
				mock.ExpectQuery(`SELECT (.+) FROM test_table WHERE name = ?`).
					WithArgs(1).
					WillReturnError(ErrRunningFunction)
			}

			err := w.QueryRow(`SELECT * FROM test_table WHERE name = $1`, 1)

			Expect(mock.ExpectationsWereMet()).To(BeNil())
			Expect(err.Err()).To(Equal(ErrRunningFunction))
		})
	})

	Context("Unwrap", func() {
		It("should unwrap the wrapper and return the original *sql.DB client (same pointer)", func() {
			db, _, _ := sqlmock.New()

			w := New(db)
			actual := w.UnWrap()

			Expect(actual).To(Equal(db))
		})
	})
})

// ErrRunningFunction is a simple error message for testing purpose
var ErrRunningFunction = errors.New("error while executing the test function")

// retryOpts is a reusable RetryOptions for testing purpose with minimum delay
var retryOpts = RetryOptions{
	MaxRetries:   3,
	InitialDelay: time.Duration(60) * time.Millisecond,
	MaxDelay:     time.Duration(600) * time.Millisecond,
	Factor:       1,
	Jitter:       &DefaultJitter,
}

// retryTestHelper is used for testing retry wrapper logic
func retryTestHelper(cnt *int, cond bool) error {
	if cond {
		return nil
	}
	*cnt++
	return ErrRunningFunction
}
