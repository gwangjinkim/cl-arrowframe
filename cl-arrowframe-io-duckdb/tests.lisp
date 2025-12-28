(defpackage #:arrowframe.io.duckdb.tests
  (:use #:cl #:fiveam #:arrowframe #:arrowframe.io.duckdb)
  (:export #:run))

(in-package #:arrowframe.io.duckdb.tests)

(defsuite suite)
(in-suite suite)

(defun %tmp-path (name)
  (merge-pathnames name (uiop:temporary-directory)))

(test duckdb-loads
  (if (duckdb-available-p)
      (is-true t)
      (skip "DuckDB shared library not available on this system; skipping IO backend tests.")))

(test csv-roundtrip
  (unless (duckdb-available-p)
    (skip "DuckDB not available"))
  (let* ((t (af:make-table '((:ts . #(1 2 3))
                             (:sym . #("BTCUSD" "BTCUSD" "ETHUSD"))
                             (:price . #(100.0d0 101.5d0 200.25d0))
                             (:qty . #(1 2 3)))
                           :schema '((:ts . :i64) (:sym . :string) (:price . :f64) (:qty . :i64))))
         (path (%tmp-path "af_test.csv")))
    (ignore-errors (delete-file path))
    (af:write-csv t path :backend :duckdb)
    (is (probe-file path))
    (let ((r (af:read-csv path :backend :duckdb :header t)))
      (is (= (af:nrows t) (af:nrows r)))
      (is (string= "BTCUSD" (aref (af:col r :sym) 0)))
      (is (= 3 (aref (af:col r :qty) 2))))))

(test parquet-roundtrip
  (unless (duckdb-available-p)
    (skip "DuckDB not available"))
  (let* ((t (af:make-table '((:a . #(1 2 3 4))
                             (:b . #("x" "y" "y" "z")))
                           :schema '((:a . :i64) (:b . :string))))
         (path (%tmp-path "af_test.parquet")))
    (ignore-errors (delete-file path))
    (af:write-parquet t path :backend :duckdb)
    (is (probe-file path))
    (let ((r (af:read-parquet path :backend :duckdb)))
      (is (= 4 (af:nrows r)))
      (is (= 2 (aref (af:col r :a) 1)))
      (is (string= "z" (aref (af:col r :b) 3))))))

(defun run ()
  (run! 'suite))
