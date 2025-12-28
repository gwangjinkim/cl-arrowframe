(in-package #:arrowframe.io.duckdb)

;;;; DuckDB-backed implementations of Arrowframe IO hooks.
;;;;
;;;; Public entrypoints are the generic functions defined in arrowframe/core.lisp:
;;;;   af:read-parquet / af:write-parquet / af:read-csv / af:write-csv
;;;;
;;;; Usage:
;;;;   (ql:quickload :cl-arrowframe-io-duckdb)
;;;;   (af:read-parquet "data.parquet" :backend :duckdb)
;;;;

(defun %kw-col (s)
  (etypecase s
    (keyword s)
    (symbol (intern (string-upcase (symbol-name s)) :keyword))
    (string (intern (string-upcase s) :keyword))))

(defun %duckdb-type->typekey (tcode)
  "Map DuckDB type code (duckdb_type enum integer) to our light schema keywords.

We keep this intentionally small for MVP; everything else becomes :unknown.
" 
  (case tcode
    ;; duckdb_type from duckdb.h (stable):
    ;; 0 INVALID, 1 BOOLEAN, 2 TINYINT, 3 SMALLINT, 4 INTEGER, 5 BIGINT,
    ;; 6 UTINYINT, 7 USMALLINT, 8 UINTEGER, 9 UBIGINT,
    ;; 10 FLOAT, 11 DOUBLE,
    ;; 12 TIMESTAMP, 13 DATE, 14 TIME, 15 INTERVAL,
    ;; 16 HUGEINT, 17 VARCHAR, 18 BLOB, 19 DECIMAL, 20 TIMESTAMP_S,
    ;; 21 TIMESTAMP_MS, 22 TIMESTAMP_NS, 23 ENUM, 24 LIST, 25 STRUCT, 26 MAP,
    ;; 27 UUID, 28 UNION, 29 BIT, 30 TIME_TZ, 31 TIMESTAMP_TZ
    ((5 9 4 8 3 7 2 6) :int)
    ((11 10) :double)
    ((17 18) :string)
    ((12 20 21 22 23 31) :timestamp)
    ((13) :date)
    (t :unknown)))

(defun %parse-number-safe (s)
  (handler-case
      (let ((*read-eval* nil))
        (read-from-string s))
    (error () s)))

(defun %result->table (res)
  (let* ((ncols (duckdb_column_count res))
         (nrows (duckdb_row_count res))
         (cols '())
         (schema '()))
    (dotimes (c (the fixnum ncols))
      (let* ((name (duckdb_column_name res c))
             (k (%kw-col name))
             (tcode (duckdb_column_type res c))
             (tkey (%duckdb-type->typekey tcode))
             (vec (make-array nrows)))
        (dotimes (r (the fixnum nrows))
          (if (duckdb_value_is_null res c r)
              (setf (aref vec r) nil)
            (case tkey
              (:int (setf (aref vec r) (duckdb_value_int64 res c r)))
              (:double (setf (aref vec r) (duckdb_value_double res c r)))
              (t
               ;; fallback to varchar and parse if it looks numeric
               (let ((p (duckdb_value_varchar res c r)))
                 (unwind-protect
                      (let ((s (foreign-string-to-lisp p)))
                        (setf (aref vec r)
                              (if (or (null s) (string= s ""))
                                  s
                                  (%parse-number-safe s))))
                   (when (and p (not (null-pointer-p p)))
                     (duckdb_free p))))))))
        (push (cons k vec) cols)
        (push (cons k tkey) schema)))
    (arrowframe:make-table (nreverse cols) :schema (nreverse schema))))

(defun %quote-sql-ident (name)
  "Quote an identifier for DuckDB SQL." 
  (format nil "\"~a\"" (substitute #\" #\" name)))

(defun %infer-duckdb-coltype (vec)
  "Very small heuristic type inference for write-parquet.

Returns a DuckDB SQL type string.
" 
  (let ((seen-int t)
        (seen-num t))
    (dotimes (i (length vec))
      (let ((v (aref vec i)))
        (when (and v (not (numberp v)))
          (setf seen-int nil seen-num nil)
          (return))
        (when (and (numberp v) (not (integerp v)))
          (setf seen-int nil))))
    (cond
      (seen-int "BIGINT")
      (seen-num "DOUBLE")
      (t "VARCHAR"))))

(defun %ensure-dir (path)
  (let ((p (pathname path)))
    (when (pathname-directory p)
      (ensure-directories-exist p))))

(defun %exec-sql (conn sql &optional (label "sql"))
  "Execute SQL and discard the result." 
  (with-foreign-object (res '(:struct duckdb_result))
    (%check (duckdb_query conn sql res) "duckdb_query failed (~a)" label)
    (duckdb_destroy_result res)))

(defun %create-and-fill (conn t table-name &key (schema-name "main"))
  "Create a DuckDB table and fill it using the appender API." 
  (let* ((names (arrowframe:colnames t))
         (create-cols
           (mapcar (lambda (k)
                     (let ((vec (arrowframe:col t k)))
                       (format nil "~a ~a" (%quote-sql-ident (string-downcase (symbol-name k)))
                               (%infer-duckdb-coltype vec))))
                   names))
         (create-sql (format nil "CREATE TABLE ~a (~{~a~^, ~});"
                             (%quote-sql-ident table-name) create-cols)))
    (%exec-sql conn create-sql "create")

    ;; Create appender
    (with-foreign-object (ap :pointer)
      (%check (duckdb_appender_create conn schema-name table-name ap)
              "duckdb_appender_create failed")
      (let ((app (mem-ref ap :pointer))
            (n (arrowframe:nrows t)))
        (unwind-protect
             (dotimes (i n)
               (%check (duckdb_appender_begin_row app) "begin_row failed")
               (dolist (k names)
                 (let ((v (aref (arrowframe:col t k) i)))
                   (cond
                     ((null v) (%check (duckdb_appender_append_null app) "append_null failed"))
                     ((integerp v) (%check (duckdb_appender_append_int64 app v) "append_int64 failed"))
                     ((numberp v) (%check (duckdb_appender_append_double app (coerce v 'double-float)) "append_double failed"))
                     (t (%check (duckdb_appender_append_varchar app (princ-to-string v)) "append_varchar failed"))))
               (%check (duckdb_appender_end_row app) "end_row failed"))
          (ignore-errors (duckdb_appender_close app))
          (duckdb_appender_destroy ap))))))

(defmethod arrowframe:%read-parquet ((backend (eql :duckdb)) path &key &allow-other-keys)
  (with-duckdb (db conn)
    (declare (ignore db))
    (let ((sql (format nil "SELECT * FROM read_parquet('~a');" (namestring (pathname path)))))
      (with-foreign-object (res '(:struct duckdb_result))
        (%check (duckdb_query conn sql res) "duckdb_query failed")
        (unwind-protect
             (%result->table res)
          (duckdb_destroy_result res))))))

(defmethod arrowframe:%read-csv ((backend (eql :duckdb)) path &key (header t) (delim ",") &allow-other-keys)
  (with-duckdb (db conn)
    (declare (ignore db))
    (let ((sql (format nil "SELECT * FROM read_csv_auto('~a', HEADER=~a, DELIM='~a');"
                       (namestring (pathname path))
                       (if header "TRUE" "FALSE")
                       delim)))
      (with-foreign-object (res '(:struct duckdb_result))
        (%check (duckdb_query conn sql res) "duckdb_query failed")
        (unwind-protect
             (%result->table res)
          (duckdb_destroy_result res))))))

(defmethod arrowframe:%write-parquet ((backend (eql :duckdb)) t path &key (table-name "af_table") &allow-other-keys)
  (%ensure-dir path)
  (with-duckdb (db conn)
    (declare (ignore db))
    ;; create + append
    (%create-and-fill conn t table-name)
    ;; export
    (let ((sql (format nil "COPY ~a TO '~a' (FORMAT 'parquet');"
                       (%quote-sql-ident table-name)
                       (namestring (pathname path)))))
      (%exec-sql conn sql "copy parquet")
      path)))

(defmethod arrowframe:%write-csv ((backend (eql :duckdb)) t path &key (table-name "af_table") (header t) (delim ",") &allow-other-keys)
  (%ensure-dir path)
  (with-duckdb (db conn)
    (declare (ignore db))
    (%create-and-fill conn t table-name)
    (let ((sql (format nil "COPY ~a TO '~a' (HEADER ~a, DELIM '~a');"
                       (%quote-sql-ident table-name)
                       (namestring (pathname path))
                       (if header "TRUE" "FALSE")
                       delim)))
      (%exec-sql conn sql "copy csv")
      path)))
