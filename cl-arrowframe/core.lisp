(in-package #:arrowframe)

(defstruct (table (:constructor %make-table))
  "A small columnar table.

COLUMNS is a hash-table mapping keyword column names to vectors.
SCHEMA is an alist of (colname . type-keyword) pairs, purely informational in this MVP.
"
  (columns (make-hash-table :test 'eq) :type hash-table)
  (schema  nil :type list)
  (nrows   0 :type fixnum))

(defun make-table (cols &key schema)
  "Create a TABLE from COLS, an alist mapping column names to vectors.

Column names are normalized to keywords.
All vectors must have equal length.
"
  (%ensure (listp cols) "COLS must be an alist, got ~S" cols)
  (let ((h (make-hash-table :test 'eq))
        (nrows nil))
    (dolist (pair cols)
      (destructuring-bind (name . vec) pair
        (%ensure (%vectorp vec) "Column ~S must be a vector, got ~S" name vec)
        (let ((k (%kw name)))
          (when (gethash k h)
            (error "Duplicate column ~S" k))
          (let ((len (%len vec)))
            (if nrows
                (%ensure (= nrows len)
                         "Column length mismatch: expected ~D, got ~D for ~S"
                         nrows len k)
                (setf nrows len)))
          (setf (gethash k h) vec))))
    (%make-table :columns h :schema (mapcar (lambda (p)
                                             (cons (%kw (car p)) (cdr p)))
                                           (or schema '()))
                 :nrows (or nrows 0))))

(defun copy-table (tbl &key (copy-columns nil))
  "Copy a table. If COPY-COLUMNS is true, vectors are copied too." 
  (let ((h (make-hash-table :test 'eq)))
    (maphash (lambda (k v)
               (setf (gethash k h)
                     (if copy-columns (%copy-vector v) v)))
             (table-columns tbl))
    (%make-table :columns h :schema (copy-list (table-schema tbl)) :nrows (table-nrows tbl))))

(defun schema (tbl) (table-schema tbl))
(defun columns (tbl) (table-columns tbl))
(defun nrows (tbl) (table-nrows tbl))

(defun colnames (tbl)
  (sort (%hash-keys (table-columns tbl)) #'string< :key #'symbol-name))

(defun has-col-p (tbl name)
  (not (null (gethash (%kw name) (table-columns tbl)))))

(defun col (tbl name)
  (or (gethash (%kw name) (table-columns tbl))
      (error "Unknown column ~S. Available: ~S" (%kw name) (colnames tbl))))

(defun val (tbl name i)
  (aref (col tbl name) i))

(defun slice (tbl start end)
  "Take rows [start, end)" 
  (%ensure (and (<= 0 start) (<= start end) (<= end (nrows tbl)))
           "Invalid slice ~D..~D for nrows=~D" start end (nrows tbl))
  (let* ((len (- end start))
         (idx (make-array len)))
    (dotimes (j len)
      (setf (aref idx j) (+ start j)))
    (let ((h (make-hash-table :test 'eq)))
      (maphash (lambda (k v)
                 (setf (gethash k h) (%take-indices v idx)))
               (columns tbl))
      (%make-table :columns h :schema (copy-list (schema tbl)) :nrows len))))

(defun head (tbl &optional (n 10))
  (slice tbl 0 (min (nrows tbl) n)))

;;; ------------------------------------------------------------
;;; IO API (front-end)
;;;
;;; The core system only defines the user-facing functions and generic
;;; back-end hooks. Concrete implementations live in separate ASDF
;;; systems (e.g., cl-arrowframe-io-duckdb).

(defparameter *default-io-backend* :duckdb
  "Default IO backend keyword used by READ-PARQUET/WRITE-PARQUET.")

(defgeneric %read-parquet (backend path &key &allow-other-keys)
  (:documentation "Backend hook for reading a parquet file into an ARROWFRAME:TABLE."))

(defgeneric %write-parquet (backend tbl path &key &allow-other-keys)
  (:documentation "Backend hook for writing an ARROWFRAME:TABLE to a parquet file."))

(defgeneric %read-csv (backend path &key &allow-other-keys)
  (:documentation "Backend hook for reading a CSV file into an ARROWFRAME:TABLE."))

(defgeneric %write-csv (backend tbl path &key &allow-other-keys)
  (:documentation "Backend hook for writing an ARROWFRAME:TABLE to a CSV file."))

(defmethod %read-parquet ((backend t) path &key &allow-other-keys)
  (declare (ignore path))
  (error "No parquet reader for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) and then (af:read-parquet ...)." backend))

(defmethod %write-parquet ((backend t) tbl path &key &allow-other-keys)
  (declare (ignore tbl path))
  (error "No parquet writer for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) and then (af:write-parquet ...)." backend))

(defmethod %read-csv ((backend t) path &key &allow-other-keys)
  (declare (ignore path))
  (error "No CSV reader for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) or implement a backend." backend))

(defmethod %write-csv ((backend t) tbl path &key &allow-other-keys)
  (declare (ignore tbl path))
  (error "No CSV writer for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) or implement a backend." backend))

(defun read-parquet (path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Read a Parquet file at PATH into an ARROWFRAME:TABLE.

This function delegates to a backend implementation.
" 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%read-parquet backend path k)))

(defun write-parquet (tbl path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Write ARROWFRAME table T to a Parquet file at PATH via BACKEND." 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%write-parquet backend tbl path k)))

(defun read-csv (path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Read a CSV file at PATH into an ARROWFRAME:TABLE via BACKEND." 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%read-csv backend path k)))

(defun write-csv (tbl path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Write ARROWFRAME table T to CSV at PATH via BACKEND." 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%write-csv backend tbl path k)))

;; Quick self-test entrypoint
(defun run-tests ()
  "Load and run the FiveAM test suite (system cl-arrowframe/tests)."
  (asdf:load-system :cl-arrowframe/tests)
  (let* ((pkg (or (find-package "ARROWFRAME.TESTS")
                  (error "Test package ARROWFRAME.TESTS not found after loading tests system.")))
         (sym (or (find-symbol "RUN" pkg)
                  (error "ARROWFRAME.TESTS:RUN not found."))))
    (funcall sym)))
