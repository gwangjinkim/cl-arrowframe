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

(defun copy-table (t &key (copy-columns nil))
  "Copy a table. If COPY-COLUMNS is true, vectors are copied too." 
  (let ((h (make-hash-table :test 'eq)))
    (maphash (lambda (k v)
               (setf (gethash k h)
                     (if copy-columns (%copy-vector v) v)))
             (table-columns t))
    (%make-table :columns h :schema (copy-list (table-schema t)) :nrows (table-nrows t))))

(defun schema (t) (table-schema t))
(defun columns (t) (table-columns t))
(defun nrows (t) (table-nrows t))

(defun colnames (t)
  (sort (%hash-keys (table-columns t)) #'string< :key #'symbol-name))

(defun has-col-p (t name)
  (not (null (gethash (%kw name) (table-columns t)))))

(defun col (t name)
  (or (gethash (%kw name) (table-columns t))
      (error "Unknown column ~S. Available: ~S" (%kw name) (colnames t))))

(defun val (t name i)
  (aref (col t name) i))

(defun slice (t start end)
  "Take rows [start, end)" 
  (%ensure (and (<= 0 start) (<= start end) (<= end (nrows t)))
           "Invalid slice ~D..~D for nrows=~D" start end (nrows t))
  (let* ((len (- end start))
         (idx (make-array len)))
    (dotimes (j len)
      (setf (aref idx j) (+ start j)))
    (let ((h (make-hash-table :test 'eq)))
      (maphash (lambda (k v)
                 (setf (gethash k h) (%take-indices v idx)))
               (columns t))
      (%make-table :columns h :schema (copy-list (schema t)) :nrows len))))

(defun head (t &optional (n 10))
  (slice t 0 (min (nrows t) n)))

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

(defgeneric %write-parquet (backend t path &key &allow-other-keys)
  (:documentation "Backend hook for writing an ARROWFRAME:TABLE to a parquet file."))

(defgeneric %read-csv (backend path &key &allow-other-keys)
  (:documentation "Backend hook for reading a CSV file into an ARROWFRAME:TABLE."))

(defgeneric %write-csv (backend t path &key &allow-other-keys)
  (:documentation "Backend hook for writing an ARROWFRAME:TABLE to a CSV file."))

(defmethod %read-parquet ((backend t) path &key &allow-other-keys)
  (declare (ignore path))
  (error "No parquet reader for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) and then (af:read-parquet ...)." backend))

(defmethod %write-parquet ((backend t) t path &key &allow-other-keys)
  (declare (ignore t path))
  (error "No parquet writer for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) and then (af:write-parquet ...)." backend))

(defmethod %read-csv ((backend t) path &key &allow-other-keys)
  (declare (ignore path))
  (error "No CSV reader for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) or implement a backend." backend))

(defmethod %write-csv ((backend t) t path &key &allow-other-keys)
  (declare (ignore t path))
  (error "No CSV writer for backend ~S is available.\n~
Try: (ql:quickload :cl-arrowframe-io-duckdb) or implement a backend." backend))

(defun read-parquet (path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Read a Parquet file at PATH into an ARROWFRAME:TABLE.

This function delegates to a backend implementation.
" 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%read-parquet backend path k)))

(defun write-parquet (t path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Write ARROWFRAME table T to a Parquet file at PATH via BACKEND." 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%write-parquet backend t path k)))

(defun read-csv (path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Read a CSV file at PATH into an ARROWFRAME:TABLE via BACKEND." 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%read-csv backend path k)))

(defun write-csv (t path &rest keys &key (backend *default-io-backend*) &allow-other-keys)
  "Write ARROWFRAME table T to CSV at PATH via BACKEND." 
  (let ((k (copy-list keys)))
    (remf k :backend)
    (apply #'%write-csv backend t path k)))

;; Quick self-test entrypoint
(defun run-tests ()
  (fiveam:run! 'arrowframe.tests:suite))
