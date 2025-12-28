(in-package #:arrowframe.io.duckdb)

;;;; Minimal DuckDB C API bindings (enough for Parquet/CSV IO)
;;;;
;;;; You need a DuckDB shared library installed on your system.
;;;; - macOS (Homebrew):   brew install duckdb
;;;; - Ubuntu/Debian:      apt install libduckdb-dev  (package names vary)
;;;; - Or download from duckdb.org
;;;;
;;;; If the library is in a non-standard location, set:
;;;;   (setf af.duckdb:*duckdb-library* "/path/to/libduckdb.so")

(define-condition duckdb-error (error)
  ((message :initarg :message :reader duckdb-error-message))
  (:report (lambda (c s)
             (format s "DuckDB error: ~a" (duckdb-error-message c)))))

(defparameter *duckdb-library* nil
  "Path to DuckDB shared library, or NIL to try common names.")

(defparameter %duckdb :duckdb
  "Backend keyword used by cl-arrowframe IO hooks.")

(define-foreign-library duckdb
  (t (:or
      ;; user-specified path
      *duckdb-library*
      ;; common names
      "libduckdb.so"
      "libduckdb.dylib"
      "duckdb.dll")))

(defvar *duckdb-loaded-p* nil)

(defun ensure-duckdb-loaded ()
  (unless *duckdb-loaded-p*
    (handler-case
        (progn
          (use-foreign-library duckdb)
          (setf *duckdb-loaded-p* t))
      (error (e)
        (error 'duckdb-error
               :message (format nil "Failed to load DuckDB library. Set af.duckdb:*duckdb-library* to the path of your DuckDB shared library. Original error: ~a" e))))))

;;;; Basic types
(deftype duckdb_database () '(:pointer))
(deftype duckdb_connection () '(:pointer))
(deftype duckdb_appender () '(:pointer))
(deftype duckdb_state () :int)
(deftype idx_t () :uint64)

(defcstruct duckdb_result
  (internal_data :pointer))

;;;; API functions
(defcfun (duckdb_open "duckdb_open") duckdb_state
  (path :string)
  (out-db :pointer))

(defcfun (duckdb_close "duckdb_close") :void
  (db :pointer))

(defcfun (duckdb_connect "duckdb_connect") duckdb_state
  (db duckdb_database)
  (out-conn :pointer))

(defcfun (duckdb_disconnect "duckdb_disconnect") :void
  (conn :pointer))

(defcfun (duckdb_query "duckdb_query") duckdb_state
  (conn duckdb_connection)
  (query :string)
  (out-result :pointer))

(defcfun (duckdb_destroy_result "duckdb_destroy_result") :void
  (result :pointer))

(defcfun (duckdb_column_count "duckdb_column_count") idx_t
  (result :pointer))

(defcfun (duckdb_row_count "duckdb_row_count") idx_t
  (result :pointer))

(defcfun (duckdb_column_name "duckdb_column_name") :string
  (result :pointer)
  (col idx_t))

(defcfun (duckdb_column_type "duckdb_column_type") :int
  (result :pointer)
  (col idx_t))

(defcfun (duckdb_value_is_null "duckdb_value_is_null") :bool
  (result :pointer)
  (col idx_t)
  (row idx_t))

(defcfun (duckdb_value_varchar "duckdb_value_varchar") :pointer
  (result :pointer)
  (col idx_t)
  (row idx_t))

(defcfun (duckdb_value_int64 "duckdb_value_int64") :int64
  (result :pointer)
  (col idx_t)
  (row idx_t))

(defcfun (duckdb_value_double "duckdb_value_double") :double
  (result :pointer)
  (col idx_t)
  (row idx_t))

(defcfun (duckdb_free "duckdb_free") :void
  (ptr :pointer))

;;;; Appender (fast insert)
(defcfun (duckdb_appender_create "duckdb_appender_create") duckdb_state
  (conn duckdb_connection)
  (schema :string)
  (table :string)
  (out-appender :pointer))

(defcfun (duckdb_appender_begin_row "duckdb_appender_begin_row") duckdb_state
  (app duckdb_appender))

(defcfun (duckdb_appender_end_row "duckdb_appender_end_row") duckdb_state
  (app duckdb_appender))

(defcfun (duckdb_appender_append_null "duckdb_appender_append_null") duckdb_state
  (app duckdb_appender))

(defcfun (duckdb_appender_append_int64 "duckdb_appender_append_int64") duckdb_state
  (app duckdb_appender)
  (val :int64))

(defcfun (duckdb_appender_append_double "duckdb_appender_append_double") duckdb_state
  (app duckdb_appender)
  (val :double))

(defcfun (duckdb_appender_append_varchar "duckdb_appender_append_varchar") duckdb_state
  (app duckdb_appender)
  (val :string))

(defcfun (duckdb_appender_close "duckdb_appender_close") duckdb_state
  (app duckdb_appender))

(defcfun (duckdb_appender_destroy "duckdb_appender_destroy") :void
  (app :pointer))

(defun %check (state fmt &rest args)
  (unless (zerop state)
    (error 'duckdb-error :message (apply #'format nil fmt args))))

(defmacro with-duckdb ((db conn &key (path nil)) &body body)
  "Open an in-memory DuckDB connection for the dynamic extent of BODY." 
  `(progn
     (ensure-duckdb-loaded)
     (with-foreign-object (dbp :pointer)
       (setf (mem-ref dbp :pointer) (null-pointer))
       (%check (duckdb_open ,path dbp) "duckdb_open failed")
       (with-foreign-object (cp :pointer)
         (%check (duckdb_connect (mem-ref dbp :pointer) cp) "duckdb_connect failed")
         (let ((,db (mem-ref dbp :pointer))
               (,conn (mem-ref cp :pointer)))
           (unwind-protect
                (progn ,@body)
             ;; NB: both functions expect pointers to their pointer variables.
             (duckdb_disconnect cp)
             (duckdb_close dbp))))))
