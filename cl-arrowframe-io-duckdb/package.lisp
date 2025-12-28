(defpackage #:arrowframe.io.duckdb
  (:nicknames #:af.duckdb)
  (:use #:cl #:cffi #:arrowframe)
  (:export
   #:*duckdb-library*
   #:ensure-duckdb-loaded
   #:with-duckdb
   ;; backend key
   #:%duckdb
   ;; low-level errors
   #:duckdb-error))
