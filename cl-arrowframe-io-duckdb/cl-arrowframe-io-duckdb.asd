(asdf:defsystem #:cl-arrowframe-io-duckdb
  :description "DuckDB-backed Parquet/CSV IO backend for cl-arrowframe"
  :author "CL Data Lab"
  :license "MIT"
  :serial t
  :depends-on (#:cl-arrowframe #:cffi)
  :components
  ((:file "package")
   (:file "duckdb-ffi")
   (:file "duckdb-io")))
