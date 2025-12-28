(defpackage #:arrowframe
  (:nicknames #:af)
  (:use #:cl)
  (:export
   ;; core
   #:table
   #:make-table
   #:copy-table
   #:schema
   #:columns
   #:nrows
   #:colnames
   #:has-col-p
   #:col
   #:val
   #:slice

   ;; IO front-ends (backends live in separate systems)
   #:read-parquet
   #:write-parquet
   #:read-csv
   #:write-csv
   #:*default-io-backend*

   ;; pipeline
   #:->

   ;; expression DSL
   #:expr
   #:where

   ;; table ops
   #:select
   #:with
   #:filter
   #:head
   #:arrange
   #:asc
   #:desc

   ;; groupby/summarise
   #:key-spec
   #:key
   #:tumble
   #:group-by
   #:summarise

   ;; aggregate forms used in summarise
   #:count*
   #:sum
   #:mean
   #:min
   #:max
   #:first
   #:last

   ;; test helper
   #:run-tests))
