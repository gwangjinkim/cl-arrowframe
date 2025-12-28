# cl-arrowframe

A minimal, columnar "data frame" core for Common Lisp with a Lisp-y pipeline DSL.

This repo contains:

- `cl-arrowframe/` – core in-memory columnar table + select/filter/mutate/group-by/summarise.
- `cl-arrowframe-io-duckdb/` – IO backend using DuckDB FFI to read/write Parquet and CSV.

> Status: MVP. The API is intentionally small and designed to evolve toward true Apache Arrow interop.

## Quickstart

```lisp
(ql:quickload :cl-arrowframe)

(defparameter t
  (af:make-table
   '((:ts     . #(1 2 3 4))
     (:price  . #(10d0 12d0 11d0 13d0))
     (:size   . #(1d0  2d0  1d0  3d0))
     (:symbol . #("BTC" "BTC" "ETH" "BTC")))))

(af:-> t
  (af:filter (and (= :symbol "BTC") (> :size 1)))
  (af:with (:dollar (* :price :size)))
  (af:select :ts :symbol :price :size :dollar)
  (af:head 10))
```

## Parquet / CSV IO via DuckDB

Install DuckDB shared library (not just the CLI). Then:

```lisp
(ql:quickload :cl-arrowframe-io-duckdb)

(defparameter df (af:read-parquet "data.parquet" :backend :duckdb))
(af:write-parquet df "out.parquet" :backend :duckdb)

(defparameter df2 (af:read-csv "data.csv" :backend :duckdb))
(af:write-csv df2 "out.csv" :backend :duckdb)
```

If the shared library isn't found automatically:

```lisp
(setf af.duckdb:*duckdb-library* "/path/to/libduckdb.so") ; or .dylib on macOS
```

## Development

- Tests use FiveAM.
- Systems:
  - `:cl-arrowframe`
  - `:cl-arrowframe/tests`
  - `:cl-arrowframe-io-duckdb`
  - `:cl-arrowframe-io-duckdb/tests` (planned)

