(defpackage #:arrowframe.tests
  (:use #:cl #:fiveam #:arrowframe)
  (:export #:run))

(in-package #:arrowframe.tests)

(defsuite suite)
(in-suite suite)

(test basic-ops
  (let* ((tbl (af:make-table '((:a . #(1 2 3))
                             (:b . #(10 20 30)))
                           :schema '((:a . :i64) (:b . :i64))))
         (t2 (af:select tbl :b))
         (t3 (af:filter tbl (af:where (> :a 1))))
         (t4 (af:with tbl (:c (+ :a :b)))))
    (is (= 3 (af:nrows tbl)))
    (is (equal '(:B) (af:colnames t2)))
    (is (= 2 (af:nrows t3)))
    (is (= 33 (aref (af:col t4 :c) 2)))))

(test pipeline-and-dsl
  (let* ((tbl (af:make-table '((:x . #(1 2 3 4))
                             (:y . #(10 20 30 40)))
                           :schema '((:x . :i64) (:y . :i64))))
         (out (af:->
               tbl
               (af:filter (af:where (and (>= :x 2) (< :x 4))))
               (af:with (:z (* :x :y)))
               (af:select :x :z))))
    (is (= 2 (af:nrows out)))
    (is (equal '(:X :Z) (af:colnames out)))
    (is (= 2 (aref (af:col out :x) 0)))
    (is (= 60 (aref (af:col out :z) 0)))
    (is (= 3 (aref (af:col out :x) 1)))
    (is (= 90 (aref (af:col out :z) 1)))))

(test head-and-slice
  (let* ((tbl (af:make-table '((:a . #(1 2 3 4 5))
                             (:b . #(10 20 30 40 50)))
                           :schema '((:a . :i64) (:b . :i64))))
         (h (af:head tbl 3))
         (s (af:slice tbl 1 4)))
    (is (= 3 (af:nrows h)))
    (is (= 1 (aref (af:col h :a) 0)))
    (is (= 3 (aref (af:col h :a) 2)))
    (is (= 3 (af:nrows s)))
    (is (= 2 (aref (af:col s :a) 0)))
    (is (= 4 (aref (af:col s :a) 2)))))

(test arrange-desc-stability
  ;; Sort by :k descending; ensure stable order for ties.
  (let* ((tbl (af:make-table '((:id . #("a" "b" "c" "d"))
                             (:k  . #(2 3 3 1)))
                           :schema '((:id . :string) (:k . :i64))))
         (out (af:arrange tbl (af:desc :k))))
    (is (equal '(:ID :K) (af:colnames out)))
    ;; keys: 3,3,2,1 and tie (b,c) keeps original order
    (is (string= "b" (aref (af:col out :id) 0)))
    (is (string= "c" (aref (af:col out :id) 1)))
    (is (string= "a" (aref (af:col out :id) 2)))
    (is (string= "d" (aref (af:col out :id) 3)))))

(test groupby-tumble
  ;; timestamps are integer nanoseconds since epoch (as per TUMBLE docstring)
  (let* ((ns 1000000000)       ; 1 second
         (tbl  (af:make-table `((:ts . #(0 ,(* 30 ns) ,(* 61 ns)))
                              (:v  . #(1 2 3)))
                            :schema '((:ts . :i64) (:v . :i64))))
         (g  (af:group-by tbl (af:tumble :ts "1m" :name :bucket)))
         (out (af:summarise g
                (:n (af:count*))
                (:s (af:sum :v)))))
    (is (equal '(:BUCKET :N :S) (af:colnames out)))
    (is (= 2 (af:nrows out)))
    ;; bucket 0 contains first two rows (v=1+2)
    (is (= 0 (aref (af:col out :bucket) 0)))
    (is (= 2 (aref (af:col out :n) 0)))
    (is (= 3 (aref (af:col out :s) 0)))
    ;; bucket 60s contains last row (v=3)
    (is (= (* 60 ns) (aref (af:col out :bucket) 1)))
    (is (= 1 (aref (af:col out :n) 1)))
    (is (= 3 (aref (af:col out :s) 1)))))

(test groupby-summarise
  (let* ((tbl (af:make-table '((:g . #("x" "x" "y"))
                             (:v . #(1 2 10)))
                           :schema '((:g . :string) (:v . :i64))))
         (out (af:summarise (af:group-by tbl :g)
                (:n (af:count*))
                (:s (af:sum :v))
                (:m (af:mean :v))
                (:mn (af:min :v))
                (:mx (af:max :v))
                (:fst (af:first :v))
                (:lst (af:last :v)))))
    (is (= 2 (af:nrows out)))
    ;; order is first-seen
    (is (string= "x" (aref (af:col out :g) 0)))
    (is (= 2 (aref (af:col out :n) 0)))
    (is (= 3 (aref (af:col out :s) 0)))
    (is (= 1 (aref (af:col out :mn) 0)))
    (is (= 2 (aref (af:col out :mx) 0)))
    (is (= 1 (aref (af:col out :fst) 0)))
    (is (= 2 (aref (af:col out :lst) 0)))
    (is (string= "y" (aref (af:col out :g) 1)))
    (is (= 1 (aref (af:col out :n) 1)))
    (is (= 10 (aref (af:col out :s) 1)))))

(defun run ()
  (run! 'suite))
