(in-package #:arrowframe.tests)

(defsuite suite)
(in-suite suite)

(test basic-ops
  (let* ((t (af:make-table '((:a . #(1 2 3))
                             (:b . #(10 20 30)))
                           :schema '((:a . :i64) (:b . :i64))))
         (t2 (af:select t :b))
         (t3 (af:filter t (af:where (> :a 1))))
         (t4 (af:with t (:c (+ :a :b)))))
    (is (= 3 (af:nrows t)))
    (is (equal '(:B) (af:colnames t2)))
    (is (= 2 (af:nrows t3)))
    (is (= 33 (aref (af:col t4 :c) 2)))))

(test groupby-summarise
  (let* ((t (af:make-table '((:g . #("x" "x" "y"))
                             (:v . #(1 2 10)))
                           :schema '((:g . :string) (:v . :i64))))
         (out (af:summarise (af:group-by t :g)
                (:n (af:count*))
                (:s (af:sum :v))
                (:m (af:mean :v)))))
    (is (= 2 (af:nrows out)))
    ;; order is first-seen
    (is (string= "x" (aref (af:col out :g) 0)))
    (is (= 3 (aref (af:col out :s) 0)))
    (is (= 10 (aref (af:col out :s) 1)))))

(defun run ()
  (run! 'suite))
