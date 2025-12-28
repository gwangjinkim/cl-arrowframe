(in-package #:arrowframe)

(defun %compare-row (tbl i j orders)
  (dolist (o orders)
    (let* ((col (order-spec-col o))
           (dir (order-spec-direction o))
           (a (val tbl col i))
           (b (val tbl col j)))
      (cond
        ((eql a b) nil)
        (t (return
             (if (eql dir :asc)
                 (if (stringp a) (string< a b) (< a b))
                 (if (stringp a) (string> a b) (> a b)))))))
  nil)

(defun sort-table (tbl orders)
  "Return a new table sorted by ORDERS (list of order-spec)." 
  (let* ((n (nrows tbl))
         (idx (%iota n)))
    (stable-sort idx (lambda (i j) (%compare-row tbl i j orders)))
    (let ((h (make-hash-table :test 'eq)))
      (maphash (lambda (k v)
                 (setf (gethash k h) (%take-indices v idx)))
               (columns tbl))
      (%make-table :columns h :schema (copy-list (schema tbl)) :nrows n))))
