(in-package #:arrowframe)

(defun %compare-row (t i j orders)
  (dolist (o orders)
    (let* ((col (order-spec-col o))
           (dir (order-spec-direction o))
           (a (val t col i))
           (b (val t col j)))
      (cond
        ((eql a b) nil)
        (t (return
             (if (eql dir :asc)
                 (if (stringp a) (string< a b) (< a b))
                 (if (stringp a) (string> a b) (> a b)))))))
  nil)

(defun sort-table (t orders)
  "Return a new table sorted by ORDERS (list of order-spec)." 
  (let* ((n (nrows t))
         (idx (%iota n)))
    (stable-sort idx (lambda (i j) (%compare-row t i j orders)))
    (let ((h (make-hash-table :test 'eq)))
      (maphash (lambda (k v)
                 (setf (gethash k h) (%take-indices v idx)))
               (columns t))
      (%make-table :columns h :schema (copy-list (schema t)) :nrows n))))
