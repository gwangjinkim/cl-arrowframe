(in-package #:arrowframe)

(defun select (t &rest cols)
  "Return a new table with only COLS." 
  (let ((h (make-hash-table :test 'eq)))
    (dolist (c cols)
      (let ((k (%kw c)))
        (setf (gethash k h) (col t k))))
    (%make-table :columns h
                 :schema (remove-if-not (lambda (p) (gethash (car p) h)) (schema t))
                 :nrows (nrows t))))

(defun filter (t predicate)
  "Filter rows using PREDICATE (table i) -> boolean." 
  (%ensure (functionp predicate) "PREDICATE must be a function")
  (let* ((n (nrows t))
         (keep (make-array n :adjustable t :fill-pointer 0)))
    (dotimes (i n)
      (when (funcall predicate t i)
        (vector-push-extend i keep)))
    (let ((h (make-hash-table :test 'eq)))
      (maphash (lambda (k v)
                 (setf (gethash k h) (%take-indices v keep)))
               (columns t))
      (%make-table :columns h :schema (copy-list (schema t)) :nrows (length keep)))))

(defmacro with (t &rest bindings)
  "Add or replace columns.

BINDINGS are like (:new-col <expr>) where <expr> can use keyword column references.
"
  (let ((g (gensym "T"))
        (i (gensym "I")))
    `(let* ((,g (copy-table ,t))
            (n (nrows ,g)))
       ,@(mapcar
          (lambda (b)
            (destructuring-bind (name form) b
              (let ((k (%kw name)))
                `(let ((out (make-array n)))
                   (dotimes (,i n)
                     (setf (aref out ,i) (expr ,form :table ,g :i ,i)))
                   (setf (gethash ,k (columns ,g)) out)
                   ;; update schema if present
                   (unless (assoc ,k (schema ,g))
                     (setf (table-schema ,g) (append (schema ,g) (list (cons ,k :unknown)))))))))
          bindings)
       ,g)))

(defstruct order-spec
  col
  direction)

(defun asc (col) (make-order-spec :col (%kw col) :direction :asc))
(defun desc (col) (make-order-spec :col (%kw col) :direction :desc))

(defun arrange (t &rest orders)
  (sort-table t orders))
