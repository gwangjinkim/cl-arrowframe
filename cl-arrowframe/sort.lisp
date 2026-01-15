(in-package :arrowframe)

(defun %cmp (a b)
  "Return -1/0/1 for a<b / a=b / a>b."
  (cond
    ((and (numberp a) (numberp b))
     (cond ((< a b) -1) ((> a b) 1) (t 0)))
    ((and (stringp a) (stringp b))
     (cond ((string< a b) -1) ((string< b a) 1) (t 0)))
    (t
     (let ((sa (prin1-to-string a))
           (sb (prin1-to-string b)))
       (cond ((string< sa sb) -1) ((string< sb sa) 1) (t 0))))))

(defun sort-table (tbl orders)
  "Return a new table sorted by ORDERS = list of (colkw direction).
direction is :asc or :desc."
  (let* ((n (nrows tbl))
         (idx (loop for i below n collect i)))
    (labels ((v (col i) (aref (col tbl col) i))
             (lessp (i j)
               (dolist (ord orders nil)
                 (destructuring-bind (col dir) ord
                   (let* ((ai (v col i))
                          (aj (v col j))
                          (c (%cmp ai aj)))
                     (when (/= c 0)
                       (return (if (eql dir :desc)
                                   (> c 0)   ; invert for desc
                                   (< c 0)))))))))
      (setf idx (stable-sort idx #'lessp))
      ;; permute columns
      (let ((h (make-hash-table :test 'eq)))
        (maphash
         (lambda (k vec)
           (let ((out (make-array n)))
             (loop for pos from 0 below n
                   for old = (nth pos idx)
                   do (setf (aref out pos) (aref vec old)))
             (setf (gethash k h) out)))
         (columns tbl))
        (%make-table :columns h :schema (copy-list (schema tbl)) :nrows n)))))
