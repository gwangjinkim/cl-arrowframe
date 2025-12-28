(in-package #:arrowframe)

(defun %kw (x)
  "Normalize column names to keywords." 
  (cond
    ((keywordp x) x)
    ((symbolp x) (intern (string-upcase (symbol-name x)) :keyword))
    ((stringp x) (intern (string-upcase x) :keyword))
    (t (error "Cannot use ~S as a column name" x))))

(defun %ensure (condition fmt &rest args)
  (unless condition
    (error (apply #'format nil fmt args))))

(defun %vectorp (x)
  (or (vectorp x) (typep x 'simple-vector)))

(defun %len (vec)
  (length vec))

(defun %hash-keys (h)
  (let (out)
    (maphash (lambda (k v)
               (declare (ignore v))
               (push k out))
             h)
    (nreverse out)))

(defun %copy-vector (v)
  (let* ((n (length v))
         (out (make-array n)))
    (dotimes (i n out)
      (setf (aref out i) (aref v i)))))

(defun %take-indices (v indices)
  (let* ((n (length indices))
         (out (make-array n)))
    (dotimes (j n out)
      (setf (aref out j) (aref v (aref indices j))))))

(defun %iota (n)
  (let ((v (make-array n)))
    (dotimes (i n v)
      (setf (aref v i) i))))

(defun %binary-search-rightmost<= (vec x)
  "Return index of rightmost element <= x in a sorted vector, or NIL." 
  (let ((lo 0)
        (hi (1- (length vec)))
        (best nil))
    (loop while (<= lo hi) do
      (let* ((mid (floor (+ lo hi) 2))
             (v (aref vec mid)))
        (cond
          ((<= v x) (setf best mid)
                   (setf lo (1+ mid)))
          (t (setf hi (1- mid))))))
    best))
