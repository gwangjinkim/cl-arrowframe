(in-package #:arrowframe)

(defstruct key-spec
  name
  fn)

(defun key (name fn)
  "Create a computed group key." 
  (%ensure (functionp fn) "KEY fn must be function")
  (make-key-spec :name (%kw name) :fn fn))

(defun %duration->ns (rule)
  "Parse duration strings like \"15m\", \"1h\", \"20d\" into nanoseconds." 
  (%ensure (stringp rule) "Rule must be a string, got ~S" rule)
  (let* ((len (length rule))
         (unit (char rule (1- len)))
         (num (parse-integer rule :end (1- len))))
    (* num
       (ecase unit
         (#\s 1000000000)
         (#\m (* 60 1000000000))
         (#\h (* 3600 1000000000))
         (#\d (* 86400 1000000000))
         (#\w (* 7 86400 1000000000))))))

(defun tumble (col rule &key (name :bucket))
  "Group timestamps in COL into fixed bins of size RULE (e.g. \"1m\").

Timestamps are assumed to be integer nanoseconds since epoch.
" 
  (let* ((c (%kw col))
         (bin (%duration->ns rule)))
    (key name (lambda (tbl i)
                (let ((ts (val tbl c i)))
                  (* (floor ts bin) bin))))))

(defstruct grouped
  table
  keys)

(defun %normalize-key (k)
  (cond
    ((typep k 'key-spec) k)
    (t
     (let ((c (%kw k)))
       (key c (lambda (tbl i) (val tbl c i)))))))

(defun group-by (tbl &rest keys)
  "Return a GROUPED object." 
  (make-grouped :table tbl
                :keys (mapcar #'%normalize-key keys)))

;; Aggregate marker functions (they are intended to be USED AS FORMS inside SUMMARISE).
;; They are never meant to be called directly at runtime.
(defun agg-count (&rest _) (declare (ignore _)) (error "AGG-COUNT is only valid inside SUMMARISE"))
(defun agg-sum   (&rest _) (declare (ignore _)) (error "AGG-SUM is only valid inside SUMMARISE"))
(defun agg-mean  (&rest _) (declare (ignore _)) (error "AGG-MEAN is only valid inside SUMMARISE"))
(defun agg-min   (&rest _) (declare (ignore _)) (error "AGG-MIN is only valid inside SUMMARISE"))
(defun agg-max   (&rest _) (declare (ignore _)) (error "AGG-MAX is only valid inside SUMMARISE"))
(defun agg-first (&rest _) (declare (ignore _)) (error "AGG-FIRST is only valid inside SUMMARISE"))
(defun agg-last  (&rest _) (declare (ignore _)) (error "AGG-LAST is only valid inside SUMMARISE"))

;; Backwards-compatible aliases (kept for convenience; prefer AGG-* names).
(defun count* (&rest _) (declare (ignore _)) (error "COUNT* is only valid inside SUMMARISE (prefer AGG-COUNT)"))
(defun sum    (&rest _) (declare (ignore _)) (error "SUM is only valid inside SUMMARISE (prefer AGG-SUM)"))
(defun mean   (&rest _) (declare (ignore _)) (error "MEAN is only valid inside SUMMARISE (prefer AGG-MEAN)"))

(defun %agg-op->kw (sym)
  (intern (string-upcase (symbol-name sym)) :keyword))

(defun %agg-fn (agg-form)
  "Return a function (lambda (table indices-vector) value) for an aggregate form.

Recommended aggregate operators:
  (agg-count)
  (agg-sum <expr>)
  (agg-mean <expr>)
  (agg-min <expr>)
  (agg-max <expr>)
  (agg-first <expr>)
  (agg-last <expr>)

For backwards compatibility, SUMMARISE also accepts COUNT*, SUM, MEAN and even
MIN/MAX/FIRST/LAST *by name* (without defining those symbols in ARROWFRAME)."
  (let* ((op (car agg-form))
         (opk (%agg-op->kw op)))
    (case opk
      ((:AGG-COUNT :COUNT*)
       (lambda (tbl idx)
         (declare (ignore tbl))
         (length idx)))

      ((:AGG-SUM :SUM
        :AGG-MEAN :MEAN
        :AGG-MIN :MIN
        :AGG-MAX :MAX
        :AGG-FIRST :FIRST
        :AGG-LAST :LAST)
       (let ((expr-form (cadr agg-form)))
         (%ensure expr-form "Aggregate ~S requires an expression, e.g. (~S :x)" op op)
         (let ((f (compile-expr-fn expr-form)))
           (ecase opk
             ((:AGG-SUM :SUM)
              (lambda (tbl idx)
                (let ((acc 0))
                  (dotimes (k (length idx) acc)
                    (incf acc (funcall f tbl (aref idx k)))))))

             ((:AGG-MEAN :MEAN)
              (lambda (tbl idx)
                (let ((n (length idx)))
                  (if (= n 0)
                      nil
                      (let ((acc 0))
                        (dotimes (k n)
                          (incf acc (funcall f tbl (aref idx k))))
                        (/ acc n))))))

             ((:AGG-MIN :MIN)
              (lambda (tbl idx)
                (let ((n (length idx)))
                  (if (= n 0)
                      nil
                      (let ((best (funcall f tbl (aref idx 0))))
                        (loop for j from 1 below n
                              for v = (funcall f tbl (aref idx j))
                              do (when (< v best) (setf best v)))
                        best)))))

             ((:AGG-MAX :MAX)
              (lambda (tbl idx)
                (let ((n (length idx)))
                  (if (= n 0)
                      nil
                      (let ((best (funcall f tbl (aref idx 0))))
                        (loop for j from 1 below n
                              for v = (funcall f tbl (aref idx j))
                              do (when (> v best) (setf best v)))
                        best)))))

             ((:AGG-FIRST :FIRST)
              (lambda (tbl idx)
                (if (= (length idx) 0)
                    nil
                    (funcall f tbl (aref idx 0)))))

             ((:AGG-LAST :LAST)
              (lambda (tbl idx)
                (let ((n (length idx)))
                  (if (= n 0)
                      nil
                      (funcall f tbl (aref idx (1- n)))))))))))

      (t
       (error "Unknown aggregate: ~S" agg-form)))))



(defmacro summarise (grouped &rest outputs)
  "Summarise a grouped table.

Usage:
  (summarise (group-by tbl :a) (:n (agg-count)) (:s (agg-sum :x)))

If GROUPED is a plain table, it will be treated as a single group.
" 
  (let ((g (gensym "G"))
        (t* (gensym "T"))
        (keys* (gensym "KEYS"))
        (groups* (gensym "GROUPS"))
        (order* (gensym "ORDER"))
        (gi (gensym "GI"))
        (idx (gensym "IDX")))
    `(let* ((,g ,grouped)
            (,t* (if (typep ,g 'grouped) (grouped-table ,g) ,g))
            (,keys* (if (typep ,g 'grouped) (grouped-keys ,g) '())))
       (multiple-value-bind (,groups* ,order*) (%group-indices ,t* ,keys*)
         (let* ((out-n (length ,order*))
                (out-cols (make-hash-table :test 'eq)))

           ;; key columns
           (dolist (ks ,keys*)
             (let ((v (make-array out-n)))
               (setf (gethash (key-spec-name ks) out-cols) v)))

           ;; output columns
           ,@(mapcar
              (lambda (o)
                (destructuring-bind (name agg-form) o
                  (let ((k (%kw name)))
                    `(setf (gethash ,k out-cols) (make-array out-n)))))
              outputs)

           ;; fill rows
           (dotimes (,gi out-n)
             (let* ((kvals (aref ,order* ,gi))
                    (,idx (gethash kvals ,groups*)))
               ;; keys
               (dotimes (kk (length ,keys*))
                 (let* ((ks (nth kk ,keys*))
                        (kname (key-spec-name ks)))
                   (setf (aref (gethash kname out-cols) ,gi) (nth kk kvals))))

               ;; aggs
               ,@(mapcar
                  (lambda (o)
                    (destructuring-bind (name agg-form) o
                      (let ((k (%kw name)))
                        `(setf (aref (gethash ,k out-cols) ,gi)
                               (funcall (%agg-fn ',agg-form) ,t* ,idx)))))
                  outputs)))

           ;; schema (best-effort)
           (let ((out-schema '()))
             (dolist (ks ,keys*)
               (push (cons (key-spec-name ks) :unknown) out-schema))
             ,@(mapcar
                (lambda (o)
                  (let ((k (%kw (car o))))
                    `(push (cons ,k :unknown) out-schema)))
                outputs)
             (setf out-schema (nreverse out-schema))
             (%make-table :columns out-cols :schema out-schema :nrows out-n)))))))
