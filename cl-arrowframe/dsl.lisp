(in-package #:arrowframe)

(defun %walk-expr (form table-sym i-sym)
  "Walk FORM and replace unquoted keywords with (val table :kw i)." 
  (cond
    ((keywordp form)
     `(val ,table-sym ,form ,i-sym))
    ((atom form) form)
    ((and (consp form) (eq (car form) 'quote))
     form)
    ((and (consp form) (eq (car form) 'function))
     form)
    (t (cons (%walk-expr (car form) table-sym i-sym)
             (mapcar (lambda (x) (%walk-expr x table-sym i-sym)) (cdr form))))))

(defmacro expr (form &key (table 'table) (i 'i))
  "Compile FORM into code that reads columns from TABLE at row I.

Keywords in FORM are treated as column references.
To use a keyword literal, quote it: ':symbol.
"
  (%walk-expr form table i))

(defmacro where (form)
  "Create a predicate function of (table i) from FORM." 
  `(lambda (table i)
     (declare (ignorable table i))
     (expr ,form :table table :i i)))

(defmacro -> (table &rest steps)
  "Thread TABLE through STEPS.

Each step is a form like (select :a :b) or (filter (where (> :x 0))).
The symbol '_' inside a step is replaced with the current value.
"
  (let ((g (gensym "T")))
    `(let ((,g ,table))
       ,@(mapcar (lambda (s)
                   (labels ((subst-underscore (x)
                              (cond
                                ((eq x '_) g)
                                ((consp x) (cons (subst-underscore (car x))
                                                 (mapcar #'subst-underscore (cdr x))))
                                (t x))))
                     `(setf ,g ,(subst-underscore s))))
                 steps)
       ,g)))

(defun compile-expr-fn (form)
  "Compile an expression FORM into a function (lambda (table i) ...).

This is used when the expression is only known at runtime (e.g. inside aggregation specs).
" 
  (let ((table (gensym "TABLE"))
        (i (gensym "I")))
    (compile nil `(lambda (,table ,i)
                    ,(%walk-expr form table i)))))
