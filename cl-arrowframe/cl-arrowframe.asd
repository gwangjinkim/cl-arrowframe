(asdf:defsystem #:cl-arrowframe
  :description "Columnar DataFrame core for CL Data Lab (MVP)"
  :author "CL Data Lab"
  :license "MIT"
  :serial t
  :depends-on ()
  :components
  ((:file "package")
   (:file "util")
   (:file "core")
   (:file "dsl")
   (:file "ops")
   (:file "groupby")
   (:file "sort")))

(asdf:defsystem #:cl-arrowframe/tests
  :description "Unit tests for cl-arrowframe"
  :author "CL Data Lab"
  :license "MIT"
  :serial t
  :depends-on (#:cl-arrowframe #:fiveam)
  :components ((:file "tests")))
