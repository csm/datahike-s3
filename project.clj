(defproject datahike-s3 "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [io.replikativ/datahike "0.2.0"]
                 [com.cognitect.aws/api "0.8.352"]
                 [com.cognitect.aws/endpoints "1.1.11.632"]
                 [com.cognitect.aws/s3 "726.2.488.0"]
                 [com.cognitect/anomalies "0.1.12"]
                 [ch.qos.logback/logback-classic "1.1.8"]
                 [ch.qos.logback/logback-core "1.1.8"]]
  :repl-options {:init-ns datahike-s3.repl})
