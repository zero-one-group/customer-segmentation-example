(defproject customer-segmentation-example "0.0.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [zero.one/geni "0.0.22"]]
  :profiles {:provided
             {:dependencies [;; Spark
                             [com.github.fommil.netlib/all "1.1.2" :extension "pom"]
                             [org.apache.spark/spark-core_2.12 "3.0.0"]
                             [org.apache.spark/spark-hive_2.12 "3.0.0"]
                             [org.apache.spark/spark-mllib_2.12 "3.0.0"]
                             [org.apache.spark/spark-sql_2.12 "3.0.0"]
                             [org.apache.spark/spark-streaming_2.12 "3.0.0"]
                             [org.apache.spark/spark-yarn_2.12 "3.0.0"]]}
             :uberjar {:aot :all}}
  :target-path "target/%s")
