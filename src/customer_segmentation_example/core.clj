(ns customer-segmentation-example.core
  (:require
    [clojure.string]
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]))

(defonce spark (g/create-spark-session {}))

(def invoices
  (-> (g/read-csv! spark "/data/online_retail_ii")
      (g/select
        {:invoice      :Invoice
         :stock-code   :StockCode
         :description  (g/coalesce (g/lower :Description) (g/lit ""))
         :quantity     (g/int :Quantity)
         :invoice-date (g/to-timestamp :InvoiceDate "d/M/yyyy HH:mm")
         :price        :Price
         :customer-id  (g/int "Customer ID")
         :country      :Country})
      g/cache))

(def log-spending
  (-> invoices
      (ml/transform (ml/tokeniser {:input-col  :description
                                   :output-col :descriptors}))
      (ml/transform (ml/stop-words-remover {:input-col  :descriptors
                                            :output-col :cleaned-descriptors}))
      (g/with-column :descriptor (g/explode :cleaned-descriptors))
      (g/with-column :descriptor (g/regexp-replace :descriptor
                                                   (g/lit "[^a-zA-Z'']")
                                                   (g/lit "")))
      (g/remove (g/||
                  (g/null? :customer-id)
                  (g/< (g/length :descriptor) 3)
                  (g/< :price 0.01)
                  (g/< :quantity 1)))
      (g/group-by :customer-id :descriptor)
      (g/agg {:log-spend (g/log (g/sum (g/* :price :quantity)))})
      (g/order-by (g/desc :log-spend))
      g/cache))

(def pipeline
  (ml/pipeline
    (ml/string-indexer {:input-col  :descriptor
                        :output-col :descriptor-id})
    (ml/als {:max-iter    5
             :reg-param   0.01
             :rank        8
             :nonnegative true
             :user-col    :customer-id
             :item-col    :descriptor-id
             :rating-col  :log-spend})))

(def pipeline-model
  (ml/fit log-spending pipeline))

(def id->descriptor
  (ml/index-to-string {:input-col  :id
                       :output-col :descriptor
                       :labels (ml/labels (first (ml/stages pipeline-model)))}))

;; TODO: vector literals
;; TODO: .itemFactors .userFactors
(def shared-patterns
  (-> (.itemFactors (last (ml/stages pipeline-model)))
      (ml/transform id->descriptor)
      (g/select :descriptor (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column :pattern-rank
                      (g/over (g/row-number)
                              (g/window {:partition-by :pattern-id
                                         :order-by     (g/desc :factor-weight)})))
      (g/filter (g/< :pattern-rank 13))
      (g/order-by :pattern-id (g/desc :factor-weight))
      (g/select :pattern-id :descriptor :factor-weight)))


(def customer-segments
  (-> (.userFactors (last (ml/stages pipeline-model)))
      (g/select (g/as :id :customer-id) (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column :customer-rank
                      (g/over (g/row-number)
                              (g/window {:partition-by :customer-id
                                         :order-by     (g/desc :factor-weight)})))
      (g/filter (g/= :customer-rank 1))))

(-> shared-patterns
    (g/group-by :pattern-id)
    (g/agg {:descriptors (g/array-sort (g/collect-set "descriptor"))}) ;; TODO: change to keyword
    (g/order-by :pattern-id)
    g/show)

(-> customer-segments
    (g/group-by :pattern-id)
    (g/agg {:n-customers (g/count-distinct :customer-id)})
    (g/order-by :pattern-id)
    g/show)

