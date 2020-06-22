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
      (ml/transform (ml/tokeniser {:input-col :description
                                   :output-col :descriptors}))
      (ml/transform (ml/stop-words-remover {:input-col :descriptors
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


;; TODO: pipeline + indexer
(def model
  (ml/fit log-spending (ml/als {:max-iter   5
                                :reg-param  0.01
                                :user-col   :customer-id
                                :item-col   :descriptor
                                :rating-col :log-spend})))
