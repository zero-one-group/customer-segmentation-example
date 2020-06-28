(ns customer-segmentation-example.core
  (:require
    [clojure.string]
    [zero-one.geni.core :as g]
    [zero-one.geni.ml :as ml]))

(defonce spark (g/create-spark-session {:checkpoint-dir "checkpoint/"}))

(def invoices
  (-> (g/read-csv! spark "/data/online_retail_ii")
      (g/select
        {:invoice      (g/int :Invoice)
         :stock-code   (g/int :StockCode)
         :description  (g/coalesce (g/lower :Description) (g/lit ""))
         :quantity     (g/int :Quantity)
         :invoice-date (g/to-timestamp :InvoiceDate "d/M/yyyy HH:mm")
         :price        (g/double :Price)
         :customer-id  (g/int "Customer ID")
         :country      :Country})))

(def descriptors
  (-> invoices
      (ml/transform
        (ml/tokeniser {:input-col  :description
                       :output-col :descriptors}))
      (ml/transform
        (ml/stop-words-remover {:input-col  :descriptors
                                :output-col :cleaned-descriptors}))
      (g/with-column :descriptor (g/explode :cleaned-descriptors))
      (g/with-column :descriptor (g/regexp-replace :descriptor
                                                   (g/lit "[^a-zA-Z'']")
                                                   (g/lit "")))
      (g/remove (g/< (g/length :descriptor) 3))))

(def log-spending
  (-> descriptors
      (g/remove (g/||
                  (g/null? :customer-id)
                  (g/< :price 0.01)
                  (g/< :quantity 1)))
      (g/group-by :customer-id :descriptor)
      (g/agg {:log-spend (g/log1p (g/sum (g/* :price :quantity)))})
      (g/order-by (g/desc :log-spend))
      g/cache))
(g/show log-spending)

(def pipeline
  (ml/pipeline
    (ml/string-indexer {:input-col  :descriptor
                        :output-col :descriptor-id})
    (ml/als {:max-iter    200
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

(def als-model (last (ml/stages pipeline-model)))

(def shared-patterns
  (-> (ml/item-factors als-model)
      (ml/transform id->descriptor)
      (g/select :descriptor (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column :pattern-rank
                     (g/windowed {:window-col (g/row-number)
                                  :partition-by :pattern-id
                                  :order-by     (g/desc :factor-weight)}))
      (g/filter (g/< :pattern-rank 9))
      (g/order-by :pattern-id (g/desc :factor-weight))
      (g/select :pattern-id :descriptor :factor-weight)))


(def customer-segments
  (-> (ml/user-factors als-model)
      (g/select (g/as :id :customer-id) (g/posexplode :features))
      (g/rename-columns {:pos :pattern-id
                         :col :factor-weight})
      (g/with-column :customer-rank
                     (g/windowed {:window-col (g/row-number)
                                  :partition-by :customer-id
                                  :order-by     (g/desc :factor-weight)}))
      (g/filter (g/= :customer-rank 1))))

(-> shared-patterns
    (g/group-by :pattern-id)
    (g/agg {:descriptors (g/array-sort (g/collect-set :descriptor))})
    (g/order-by :pattern-id)
    g/show)
; +----------+------------------------------------------------------------------------+
; |pattern-id|descriptors                                                             |
; +----------+------------------------------------------------------------------------+
; |0         |[eye, hanging, heart, holder, jun, peter, tigris, tlight]               |
; |1         |[bar, cabin, draw, garld, kashmiri, roccoco, seventeen, sideboard]      |
; |2         |[clockfuschia, coathangers, hot, jun, peter, pinkblack, rucksack, water]|
; |3         |[bag, design, jumbo, lunch, pink, red, retrospot, suki]                 |
; |4         |[bamboo, fig, retrodisc, rnd, scissor, sculpted, shapes, shelves]       |
; |5         |[afghan, capiz, frutti, lazer, pair, seventeen, sideboard, yellowblue]  |
; |6         |[cake, ceramic, fairy, hanging, metal, sign, stand, time]               |
; |7         |[circus, mintivory, necklturquois, paper, parade, regency, set, tin]    |
; +----------+------------------------------------------------------------------------+

(-> customer-segments
    (g/group-by :pattern-id)
    (g/agg {:n-customers (g/count-distinct :customer-id)})
    (g/order-by :pattern-id)
    g/show)
; +----------+-----------+
; |pattern-id|n-customers|
; +----------+-----------+
; |0         |659        |
; |1         |966        |
; |2         |431        |
; |3         |370        |
; |4         |2017       |
; |5         |621        |
; |6         |347        |
; |7         |467        |
; +----------+-----------+

(comment
  (g/print-schema (g/read-csv! spark "data/online_retail_ii"))
  ; |-- Invoice: string (nullable = true)
  ; |-- StockCode: string (nullable = true)
  ; |-- Description: string (nullable = true)
  ; |-- Quantity: string (nullable = true)
  ; |-- InvoiceDate: string (nullable = true)
  ; |-- Price: string (nullable = true)
  ; |-- Customer ID: string (nullable = true)
  ; |-- Country: string (nullable = true)

  (-> invoices (g/limit 2) g/show-vertical)
  ; -RECORD 0-------------------------------------------
  ;  invoice      | 489434
  ;  stock-code   | 85048
  ;  description  | 15cm christmas glass ball 20 lights
  ;  quantity     | 12
  ;  invoice-date | 2009-12-01 07:45:00
  ;  price        | 6.95
  ;  customer-id  | 13085
  ;  country      | United Kingdom
  ; -RECORD 1-------------------------------------------
  ;  invoice      | 489434
  ;  stock-code   | 79323P
  ;  description  | pink cherry lights
  ;  quantity     | 12
  ;  invoice-date | 2009-12-01 07:45:00
  ;  price        | 6.75
  ;  customer-id  | 13085
  ;  country      | United Kingdom

  (-> descriptors
      (g/group-by :descriptor)
      (g/agg {:total-quantity (g/sum :quantity)})
      (g/sort (g/desc :total-quantity))
      (g/limit 5)
      g/show)
  ; +----------+--------------+
  ; |descriptor|total-quantity|
  ; +----------+--------------+
  ; |bag       |1092311       |
  ; |set       |1033108       |
  ; |red       |946096        |
  ; |heart     |818834        |
  ; |pack      |685145        |
  ; +----------+--------------+

  (-> log-spending (g/describe :log-spend) g/show))
  ; +-------+--------------------+
  ; |summary|log-spend           |
  ; +-------+--------------------+
  ; |count  |837985              |
  ; |mean   |3.1732959032267756  |
  ; |stddev |1.3183533551301005  |
  ; |min    |0.058268908123975775|
  ; |max    |12.034516532838857  |
  ; +-------+--------------------+
