(ns datahike-ddb+s3.core
  (:require [clojure.spec.alpha :as s]
            [datahike.store :refer [empty-store delete-store connect-store release-store scheme->index]]
            [konserve-ddb-s3.core :as kc-ddb-s3]
            [superv.async :refer [<?? S]]
            [hitchhiker.konserve :as kons]
            [clojure.tools.logging :as log]))

; add ddb+s3 backend. Is there a better way to add values to a set spec?
(s/def :datahike.config/backend #{:mem :file :pg :level :ddb+s3})

; bind these to custom clients if you want, e.g. to connect to local instances instead.
(def ^:dynamic *ddb-client* nil)
(def ^:dynamic *s3-client* nil)

(defn- ->config
  [{:keys [host path]}]
  (let [[table bucket database] (remove empty? (.split path "/"))]
    (if (or (nil? host) (nil? table) (nil? bucket))
      (throw (IllegalArgumentException. "ddb+s3 URL is of the form datahike:ddb+s3://<region>/<ddb-table>/<s3-bucket>[/<db-name>]")))
    {:region host
     :table table
     :bucket bucket
     :database (or database :datahike)
     :consistent-key #{:db}
     :ddb-client *ddb-client*
     :s3-client *s3-client*}))

(defmethod empty-store :ddb+s3
  [config]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (kc-ddb-s3/empty-store (->config config)))))

(defmethod delete-store :ddb+s3
  [config]
  (<?? S (kc-ddb-s3/delete-store (->config config))))

(defmethod connect-store :ddb+s3
  [config]
  (let [config (->config config)]
    (log/debug "ddb+s3 connect-store config:" (pr-str config))
    (<?? S (kc-ddb-s3/connect-store config))))

(defmethod release-store :ddb+s3
  [_ store]
  (.close store))

(defmethod scheme->index :ddb+s3
  [_]
  :datahike.index/hitchhiker-tree)