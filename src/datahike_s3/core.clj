(ns datahike-s3.core
  (:require [clojure.spec.alpha :as s]
            datahike.config
            [datahike.store :refer [empty-store delete-store connect-store release-store scheme->index]]
            [konserve-s3.core :as kc-s3]
            [superv.async :refer [<?? S]]
            [hitchhiker.konserve :as kons]))

; add s3 to config backends
(s/def :datahike.config/backend #{:mem :file :pg :level :s3})

(defn- path->bucket
  [path]
  (let [parts (remove empty? (.split path "/"))]
    (if (= 1 (count parts))
      (first parts)
      (throw (ex-info "invalid path; S3 path should be just the bucket name, nothing else" {:path path})))))

(defmethod empty-store :s3
  [{:keys [host path]}]
  (kons/add-hitchhiker-tree-handlers
    (<?? S (kc-s3/empty-s3-store {:region host :bucket (path->bucket path)}))))

(defmethod delete-store :s3
  [{:keys [host path]}]
  (<?? S (kc-s3/delete-s3-store {:region host :bucket (path->bucket path)})))

(defmethod connect-store :s3
  [{:keys [host path]}]
  (<?? S (kc-s3/new-s3-store {:region host :bucket (path->bucket path)})))

(defmethod release-store :s3
  [_ store]
  (.close store))

(defmethod scheme->index :s3 [_]
  :datahike.index/hitchhiker-tree)