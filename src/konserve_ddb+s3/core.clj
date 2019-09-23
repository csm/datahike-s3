(ns konserve-ddb+s3.core
  "Konserve store implementation using DynamoDB and S3.

  This assumes usage by datahike, it's not intended to be a
  generic Konserve store.

  By default, the :db toplevel key is always stored in DynamoDB,
  and is subject to atomicity guarantees; all other keys are stored
  in S3, and do *not* provide atomicity guarantees. The set of toplevel
  keys that are stored in DynamoDB is configurable, if you want to try
  something different."
  (:require [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api.async :as aws]
            [konserve.protocols :as kp]
            [konserve.serializers :as ser]
            [cognitect.aws.client.api :as aws-client]
            [clojure.tools.logging :as log])
  (:import [java.io PushbackReader ByteArrayOutputStream ByteArrayInputStream Closeable]
           [java.util Base64]
           [java.time Clock Duration]))

(defn- encode-key
  "Encodes a key to URL-safe Base64."
  [prefix key]
  (.encodeToString (Base64/getUrlEncoder) (.getBytes (pr-str [prefix key]) "UTF-8")))

(defn- decode-key
  "Decodes a URL-safe Base64 key string to its components, [prefix key]."
  [key]
  (->> (.getBytes key "UTF-8")
       (.decode (Base64/getUrlDecoder))
       (io/reader)
       (PushbackReader.)
       (edn/read)))

(defn- nano-clock
  ([] (nano-clock (Clock/systemUTC)))
  ([clock]
   (let [initial-instant (.instant clock)
         initial-nanos (System/nanoTime)]
     (proxy [Clock] []
       (getZone [] (.getZone clock))
       (withZone [zone] (nano-clock (.withZone clock zone)))
       (instant [] (.plusNanos initial-instant (- (System/nanoTime) initial-nanos)))))))

(defn- ms
  [clock begin]
  (-> (Duration/between begin (.instant clock))
      (.toNanos)
      (double)
      (/ 1000000.0)))

(defrecord DDB+S3Store [ddb-client s3-client table-name bucket-name dynamo-prefix s3-prefix serializer read-handlers write-handlers ddb-keys locks clock]
  kp/PEDNAsyncKeyValueStore
  (-exists? [_ key]
    (let [begin (.instant clock)]
      (async/go
        (if (ddb-keys key)
          (let [ek (encode-key dynamo-prefix key)
                response (async/<! (aws/invoke ddb-client {:op      :GetItem
                                                           :request {:TableName       table-name
                                                                     :Key             {"id" {:S ek}}
                                                                     :AttributesToGet ["id"]
                                                                     :ConsistentRead  true}}))]
            (log/debug {:task :ddb-get-item :phase :end :key ek :ms (ms clock begin)})
            (if (s/valid? ::anomalies/anomaly response)
              (ex-info "failed to read dynamodb" {:error response})
              (not (empty? response))))
          (let [ek (encode-key s3-prefix key)
                response (async/<! (aws/invoke s3-client {:op      :HeadObject
                                                          :request {:Bucket bucket-name
                                                                    :Key    ek}}))]
            (log/debug {:task :s3-head-object :phase :end :key ek :ms (ms clock begin)})
            (cond (and (s/valid? ::anomalies/anomaly response)
                       (= ::anomalies/not-found (::anomalies/category response)))
                  false

                  (s/valid? ::anomalies/anomaly response)
                  (ex-info "failed to read S3" {:error response})

                  :else
                  true))))))

  (-get-in [_ key-vec]
    (let [begin (.instant clock)]
      (async/go
        (let [[k & ks] key-vec
              value (if (ddb-keys k)
                      (let [ek (encode-key dynamo-prefix k)
                            response (async/<! (aws/invoke ddb-client {:op      :GetItem
                                                                       :request {:TableName       table-name
                                                                                 :Key             {"id" {:S ek}}
                                                                                 :AttributesToGet ["val"]
                                                                                 :ConsistentRead  true}}))]
                        (log/debug {:task :ddb-get-item :phase :end :key ek :ms (ms clock begin)})
                        (cond (empty? response) nil

                              (s/valid? ::anomalies/anomaly response)
                              (ex-info "failed to read dynamodb" {:error response})

                              :else
                              (kp/-deserialize serializer read-handlers (-> response :Item :val :B))))
                      (let [ek (encode-key s3-prefix k)
                            response (async/<! (aws/invoke s3-client {:op      :GetObject
                                                                      :request {:Bucket bucket-name
                                                                                :Key    ek}}))]
                        (log/debug {:task :s3-get-object :phase :end :key ek :ms (ms clock begin)})
                        (cond (and (s/valid? ::anomalies/anomaly response)
                                   (= ::anomalies/not-found (::anomalies/category response)))
                              nil

                              (s/valid? ::anomalies/anomaly response)
                              (ex-info "failed to read S3" {:error response})

                              :else
                              (kp/-deserialize serializer read-handlers (:Body response)))))]
          (if (instance? Throwable value)
            value
            (get-in value ks))))))

  (-update-in [_ key-vec up-fn]
    (let [begin (.instant clock)]
      (async/go
        (try
          (let [[k & ks] key-vec
                result (if (ddb-keys k)
                         (loop []
                           (let [ek (encode-key dynamo-prefix k)
                                 response (async/<! (aws/invoke ddb-client {:op      :GetItem
                                                                            :request {:TableName       table-name
                                                                                      :Key             {"id" {:S ek}}
                                                                                      :AttributesToGet ["val" "rev"]
                                                                                      :ConsistentRead  true}}))]
                             (log/debug {:task :ddb-get-item :phase :end :key ek :ms (ms clock begin)})
                             (log/debug :response response)
                             (if (s/valid? ::anomalies/anomaly response)
                               (ex-info "failed to read dynamodb" {:error response})
                               (let [current-value (some->> response :Item :val :B (kp/-deserialize serializer read-handlers))
                                     _ (log/debug "current value" current-value)
                                     current-rev (some-> response :Item :rev :N (Long/parseLong))
                                     _ (log/debug "current rev" current-rev)
                                     new-value (if (empty? ks)
                                                 (up-fn current-value)
                                                 (update-in current-value ks up-fn))
                                     _ (log/debug "new-value" new-value)
                                     new-rev (some-> current-rev inc)
                                     _ (log/debug "new-rev" new-rev)
                                     encoded (let [out (ByteArrayOutputStream.)]
                                               (kp/-serialize serializer out write-handlers new-value)
                                               (.toByteArray out))
                                     response (async/<! (aws/invoke ddb-client (if (some? current-value)
                                                                                 {:op      :UpdateItem
                                                                                  :request {:TableName                 table-name
                                                                                            :Key                       {"id" {:S ek}}
                                                                                            :UpdateExpression          "SET rev = :newrev, val = :newval"
                                                                                            :ConditionExpression       "attribute_exists(id) AND rev = :oldrev"
                                                                                            :ExpressionAttributeValues {":oldrev" {:N (str current-rev)}
                                                                                                                        ":newrev" {:N (str new-rev)}
                                                                                                                        ":newval" {:B encoded}}}}
                                                                                 {:op      :PutItem
                                                                                  :request {:TableName table-name
                                                                                            :Item {"id" {:S (encode-key dynamo-prefix k)}
                                                                                                   "rev" {:N "0"}
                                                                                                   "val" {:B encoded}}
                                                                                            :ConditionExpression "attribute_not_exists(id)"}})))]
                                 (log/debug {:task :ddb-put-item :phase :end :key ek :ms (ms clock begin)})
                                 (log/debug :response response)
                                 ; return values for conditional operation failures don't seem that "strongly typed"...
                                 ; I'm not sure what to rely on: category? message? __type?
                                 (cond (and (s/valid? ::anomalies/anomaly response)
                                            (= ::anomalies/incorrect (::anomalies/category response))
                                            (or (= "The conditional request failed" (:message response))
                                                (.contains (:__type response "") "ConditionalCheckFailedException")))
                                       (recur)

                                       (s/valid? ::anomalies/anomaly response)
                                       (ex-info "failed to update item" {:error response})

                                       :else
                                       [(get-in current-value ks) (get-in new-value ks)])))))
                         (let [ek (encode-key s3-prefix k)
                               response (async/<! (aws/invoke s3-client {:op      :GetObject
                                                                         :request {:Bucket bucket-name
                                                                                   :Key    ek}}))
                               current-value (do
                                               (log/debug {:task :s3-get-object :phase :end :key ek :ms (ms clock begin)})
                                               (cond (and (s/valid? ::anomalies/anomaly response)
                                                          (= ::anomalies/not-found (::anomalies/category response)))
                                                     nil

                                                     (s/valid? ::anomalies/anomaly response)
                                                     (ex-info "failed to read S3 object" {:error response})

                                                     :else
                                                     (kp/-deserialize serializer read-handlers (:Body response))))
                               error? (instance? Throwable current-value)
                               new-value (when-not error?
                                           (if (empty? ks)
                                             (up-fn current-value)
                                             (update-in current-value ks up-fn)))
                               encoded (when-let [out (when-not error?
                                                        (ByteArrayOutputStream.))]
                                         (kp/-serialize serializer out write-handlers new-value)
                                         (.toByteArray out))
                               response (when-not error?
                                          (async/<! (aws/invoke s3-client {:op :PutObject
                                                                           :request {:Bucket bucket-name
                                                                                     :Key    ek
                                                                                     :Body   encoded}})))]
                           (when (some? response)
                             (log/debug {:task :s3-put-object :phase :end :key ek :ms (ms clock begin)}))
                           (cond error?
                                 current-value

                                 (s/valid? ::anomalies/anomaly response)
                                 (ex-info "failed to put S3 object" {:error response})

                                 :else
                                 [(get-in current-value ks) (get-in new-value ks)])))]
            result)
          (catch Throwable t t)))))

  (-assoc-in [this key-vec val]
    (kp/-update-in this key-vec (constantly val)))

  (-dissoc [_ key]
    (let [begin (.instant clock)]
      (async/go
        (if (ddb-keys key)
          (let [ek (encode-key dynamo-prefix key)
                response (async/<! (aws/invoke ddb-client {:op :DeleteItem
                                                           :request {:TableName table-name
                                                                     :Key {"id" {:S ek}}}}))]
            (log/debug {:task :ddb-delete-item :phase :end :key ek :ms (ms clock begin)})
            (when (s/valid? ::anomalies/anomaly response)
              (ex-info "failed to delete dynamodb item" {:error response})))
          (let [ek (encode-key s3-prefix key)
                response (async/<! (aws/invoke s3-client {:op :DeleteObject
                                                          :request {:Bucket bucket-name
                                                                    :Key    ek}}))]
            (log/debug {:task :s3-delete-object :phase :end :key ek :ms (ms clock begin)})
            (when (and (s/valid? ::anomalies/anomaly response)
                       (not= ::anomalies/not-found (::anomalies/category response)))
              (ex-info "failed to delete S3 item" {:error response})))))))

  Closeable
  (close [_]
    (aws-client/stop ddb-client)
    (aws-client/stop s3-client)))

(defn empty-store
  [{:keys [region table bucket database serializer read-handlers write-handlers read-throughput write-throughput]
    :or   {serializer (ser/fressian-serializer)
           read-handlers (atom {})
           write-handlers (atom {})
           read-throughput 5
           write-throughput 5}}]
  (async/go
    (let [ddb-client (aws-client/client {:api :dynamodb :region region})
          s3-client (aws-client/client {:api :s3 :region region :http-client (-> ddb-client .-info :http-client)})
          table-exists (async/<! (aws/invoke ddb-client {:op :DescribeTable
                                                         :request {:TableName table}}))
          table-ok (if (s/valid? ::anomalies/anomaly table-exists)
                     (async/<! (aws/invoke ddb-client {:op :CreateTable
                                                       :request {:TableName table
                                                                 :AttributeDefinitions [{:AttributeName "id"
                                                                                         :AttributeType "S"}]
                                                                 :KeySchema [{:AttributeName "id"
                                                                              :KeyType "HASH"}]
                                                                 :ProvisionedThroughput {:ReadCapacityUnits read-throughput
                                                                                         :WriteCapacityUnits write-throughput}}}))
                     (if (and (= [{:AttributeName "id" :AttributeType "S"}] (-> table-exists :Table :AttributeDefinitions))
                              (= [{:AttributeName "id" :KeyType "HASH"}] (-> table-exists :Table :KeySchema)))
                       :ok
                       {::anomalies/category ::anomalies/incorrect
                        ::anomalies/message "table exists but has different attribute definitions or key schema than expected"}))]
      (if (s/valid? ::anomalies/anomaly table-ok)
        (ex-info "failed to initialize dynamodb" {:error table-ok})
        (let [bucket-exists (async/<! (aws/invoke s3-client {:op :HeadBucket :request {:Bucket bucket}}))
              bucket-ok (cond (and (s/valid? ::anomalies/anomaly bucket-exists)
                                   (= ::anomalies/not-found (::anomalies/category bucket-exists)))
                              (async/<! (aws/invoke s3-client {:op :CreateBucket
                                                               :request {:Bucket bucket
                                                                         :CreateBucketConfiguration {:LocationConstraint region}}}))

                              (s/valid? ::anomalies/anomaly bucket-exists)
                              bucket-exists

                              :else :ok)]
          (if (s/valid? ::anomalies/anomaly bucket-ok)
            (ex-info "failed to initialize S3 (your dynamodb table will not be deleted if it was created)" {:error bucket-ok})
            (->DDB+S3Store ddb-client s3-client table bucket database database serializer read-handlers write-handlers #{:db} (atom {}) (nano-clock))))))))

(defn delete-store
  [config]
  (async/go (ex-info "not yet implemented" {})))

(defn connect-store
  [{:keys [region table bucket database serializer read-handlers write-handlers read-throughput write-throughput]
    :or   {serializer (ser/fressian-serializer)
           read-handlers (atom {})
           write-handlers (atom {})
           read-throughput 5
           write-throughput 5}}]
  (async/go
    (let [ddb-client (aws-client/client {:api :dynamodb :region region})
          s3-client (aws-client/client {:api :s3 :region region :http-client (-> ddb-client .-info :http-client)})
          table-ok (async/<! (aws/invoke ddb-client {:op :DescribeTable :request {:TableName table}}))
          table-ok (if (s/valid? ::anomalies/anomaly table-ok)
                     table-ok
                     (when-not (and (= [{:AttributeName "id" :AttributeType "S"}] (-> table-ok :Table :AttributeDefinitions))
                                    (= [{:AttributeName "id" :KeyType "HASH"}] (-> table-ok :Table :KeySchema)))
                       {::anomalies/category ::anomalies/incorrect
                        ::anomalies/message "table has invalid attribute definitions or key schema"}))]
      (if (s/valid? ::anomalies/anomaly table-ok)
        (ex-info "invalid dynamodb table" {:error table-ok})
        (let [bucket-ok (async/<! (aws/invoke s3-client {:op :HeadBucket :request {:Bucket bucket}}))]
          (if (s/valid? ::anomalies/anomaly bucket-ok)
            (ex-info "invalid S3 bucket" {:error bucket-ok})
            (->DDB+S3Store ddb-client s3-client table bucket database database serializer read-handlers write-handlers #{:db} (atom {}) (nano-clock))))))))