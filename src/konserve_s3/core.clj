(ns konserve-s3.core
  (:require [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [cognitect.anomalies :as anomalies]
            [cognitect.aws.client.api :as aws-client]
            [cognitect.aws.client.api.async :as aws]
            [konserve.protocols :as kp]
            [konserve.serializers :as ser]
            [clojure.java.io :as io])
  (:import [java.util Base64]
           [clojure.lang ExceptionInfo IObj]
           [java.io Closeable ByteArrayOutputStream PushbackReader]))

(defn- encode-key
  [key]
  (.encodeToString (Base64/getUrlEncoder) (.getBytes (pr-str key) "UTF-8")))

(defn- decode-key
  [key]
  (->> (.getBytes key "UTF-8")
       (.decode (Base64/getUrlDecoder))
       (io/reader)
       (PushbackReader.)
       (edn/read)))

(defrecord S3Store [client bucket read-handlers write-handlers serializer locks]
  Closeable
  (close [_] (aws-client/stop client))

  kp/PEDNAsyncKeyValueStore
  (-exists? [_ key]
    (log/debug {:task ::kp/-exists?
                :component ::S3Store
                :key key})
    (async/go
      (let [response (async/<! (aws/invoke client {:op :ListObjectsV2
                                                   :request {:Bucket bucket
                                                             :MaxKeys 1
                                                             :Prefix (str "edn/" (encode-key key))}}))]
        (not (empty? (:Contents response))))))

  (-get-in [_ key-vec]
    (log/debug {:task ::kp/-get-in
                :component ::S3Store
                :key-vec key-vec})
    (async/go
      (let [[object-key & ks] key-vec]
        (if (empty? ks)
          (let [prefix (str "edn/" (encode-key object-key) \/)]
            (loop [result (volatile! (transient {}))
                   token nil]
              (let [response (async/<! (aws/invoke client {:op :ListObjectsV2
                                                           :request {:Bucket bucket
                                                                     :Prefix prefix
                                                                     :ContinuationToken token}}))]
                (if (s/valid? ::anomalies/anomaly response)
                  (ex-info "error making S3 request" {:error response})
                  (let [res (loop [object-keys (:Contents response)]
                              (when-let [object-key (some-> object-keys first :Key)]
                                (let [object-resp (async/<! (aws/invoke client {:op      :GetObject
                                                                                :request {:Bucket bucket
                                                                                          :Key    object-key}}))]
                                  (if (s/valid? ::anomalies/anomaly object-resp)
                                    object-resp
                                    (let [res (try
                                                (let [k (->> (re-matches #"edn/([^/]+)/([^/]+)" object-key)
                                                             (last)
                                                             (decode-key))
                                                      object (kp/-deserialize serializer read-handlers (:Body object-resp))]
                                                  (log/debug :key object-key :object object)
                                                  (vswap! result assoc! k
                                                          (if (instance? IObj object)
                                                            (with-meta object {::version (:VersionId object-resp)})
                                                            object)))
                                                (catch Throwable t t))]
                                      (if (instance? Throwable res)
                                        res
                                        (recur (rest object-keys))))))))]
                    (cond
                      (s/valid? ::anomalies/anomaly res)
                      (ex-info "error making S3 request" {:error res})

                      (instance? Throwable res)
                      res

                      (:IsTruncated response)
                      (recur result (:NextContinuationToken response))

                      :else (not-empty (persistent! @result))))))))
          (let [[kk & ks] ks
                response (async/<! (aws/invoke client {:op :GetObject
                                                       :request {:Bucket  bucket
                                                                 :Key     (str "edn/" (encode-key object-key) \/ (encode-key kk))}}))]
            (cond
              (and (s/valid? ::anomalies/anomaly response)
                   (= ::anomalies/not-found (::anomalies/category response)))
              nil

              (s/valid? ::anomalies/anomaly response)
              (ex-info "failed to read object" {:error response :key-vec [object-key kk]})

              :else
              (try
                (let [object (kp/-deserialize serializer write-handlers (:Body response))]
                  (if (not-empty ks)
                    (with-meta (get-in object ks) {::version-id (:VersionId response)})
                    (with-meta object {::version-id (:VersionId response)})))
                (catch Exception x x))))))))

  (-update-in [this key-vec up-fn]
    (log/debug {:task ::kp/-update-in
                :component ::S3Store
                :key-vec key-vec
                :up-fn up-fn})
    (async/go-loop []
      (let [[k1 k2 & ks] key-vec
            result (try
                     (let [object-key (str "edn/" (encode-key k1) \/ (encode-key k2))
                           current-container (async/<! (kp/-get-in this [k1 k2]))
                           _ (log/debug :current-container current-container)
                           current-container (if (instance? Throwable current-container)
                                               (throw current-container)
                                               current-container)
                           current-value (get-in current-container ks)
                           new-value (up-fn current-value)
                           new-container (if (empty? ks)
                                           new-value
                                           (assoc-in current-container ks new-value))
                           out (ByteArrayOutputStream.)
                           encoded (do
                                     (kp/-serialize serializer out write-handlers new-container)
                                     (.toByteArray out))
                           upload (let [res (async/<! (aws/invoke client {:op :CreateMultipartUpload
                                                                          :request {:Bucket bucket
                                                                                    :Key object-key
                                                                                    :ContentType "application/edn"}}))]
                                    (if (s/valid? ::anomalies/anomaly res)
                                      (throw (ex-info "starting upload failed" {:error res}))
                                      (do
                                        (log/debug :upload-id (:UploadId res))
                                        res)))
                           part-upload (let [res (async/<! (aws/invoke client {:op :UploadPart
                                                                               :request {:Bucket bucket
                                                                                         :Key object-key
                                                                                         :UploadId (:UploadId upload)
                                                                                         :PartNumber 1
                                                                                         :Body encoded}}))]
                                         (if (s/valid? ::anomalies/anomaly res)
                                           (throw (ex-info "uploading failed" {:error res}))
                                           res))
                           current-metadata (let [res (async/<! (aws/invoke client {:op :HeadObject
                                                                                    :request {:Bucket bucket
                                                                                              :Key object-key}}))]
                                              (cond (and (s/valid? ::anomalies/anomaly res)
                                                         (= ::anomalies/not-found (::anomalies/category res)))
                                                    nil

                                                    (s/valid? ::anomalies/anomaly res)
                                                    (throw (ex-info "failed to get target object info" {:error res}))

                                                    :else res))]
                       ; this is not fully atomic; it might be possible to wrangle
                       ; something with the current S3 API to make it atomic, possibly
                       ; with the help of another service that offers atomicity (such as
                       ; DynamoDB). But for now we're just winging it.
                       (if (= (:VersionId current-metadata) (some-> current-container meta ::version-id))
                         (let [commit (async/<! (aws/invoke client {:op :CompleteMultipartUpload
                                                                    :request {:Bucket          bucket
                                                                              :Key             object-key
                                                                              :MultipartUpload {:Parts [{:ETag (:ETag part-upload)
                                                                                                         :PartNumber 1}]}
                                                                              :UploadId        (:UploadId upload)}}))]
                           (if (s/valid? ::anomalies/anomaly commit)
                             (throw (ex-info "failed to commit upload" {:error commit}))
                             [current-value new-value]))
                         (let [abort (async/<! (aws/invoke client {:op :AbortMultipartUpload
                                                                   :request {:Bucket   bucket
                                                                             :Key      object-key
                                                                             :UploadId (:UploadId upload)}}))]
                           (if (s/valid? ::anomalies/anomaly abort)
                             (throw (ex-info "failed to abort upload" {:error abort}))
                             ::retry))))
                     (catch Throwable t t))]
        (cond
          (vector? result) result
          (instance? Throwable result) result
          :else (recur)))))

  (-assoc-in [this key-vec val]
    (log/debug {:task ::kp/-assoc-in
                :component ::S3Store
                :key-vec key-vec
                :val val})
    (if (and (map? val) (= 1 (count key-vec)))
      (async/go-loop [kvs (seq val)]
        (when-let [[k v] (first kvs)]
          (let [result (async/<! (kp/-update-in this (conj (vec key-vec) k) (constantly v)))]
            (if (instance? Throwable result)
              result
              (recur (rest kvs))))))
      (kp/-update-in this key-vec (constantly val))))

  (-dissoc [_ key]
    (async/go
      (loop [token nil]
        (let [response (async/<! (aws/invoke client {:op :ListObjectsV2
                                                     :request {:Bucket bucket
                                                               :Prefix (str "edn/" (encode-key key) \/)
                                                               :ContinuationToken token}}))]
          (if (s/valid? ::anomalies/anomaly response)
            (ex-info "failed to list objects" {:error response})
            (do
              (loop [ks (:Contents response)]
                (when-let [k (first ks)]
                  (let [response (async/<! (aws/invoke client {:op :DeleteObject
                                                               :request {:Bucket bucket
                                                                         :Key (:Key k)}}))]
                    (comment "handle response at all? Abort if we fail to delete? Throttle and retry if we exceed requests?"))
                  (recur (rest ks))))
              (when (:IsTruncated response)
                (recur (:NextContinuationToken response))))))))))

  ;kp/PBinaryAsyncKeyValueStore TODO
  ;(-bget [_ key locked-cb])
  ;(-bassoc [_ key val]))

(defn new-s3-store
  [{:keys [bucket region read-handlers write-handlers serializer]
    :or   {read-handlers  (atom {})
           write-handlers (atom {})
           serializer     (ser/fressian-serializer)}}]
  {:pre [(string? bucket) (not (empty? bucket))]}
  (async/go
    (let [client (aws-client/client {:api :s3 :region region})
          versioning (async/<! (aws/invoke client {:op :GetBucketVersioning
                                                   :request {:Bucket bucket}}))]
      (cond (s/valid? ::anomalies/anomaly versioning)
            (ex-info "error looking up bucket" {:error versioning})

            (not= "Enabled" (:Status versioning))
            (ex-info "versioning not enabled in bucket" {:bucket bucket})

            :else
            (->S3Store client bucket read-handlers write-handlers serializer (atom {}))))))

(defn empty-s3-store
  [{:keys [bucket region read-handlers write-handlers serializer]
    :or   {read-handlers  (atom {})
           write-handlers (atom {})
           serializer     (ser/fressian-serializer)}}]
  (async/go
    (let [client (aws-client/client {:api :s3 :region region})
          create-bucket (aws-client/invoke client
                                           {:op :CreateBucket
                                            :request {:Bucket bucket
                                                      :CreateBucketConfiguration {:LocationConstraint region}}})]
      (if (s/valid? ::anomalies/anomaly create-bucket)
        (throw (ex-info "failed to create bucket" {:error create-bucket}))
        (let [version-config (aws-client/invoke client
                                                {:op :PutBucketVersioning
                                                 :request {:Bucket bucket
                                                           :VersioningConfiguration {:Status "Enabled"}}})]
          (if (s/valid? ::anomalies/anomaly version-config)
            (throw (ex-info "failed to enable versioning; your new bucket is not deleted, and is misconfigured for usage by this library"
                            {:error version-config}))
            (->S3Store client bucket read-handlers write-handlers serializer (atom {}))))))))

(defn delete-s3-store
  [{:keys [bucket region]}]
  (async/go
    (let [client (aws-client/client {:api :s3 :region region})
          result (async/<! (aws/invoke client
                                       {:op :DeleteBucket
                                        :request {:Bucket bucket}}))]
      (when (s/valid? ::anomalies/anomaly result)
        (ex-info "failed to delete S3 store" {:error result})))))