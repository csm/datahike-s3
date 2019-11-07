# datahike-s3

**Not doing much in this repo anymore;** see [this fork of datahike](https://github.com/csm/datahike/tree/aws), which
requires [this fork of hitchhiker-tree](https://github.com/csm/hitchhiker-tree/tree/separate-ops-buffer) and [this
konserve implementation](https://github.com/csm/konserve-ddb-s3).

Experimental.

Playing around with [datahike](https://github.com/replikativ/datahike)
on S3 and DynamoDB.

To try things out:

```clojure
(require '[datahike.api :as d])

; this step is important:
(require 'datahike-ddb+s3.core)

; URL format is datahike:ddb+s3://<region>/<ddb-table-name>/<s3-bucket-name>[/<database>]
; if you give a database argument, you can home multiple DBs in the same dynamodb table
; and s3 bucket. This defaults to something reasonable if you don't specify it.

; This will create the DynamoDB table and S3 bucket if they don't exist.
(d/create-database "datahike:ddb+s3://us-west-2/my-ddb-table/my-s3-bucket")

(def conn (d/connect "datahike:ddb+s3://us-west-2/my-ddb-table/my-s3-bucket"))
```
