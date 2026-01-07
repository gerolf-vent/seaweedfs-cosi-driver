# BucketClass configuration

The SeaweedFS COSI driver supports configuring storage parameters through BucketClass `parameters`. These parameters are translated into SeaweedFS `filer.conf` PathConf entries, which control how data is stored for each bucket and are created with the bucket.

> [!IMPORTANT]
> Because there is no locking on the `filer.conf`, multiple concurrent writes could lead to race conditions and an inconsistant configuration for some buckets. **Therefore you should only run one instance of the SeaweedFS COSI driver at any time.**

## Available Parameters

> [!NOTE]
> The `collection` parameter is automatically set to the bucket name and cannot be configured via BucketClass parameters. This ensures that every bucket is independent from any other files in SeaweedFS.

### `seaweedfs.objectstorage.k8s.io/replication`
Controls data replication across the SeaweedFS cluster.

**Format:** `XYZ` (See [replication string](https://github.com/seaweedfs/seaweedfs/wiki/Replication#replication-string))
- `X` = number of replicas in different data centers
- `Y` = number of replicas in different racks
- `Z` = number of replicas in different servers

See https://github.com/seaweedfs/seaweedfs/wiki/Replication.

---

### `seaweedfs.objectstorage.k8s.io/diskType`
Specifies the type of disk storage to use.

**Format:** lower-case alphanumerical

See https://github.com/seaweedfs/seaweedfs/wiki/Tiered-Storage.

---

### `seaweedfs.objectstorage.k8s.io/ttl`
Time-to-live for data. Objects will be automatically deleted after this period.

**Format:** `<number><unit>` (See [units](https://github.com/seaweedfs/seaweedfs/wiki/Store-file-with-a-Time-To-Live#supported-ttl-format))

See https://github.com/seaweedfs/seaweedfs/wiki/Store-file-with-a-Time-To-Live.

---

### `seaweedfs.objectstorage.k8s.io/volumeGrowthCount`
Number of volumes to create at once when new storage capacity is needed.

**Format:** Positive integer as string

---

### `seaweedfs.objectstorage.k8s.io/dataCenter`
Pin data to a specific data center.

**Format:** alphanumerical and `-`, `_` or `.`

---

### `seaweedfs.objectstorage.k8s.io/rack`
Pin data to a specific rack within a data center.

**Format:** alphanumerical and `-`, `_` or `.`

---

### `seaweedfs.objectstorage.k8s.io/dataNode`
Pin data to a specific data node (server).

**Format:** alphanumerical and `-`, `_` or `.`

## Example BucketClasses

### 1. Production SSD Storage
```yaml
kind: BucketClass
apiVersion: objectstorage.k8s.io/v1alpha1
metadata:
  name: ssd-replicated
driverName: seaweedfs.objectstorage.k8s.io
deletionPolicy: Delete
parameters:
  seaweedfs.objectstorage.k8s.io/replication: "001"
  seaweedfs.objectstorage.k8s.io/diskType: "ssd"
```

### 2. Archive Storage with TTL
```yaml
kind: BucketClass
apiVersion: objectstorage.k8s.io/v1alpha1
metadata:
  name: hdd-archive
driverName: seaweedfs.objectstorage.k8s.io
deletionPolicy: Retain
parameters:
  seaweedfs.objectstorage.k8s.io/replication: "010"
  seaweedfs.objectstorage.k8s.io/diskType: "hdd"
  seaweedfs.objectstorage.k8s.io/ttl: "365d"
```

### 4. Temporary Storage
```yaml
kind: BucketClass
apiVersion: objectstorage.k8s.io/v1alpha1
metadata:
  name: temp-storage
driverName: seaweedfs.objectstorage.k8s.io
deletionPolicy: Delete
parameters:
  seaweedfs.objectstorage.k8s.io/diskType: "ssd"
  seaweedfs.objectstorage.k8s.io/ttl: "24h"
```
