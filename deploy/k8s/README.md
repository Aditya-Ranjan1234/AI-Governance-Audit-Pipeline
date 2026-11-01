# EKS deployment

Use the Terraform outputs to replace the processing role ARN, five producer role ARNs, MSK bootstrap brokers, region, and eight immutable ECR image references in `all.yaml` and `producers.yaml`.

Create the database schema from a private runner that can reach RDS:

```bash
psql "$POSTGRES_DSN" -f db/001_schema.sql
```

Create the Kubernetes secret without committing the password:

```bash
kubectl create namespace governance --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic governance-database -n governance \
  --from-literal=dsn="$POSTGRES_DSN"
kubectl apply -f deploy/k8s/all.yaml
kubectl apply -f deploy/k8s/producers.yaml
```

MSK is configured with six default partitions and replication factor 3. Topics are created on the first producer/consumer metadata request. Verify the eight expected topics after deployment: five raw producer topics, two finding topics, and one dead-letter topic.

The rule engine runs three replicas in one Kafka Streams application group and can scale to six, matching the partition count. The PostgreSQL sink also scales through its consumer group. The Isolation Forest scorer stays at one replica because its bounded training history is local state; if it becomes a throughput bottleneck, move model training and inference behind a shared model service before scaling it horizontally.

Each producer adapter is a separate Deployment and Service with two starting replicas and its own HPA. Its IRSA role can write only to that producer's topic and the dead-letter topic. Route each source webhook only to its matching internal service; do not expose all adapter endpoints through one shared public route.
