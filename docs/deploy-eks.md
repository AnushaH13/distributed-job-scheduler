# Deploying to AWS EKS

**Nothing in this repository creates AWS resources automatically.** Every step
below is run by hand, after reading the costs. No AWS credentials are assumed.

## 1. What it costs (read first)

AWS charges by the hour from the moment a resource exists, whether or not it
does any work. Approximate on-demand prices; **check the official pricing
pages for your region on the day you create anything**:

| Resource | Why we need it | Approx. price | Per month (730 h) |
|---|---|---|---|
| EKS control plane (standard support) | Kubernetes API server, managed by AWS | $0.10 / hour | ~$73 |
| 2 × t3.medium nodes | Run our pods | ~$0.04–0.05 / hour each | ~$60–75 |
| NAT gateway (1) | Private nodes reach the internet (ECR, Atlas) | ~$0.045–0.056 / hour + per GB | ~$35–45 + data |
| EBS volumes (2 × 20 GB gp3) | Node disks | ~$0.08–0.09 / GB-month | ~$4 |
| Load balancer (if the API is exposed) | Public entry point | ~$0.02–0.03 / hour + usage | ~$18–25 |
| ElastiCache (smallest node) | Managed Redis-compatible cache | ~$0.02 / hour | ~$12–20 |
| MongoDB Atlas M10 (on AWS) | Managed MongoDB replica set | see Atlas pricing | ~$55–65 |

**Rough total: $250–320 per month if left running.** For learning, create the
cluster, practise, and delete it the same day: a 4-hour session costs a few dollars.
EKS clusters on a Kubernetes version past standard support move to
extended support at $0.60 / hour (6×), so keep the version current.

Sources (check current values): <https://aws.amazon.com/eks/pricing/>,
<https://aws.amazon.com/ec2/pricing/on-demand/>, <https://aws.amazon.com/vpc/pricing/>,
<https://aws.amazon.com/elasticache/pricing/>, <https://www.mongodb.com/pricing>.

Set a billing alarm before you start: AWS Budgets, for example $20 per month
with an email alert (<https://aws.amazon.com/aws-cost-management/aws-budgets/>).

## 2. Managed dependencies

- **MongoDB:** use **MongoDB Atlas** on AWS in the same region, a replica set
  (which also enables multi-document transactions). Amazon DocumentDB
  implements only part of the MongoDB API; this project uses aggregation-pipeline
  updates with `$$NOW` and partial indexes, so DocumentDB would need careful
  verification first. Connect over VPC peering or PrivateLink, not the internet.
- **Redis:** **ElastiCache** (Valkey or Redis OSS) in *cluster mode disabled*.
  Our Lua scripts touch two keys (`js:delayed`, `js:ready`); cluster mode
  would need hash tags such as `js:{sched}:delayed` to keep them in one slot.
  Turn on encryption in transit and an auth token (`REDIS_PASSWORD`).

## 3. Steps

Prerequisites (installed on the machine you deploy from, after approval;
not needed for local development): `aws` CLI v2, `eksctl`, `kubectl`.

```bash
# 0. Credentials: your own AWS account, a named profile; never shared or work credentials.
export AWS_PROFILE=djs-learning
aws sts get-caller-identity                 # confirm WHICH account you are in

# 1. Container registry and image
aws ecr create-repository --repository-name djs --region ap-south-1
aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com
docker build -f deployments/Dockerfile -t ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/djs:$(git rev-parse --short HEAD) .
docker push ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/djs:$(git rev-parse --short HEAD)

# 2. Cluster (about 15-20 minutes; billing starts now)
eksctl create cluster -f deployments/eks/cluster.yaml

# 3. Secret with the Atlas URI and ElastiCache token (values never in Git)
kubectl create namespace djs
kubectl -n djs create secret generic djs-secrets \
  --from-literal=MONGO_URI='mongodb+srv://...' --from-literal=REDIS_PASSWORD='...'

# 4. Edit deployments/k8s/overlays/eks/kustomization.yaml (image, ElastiCache endpoint), then
kubectl apply -k deployments/k8s/overlays/eks
kubectl -n djs get pods -w
kubectl -n djs port-forward svc/api 8080:80     # test without a public load balancer

# 5. Delete everything when done
kubectl delete -k deployments/k8s/overlays/eks
eksctl delete cluster -f deployments/eks/cluster.yaml
aws ecr delete-repository --repository-name djs --force --region ap-south-1
```

## 4. Things to know

- The pods need Kubernetes 1.30 or newer (the `preStop.sleep` action).
- Liveness never checks dependencies; readiness does. A MongoDB outage makes
  pods unready (no traffic) instead of restarting them all.
- `terminationGracePeriodSeconds` (30 s) > `preStop` sleep (5 s) + `SHUTDOWN_TIMEOUT` (20 s).
- Scheduler: 2 replicas, one leader. Worker: HPA on CPU (2–10); KEDA on the
  Redis ready-list length is the better signal once there is real traffic.
- **Known gaps before real production use:**
  Go 1.24 no longer gets security fixes; mongo-driver v2.8.2 has a known
  `bson.Raw.Validate` issue fixed only in v2.9 (needs Go 1.25). Building
  release images with a newer Go inside Docker would fix both without touching
  the local Go version; this is not decided yet.
