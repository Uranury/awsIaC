# SIS 3: Orders API on ECS Fargate with event-driven processing (CloudFormation)

`POST /orders` on the HTTP API (ECS Fargate behind an ALB) publishes an
`OrderPlaced` event to SNS. SNS delivers it to an SQS queue. A Lambda consumer
records the order exactly once, using a DynamoDB table of deduplication keys.
Messages that keep failing go to a dead-letter queue.

Everything is in one CloudFormation template (`template.yaml`). No resource
names are hardcoded, so you can deploy the stack several times under different
stack names in the same account.

```
client ──HTTP:80──► ALB ──► target group (ip, GET /health) ──► ECS Fargate service (2 tasks, cmd/service)
                                                                   │ sns:Publish (task role)
                                                                   ▼
                     SNS topic ──subscription (raw, filter type=OrderPlaced)──► SQS queue ──5 failed receives──► DLQ
                                                                                   │ event source mapping
                                                                                   ▼
                                                                  Lambda consumer (cmd/consumer) ──► DynamoDB dedupe table
```

## Prerequisites

- An AWS account, and the AWS CLI v2 logged in (`aws login` or `aws configure`)
  with permission to create the resources below (IAM roles included).
- A default region (`aws configure set region eu-north-1`), or set `AWS_REGION`.
- Docker running locally, with `buildx` (Docker Desktop includes it).
- A default VPC in that region. Otherwise set `VPC_ID` and `SUBNET_IDS`
  (comma-separated public subnets in at least two AZs).
- `bash` and `curl`. Go is **not** needed locally because the build runs inside Docker.

## Deploy (one command)

```bash
./deploy.sh <stack-name>        # e.g. ./deploy.sh sis3-a
```

The script:
1. deploys `template.yaml`. On a new stack this creates the ECR repository and all the infrastructure, with 0 tasks and no Lambda yet.
2. builds the `service` and `consumer` images (one Dockerfile, two targets) and pushes them as `service-<tag>` / `consumer-<tag>`. The tag is a UTC timestamp, because tags are immutable.
3. deploys the template again with `ImageTag=<tag>`. This creates or updates the Lambda and rolls the ECS service out to `DESIRED_COUNT` tasks (default 2).

It finishes by printing the stack outputs (ALB URL, ECR repository URI,
queue URL, …). Running it again deploys a new version.

Optional variables: `AWS_REGION`, `DESIRED_COUNT`, `ENVIRONMENT`, `VPC_ID`, `SUBNET_IDS`.

## Try it

```bash
URL=$(aws cloudformation describe-stacks --stack-name <stack-name> \
  --query "Stacks[0].Outputs[?OutputKey=='AlbUrl'].OutputValue" --output text)

curl $URL/                       # which task answered, which version
curl -X POST $URL/orders -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: evt-1' -d '{"orderId":"o-1","amount":42.5}'
curl -X POST $URL/orders -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: evt-1' -d '{"orderId":"o-1","amount":42.5}'   # duplicate, processed once
curl $URL/orders/evt-1           # what the consumer recorded (status COMPLETED)

# a poison message: fails 5 times, then lands in the DLQ (about 15 min, visibility timeout 185 s)
curl -X POST $URL/orders -H 'Content-Type: application/json' \
     -d '{"orderId":"o-bad","simulateFailure":true}'
```

## Teardown

```bash
./teardown.sh <stack-name>
```

This deletes the stack. The ECR repository is emptied automatically
(`EmptyOnDelete`), and both log groups belong to the stack, so nothing is left behind.

## Repository layout

| Path | What it is |
|---|---|
| `template.yaml` | The CloudFormation template (parameters, messaging, container, outputs). |
| `cmd/service/` | HTTP API: `/health`, `/`, `POST /orders`, `GET /orders/{eventId}`. |
| `cmd/consumer/` | Idempotent SQS consumer (Lambda, partial batch failures). |
| `Dockerfile` | Multi-stage build with two targets, `service` (distroless) and `consumer` (Lambda `provided:al2023`). |
| `deploy.sh` / `teardown.sh` | Deploy and delete a stack by name. |

## Design notes

- **Idempotency.** The API uses the `Idempotency-Key` header as the event ID
  (otherwise it generates a random one). The consumer claims the ID with a
  conditional `PutItem` (`IN_PROGRESS` with a 30 s lease), records the order,
  then marks the row `COMPLETED`. A redelivered or duplicate event finds
  `COMPLETED` and is skipped. Rows expire after 7 days (TTL), which is longer
  than the queue's 4-day retention.
- **Retries.** The Lambda reports only the failed messages
  (`ReportBatchItemFailures`). The visibility timeout is 185 s, which is
  6 × the 30 s function timeout plus the 5 s batching window.
  After `maxReceiveCount = 5`, SQS moves the message to the DLQ (14-day retention).
- **Two IAM roles for ECS.** The *execution role* is used by ECS to pull the
  image and write logs. The *task role* is used by the app, and allows only
  `sns:Publish` on the topic and `dynamodb:GetItem` on the table.
- **Security groups.** The ALB accepts port 80 from anywhere. The tasks accept
  port 8080 only from the ALB's security group.
- **Two-pass deploy.** The ECS service and the Lambda need an image, and the
  repository that holds the image is created by the same stack. So the first
  pass runs with an empty `ImageTag`: the service is created with 0 tasks and
  the Lambda isn't created (a condition).
- **No secrets.** Nothing secret is stored in the template or the repository.
  The tasks and the Lambda get temporary credentials from their roles.
