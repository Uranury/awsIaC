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
| `.github/workflows/` | `deploy.yml` (test → build → deploy on push to `master`) and `ci.yml` (tests on pull requests). |
| `infra/github-oidc.yaml` | One-time bootstrap: GitHub OIDC provider, deploy role, CloudFormation service role. |

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

## CI/CD

`.github/workflows/deploy.yml` tests, builds and deploys the stack `sis3-prod`
on every push to `master` (or by hand: *Actions → deploy → Run workflow*).
`.github/workflows/ci.yml` runs only the tests on pull requests, with no AWS access at all.

### How OIDC works here

When the deploy job starts, GitHub issues it a short-lived, signed OIDC token
(a JWT) that says which repository, environment and run it comes from.
`aws-actions/configure-aws-credentials` sends that token to AWS STS
(`AssumeRoleWithWebIdentity`) together with the deploy role's ARN.
STS checks the signature against the GitHub identity provider registered in
the account and checks the token's `aud` and `sub` claims against the role's
trust policy. If they match, it returns credentials that expire after at most
an hour, so there is no AWS key stored anywhere to leak or rotate.

### The trust policy, line by line (`infra/github-oidc.yaml`, `DeployRole`)

```yaml
Effect: Allow
Action: sts:AssumeRoleWithWebIdentity            # only via a web identity token, never with keys
Principal:
  Federated: arn:aws:iam::667291758695:oidc-provider/token.actions.githubusercontent.com
                                                 # the token must be signed by GitHub Actions' issuer
Condition:
  StringEquals:
    token.actions.githubusercontent.com:aud: sts.amazonaws.com
                                                 # the token was minted for AWS STS, not another audience
  StringLike:
    token.actions.githubusercontent.com:sub: repo:Uranury@156892404/awsIaC@1460415923:environment:production
                                                 # only this repository (pinned by owner ID and repo ID),
                                                 # and only jobs that run in the "production" environment
```

The repository was created after 15 July 2026, so GitHub uses the immutable
subject format with numeric IDs (`gh api repos/Uranury/awsIaC/actions/oidc/customization/sub`
returns `"use_immutable_subject": true`). A repository deleted and re-created
under the same name gets a new ID and can't assume the role. Because the
environment only accepts deployments from `master`, a workflow on another
branch can't get a `production` token either.

### Permissions: two roles

| Role | Assumed by | Can do |
|---|---|---|
| `DeployRole` | GitHub Actions (OIDC) | create/execute change sets on `sis3-prod` **only with** the service role, push and describe images in the stack's ECR repository, read the ECS service and its task definition |
| `CloudFormationExecutionRole` | CloudFormation (`--role-arn`) | create, update and delete the resources in `template.yaml`, scoped to names starting with `sis3-prod-` (roles, queues, topic, table, log groups, function, repository, cluster, service), security groups in the app's VPC, and load balancers in this account and region |

Trade-off: the pipeline can't touch anything outside its own stack, but
whoever can push to `master` can change `template.yaml`, and the service role
can create IAM roles named `sis3-prod-*` with any inline policy. Branch
protection on `master` is what guards that. The ALB and target group are
scoped to the account and region only, because CloudFormation truncates their
generated names (for example `sis3-a-LoadB-1h287baxwVLv`), so a name prefix
can't match them reliably.

CloudFormation keeps using the service role for every later operation on the
stack, including `./teardown.sh sis3-prod`. So delete `sis3-prod` **before** the
bootstrap stack.

### One-time bootstrap (admin credentials, run locally)

```bash
export AWS_REGION=eu-north-1 AWS_PAGER=""

# 1. Is there already a GitHub OIDC provider? (one per account)
aws iam list-open-id-connect-providers \
  --query "OpenIDConnectProviderList[?ends_with(Arn, 'token.actions.githubusercontent.com')]"
# empty list -> CreateOidcProvider=true, otherwise false

# 2. Deploy the bootstrap stack (never from the pipeline)
VPC_ID=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)
aws cloudformation deploy \
  --stack-name github-oidc-sis3 \
  --template-file infra/github-oidc.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides CreateOidcProvider=true VpcId="$VPC_ID"

aws cloudformation describe-stacks --stack-name github-oidc-sis3 \
  --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output table
```

### GitHub settings

```bash
REPO=Uranury/awsIaC
out() { aws cloudformation describe-stacks --stack-name github-oidc-sis3 \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }

# Environment "production": no reviewers, deployments only from master
gh api -X PUT "repos/$REPO/environments/production" --input - <<'JSON'
{"deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}}
JSON
gh api -X POST "repos/$REPO/environments/production/deployment-branch-policies" \
  -f name=master -f type=branch

# Environment variables (none of these is a secret)
gh variable set DEPLOY_ROLE_ARN --env production --repo "$REPO" --body "$(out DeployRoleArn)"
gh variable set CFN_ROLE_ARN    --env production --repo "$REPO" --body "$(out CloudFormationExecutionRoleArn)"
gh variable set AWS_REGION      --env production --repo "$REPO" --body eu-north-1
gh variable set STACK_NAME      --env production --repo "$REPO" --body sis3-prod
gh variable set VPC_ID          --env production --repo "$REPO" --body vpc-068ea30a35f8def3b
gh variable set SUBNET_IDS      --env production --repo "$REPO" \
  --body subnet-0d39664bbb3ef6f82,subnet-0f66983209ff26a31,subnet-0ba695f70732878ca

gh variable list --env production --repo "$REPO"
```

### Verify

1. *Actions → deploy*: the run triggered by the push is green. The job summary
   and the last step print the task definition ARN and the image tag `service-<commit SHA>`.
2. The same from the CLI:
   ```bash
   SHA=$(git rev-parse HEAD)
   CLUSTER=$(aws cloudformation describe-stacks --stack-name sis3-prod --query "Stacks[0].Outputs[?OutputKey=='ClusterName'].OutputValue" --output text)
   SERVICE=$(aws cloudformation describe-stacks --stack-name sis3-prod --query "Stacks[0].Outputs[?OutputKey=='ServiceName'].OutputValue" --output text)
   TD=$(aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" --query 'services[0].taskDefinition' --output text)
   aws ecs describe-task-definition --task-definition "$TD" --query 'taskDefinition.[taskDefinitionArn,containerDefinitions[0].image]' --output text
   # ...:task-definition/sis3-prod-TaskDefinition-...:N   ...:service-<SHA>
   ```
3. The role was assumed through OIDC, not with keys (CloudTrail, may take a few minutes):
   ```bash
   aws cloudtrail lookup-events --lookup-attributes AttributeKey=EventName,AttributeValue=AssumeRoleWithWebIdentity \
     --max-results 5 --query 'Events[].[EventTime,Username]' --output text
   ```
4. `gh secret list --repo Uranury/awsIaC` shows no AWS keys.
