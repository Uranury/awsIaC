#!/usr/bin/env bash
# Usage: ./deploy.sh <stack-name>
#
# 1. Deploys template.yaml (creates the ECR repository and everything else;
#    on the very first run the ECS service has 0 tasks and there is no Lambda yet).
# 2. Builds the service and consumer images and pushes them to the stack's ECR repository.
# 3. Deploys the template again with the new image tag: the Lambda consumer is
#    created/updated and the ECS service rolls out DesiredCount tasks.
#
# Optional environment variables:
#   AWS_REGION      region (default: AWS CLI config, else eu-north-1)
#   DESIRED_COUNT   Fargate tasks (default 2)
#   ENVIRONMENT     EnvironmentName parameter (default dev)
#   VPC_ID, SUBNET_IDS  (comma-separated) network to use (default: the default VPC and its default subnets)
set -euo pipefail

STACK="${1:-}"
if [[ -z "$STACK" ]]; then
  echo "usage: $0 <stack-name>" >&2
  exit 1
fi
if [[ ! "$STACK" =~ ^[a-zA-Z][a-zA-Z0-9-]{0,40}$ ]]; then
  echo "stack name must start with a letter and contain only letters, digits and '-' (max 41 chars)" >&2
  exit 1
fi

cd "$(dirname "$0")"
export AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}"
export AWS_REGION="${AWS_REGION:-eu-north-1}"
export AWS_PAGER=""
DESIRED_COUNT="${DESIRED_COUNT:-2}"
ENVIRONMENT="${ENVIRONMENT:-dev}"

for tool in aws docker; do
  command -v "$tool" >/dev/null || { echo "$tool is not installed" >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "Docker daemon is not running" >&2; exit 1; }
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
echo "==> account $ACCOUNT_ID, region $AWS_REGION, stack $STACK"

# --- network ---------------------------------------------------------------
if [[ -z "${VPC_ID:-}" ]]; then
  VPC_ID="$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)"
  [[ "$VPC_ID" != "None" ]] || { echo "no default VPC; set VPC_ID and SUBNET_IDS" >&2; exit 1; }
fi
if [[ -z "${SUBNET_IDS:-}" ]]; then
  SUBNET_IDS="$(aws ec2 describe-subnets \
    --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true \
    --query 'Subnets[].SubnetId' --output text | tr '\t' ',')"
fi
echo "==> VPC $VPC_ID, subnets $SUBNET_IDS"

# A stack whose first create failed is stuck in ROLLBACK_COMPLETE and can only be deleted.
STATUS="$(aws cloudformation describe-stacks --stack-name "$STACK" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo NONE)"
if [[ "$STATUS" == "ROLLBACK_COMPLETE" ]]; then
  echo "==> $STACK is in ROLLBACK_COMPLETE from an earlier failed create; deleting it first"
  aws cloudformation delete-stack --stack-name "$STACK"
  aws cloudformation wait stack-delete-complete --stack-name "$STACK"
fi

cfn_deploy() {
  aws cloudformation deploy \
    --stack-name "$STACK" \
    --template-file template.yaml \
    --capabilities CAPABILITY_IAM \
    --no-fail-on-empty-changeset \
    --tags "Project=sis3" "Stack=$STACK" \
    --parameter-overrides "EnvironmentName=$ENVIRONMENT" "VpcId=$VPC_ID" "SubnetIds=$SUBNET_IDS" "$@"
}

output() {
  aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

# --- 1. infrastructure (keeps the current image tag if the stack exists) ----
echo "==> [1/3] deploying template"
cfn_deploy

REPO_URI="$(output EcrRepositoryUri)"
REGISTRY="${REPO_URI%%/*}"

# --- 2. images --------------------------------------------------------------
# Tags are immutable in ECR, so every deploy gets a new one.
TAG="$(date -u +%Y%m%d%H%M%S)"
if git rev-parse --short HEAD >/dev/null 2>&1; then
  TAG="$TAG-$(git rev-parse --short HEAD)"
fi
echo "==> [2/3] building and pushing images with tag $TAG to $REPO_URI"
aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null

for target in service consumer; do
  # Single-platform image without attestations: Lambda rejects multi-manifest image indexes.
  docker buildx build \
    --platform linux/amd64 \
    --provenance=false --sbom=false \
    --target "$target" \
    --tag "$REPO_URI:$target-$TAG" \
    --push .
done

# --- 3. roll out ------------------------------------------------------------
echo "==> [3/3] deploying template with ImageTag=$TAG, DesiredCount=$DESIRED_COUNT"
cfn_deploy "ImageTag=$TAG" "DesiredCount=$DESIRED_COUNT"

URL="$(output AlbUrl)"
echo "==> waiting for $URL/health"
for _ in $(seq 1 30); do
  if curl -fsS --max-time 5 "$URL/health" >/dev/null 2>&1; then
    break
  fi
  sleep 5
done
curl -fsS --max-time 5 "$URL/" && echo

echo
aws cloudformation describe-stacks --stack-name "$STACK" \
  --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output table
echo "Deployed. Try:  curl -X POST $URL/orders -H 'Content-Type: application/json' -d '{\"orderId\":\"o-1\",\"amount\":10}'"
