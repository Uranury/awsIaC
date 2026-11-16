#!/usr/bin/env bash
# Usage: ./teardown.sh <stack-name>
# Deletes the stack and everything in it. The ECR repository is emptied
# automatically (EmptyOnDelete), and the log groups are part of the stack.
set -euo pipefail

STACK="${1:-}"
if [[ -z "$STACK" ]]; then
  echo "usage: $0 <stack-name>" >&2
  exit 1
fi
export AWS_REGION="${AWS_REGION:-$(aws configure get region 2>/dev/null || true)}"
export AWS_REGION="${AWS_REGION:-eu-north-1}"
export AWS_PAGER=""

if ! aws cloudformation describe-stacks --stack-name "$STACK" >/dev/null 2>&1; then
  echo "stack $STACK does not exist in $AWS_REGION" >&2
  exit 1
fi

echo "==> deleting stack $STACK in $AWS_REGION"
aws cloudformation delete-stack --stack-name "$STACK"
if ! aws cloudformation wait stack-delete-complete --stack-name "$STACK"; then
  echo "delete failed; events:" >&2
  aws cloudformation describe-stack-events --stack-name "$STACK" \
    --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[LogicalResourceId,ResourceStatusReason]" --output table >&2
  exit 1
fi
echo "==> $STACK deleted"
