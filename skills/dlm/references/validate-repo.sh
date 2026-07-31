#!/usr/bin/env bash
# Validate a HuggingFace or ModelScope repo — checks existence, type, gated status.
# Usage: validate-repo.sh <url_or_repo> [source]
# Output: key=value pairs for the agent to parse.
#
# Examples:
#   validate-repo.sh https://huggingface.co/datasets/org/name
#   validate-repo.sh org/name hf
#   validate-repo.sh https://modelscope.cn/datasets/org/name

set -euo pipefail

RAW_INPUT="${1:-}"
SOURCE_HINT="${2:-}"

if [ -z "$RAW_INPUT" ]; then
  echo "ERROR=no input provided"
  exit 0
fi

# --- Normalize input ---

REPO_ID=""
SOURCE=""
TYPE_HINT=""

# HuggingFace URLs
if echo "$RAW_INPUT" | grep -qiE '(huggingface\.co|hf\.co)'; then
  SOURCE="hf"
  # Extract org/name from URL, handle datasets/ and models/ prefixes
  REPO_ID=$(echo "$RAW_INPUT" | sed -E 's|https?://(huggingface\.co\|hf\.co)/||' | sed 's|/$||')
  if echo "$REPO_ID" | grep -q '^datasets/'; then
    TYPE_HINT="dataset"
    REPO_ID=$(echo "$REPO_ID" | sed 's|^datasets/||')
  elif echo "$REPO_ID" | grep -q '^models/'; then
    TYPE_HINT="model"
    REPO_ID=$(echo "$REPO_ID" | sed 's|^models/||')
  fi
  # Strip query params and fragments
  REPO_ID=$(echo "$REPO_ID" | sed 's|[?#].*||' | sed 's|/tree/.*||' | sed 's|/blob/.*||')

# ModelScope URLs
elif echo "$RAW_INPUT" | grep -qiE 'modelscope\.cn'; then
  SOURCE="modelscope"
  REPO_ID=$(echo "$RAW_INPUT" | sed -E 's|https?://.*modelscope\.cn/||' | sed 's|/$||')
  if echo "$REPO_ID" | grep -q '^datasets/'; then
    TYPE_HINT="dataset"
    REPO_ID=$(echo "$REPO_ID" | sed 's|^datasets/||')
  elif echo "$REPO_ID" | grep -q '^models/'; then
    TYPE_HINT="model"
    REPO_ID=$(echo "$REPO_ID" | sed 's|^models/||')
  fi
  REPO_ID=$(echo "$REPO_ID" | sed 's|[?#].*||' | sed 's|/files.*||' | sed 's|/summary.*||')

# Bare repo_id (org/name)
elif echo "$RAW_INPUT" | grep -qE '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'; then
  REPO_ID="$RAW_INPUT"
  SOURCE="${SOURCE_HINT:-hf}"

else
  echo "ERROR=unrecognized input format"
  echo "INPUT=$RAW_INPUT"
  exit 0
fi

# Validate repo_id format
if ! echo "$REPO_ID" | grep -qE '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'; then
  echo "ERROR=invalid repo_id format"
  echo "REPO_ID=$REPO_ID"
  exit 0
fi

echo "REPO_ID=$REPO_ID"
echo "SOURCE=$SOURCE"

# --- Check existence ---

if [ "$SOURCE" = "hf" ]; then
  # Try dataset first, then model
  for try_type in dataset model; do
    if [ "$try_type" = "dataset" ]; then
      API_URL="https://huggingface.co/api/datasets/$REPO_ID"
    else
      API_URL="https://huggingface.co/api/models/$REPO_ID"
    fi

    HTTP_CODE=$(curl -s -o /tmp/hf_check.json -w "%{http_code}" \
      -H "Authorization: Bearer ${HF_TOKEN:-}" \
      "$API_URL" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "200" ]; then
      TYPE="${TYPE_HINT:-$try_type}"
      echo "EXISTS=true"
      echo "TYPE=$TYPE"

      # Check if gated
      GATED=$(python3 -c "import json; d=json.load(open('/tmp/hf_check.json')); print(d.get('gated', False))" 2>/dev/null || echo "unknown")
      if [ "$GATED" = "True" ] || [ "$GATED" = "true" ]; then
        echo "GATED=true"
      else
        echo "GATED=false"
      fi

      # Get approximate size if available
      SIZE=$(python3 -c "
import json
d=json.load(open('/tmp/hf_check.json'))
# cardData or dataset_info might have size
s = d.get('cardData', {}).get('dataset_size') or d.get('safetensors', {}).get('total', 0)
if s and int(s) > 0:
    print(f'SIZE_BYTES={s}')
" 2>/dev/null || true)
      [ -n "$SIZE" ] && echo "$SIZE"

      exit 0
    elif [ "$HTTP_CODE" = "401" ] || [ "$HTTP_CODE" = "403" ]; then
      echo "EXISTS=true"
      echo "TYPE=${TYPE_HINT:-$try_type}"
      echo "GATED=true"
      echo "NOTE=requires authentication (HF_TOKEN)"
      exit 0
    fi
  done

  echo "EXISTS=false"
  echo "NOTE=not found on HuggingFace (checked both datasets and models)"

elif [ "$SOURCE" = "modelscope" ]; then
  # ModelScope API check
  for try_type in dataset model; do
    if [ "$try_type" = "dataset" ]; then
      API_URL="https://modelscope.cn/api/v1/datasets/$REPO_ID"
    else
      API_URL="https://modelscope.cn/api/v1/models/$REPO_ID"
    fi

    HTTP_CODE=$(curl -s -o /tmp/ms_check.json -w "%{http_code}" \
      "$API_URL" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "200" ]; then
      # ModelScope answers 200 for both hit and miss; the body carries the
      # verdict as Code + a non-null Data object (there is no Success field).
      FOUND=$(python3 -c "
import json
d = json.load(open('/tmp/ms_check.json'))
print('yes' if d.get('Code') == 200 and d.get('Data') else 'no')
" 2>/dev/null || echo "no")
      if [ "$FOUND" = "yes" ]; then
        echo "EXISTS=true"
        echo "TYPE=${TYPE_HINT:-$try_type}"
        echo "GATED=false"
        exit 0
      fi
    fi
  done

  echo "EXISTS=false"
  echo "NOTE=not found on ModelScope (checked both datasets and models)"
fi
