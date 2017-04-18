#!/usr/bin/env bash

STATUS="$1"
TEXT="$2"

REPO="${TRAVIS_REPO_SLUG}"
if [[ "${TRAVIS_PULL_REQUEST}" == "false" ]]; then
    SHA="${TRAVIS_COMMIT}"
    CONTEXT="flake8/push"
else
    SHA="${TRAVIS_PULL_REQUEST_SHA}"
    CONTEXT="flake8/pr"
fi
LOG_URL="https://travis-ci.org/${TRAVIS_REPO_SLUG}/jobs/${TRAVIS_JOB_ID}"


curl "https://api.github.com/repos/${REPO}/statuses/${SHA}?access_token=${GITHUB_TOKEN}" \
  -H "Content-Type: application/json" \
  -X POST \
  -d "{\"state\": \"${STATUS}\", \"context\": \"${CONTEXT}\", \"description\": \"${TEXT}\", \"target_url\": \"${LOG_URL}\"}"

# reference:
# https://gist.github.com/justincampbell/5066394
# https://developer.github.com/v3/repos/statuses/
