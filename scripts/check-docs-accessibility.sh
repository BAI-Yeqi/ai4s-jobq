#!/bin/bash

set -ex

PORT=8080
SERVER="http://localhost:$PORT"

# Use system Chrome (pre-installed on GitHub Actions runners) instead of
# downloading via Puppeteer, which fails due to npm allow-scripts blocking
# the postinstall and subsequent cache corruption.
if command -v google-chrome-stable &> /dev/null; then
    export PUPPETEER_EXECUTABLE_PATH=$(which google-chrome-stable)
else
    # Fallback: force fresh Puppeteer download
    rm -rf ~/.cache/puppeteer
    npx --yes puppeteer browsers install chrome
fi

export DOCS_BASEURL="$SERVER/"
rm -fr dist-doc ; sphinx-build -b html ./docs dist-doc
python -m http.server $PORT --directory dist-doc >/dev/null 2>&1 &
SERVER_PID=$!

# Ensure the server is stopped on any exit path.
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait for the server to start accepting connections before running pa11y,
# otherwise the sitemap fetch races the server startup and fails
# intermittently with "The sitemap ... could not be loaded".
for _ in $(seq 1 30); do
    if curl -sf "$SERVER/sitemap.xml" -o /dev/null; then
        break
    fi
    sleep 1
done

./node_modules/.bin/pa11y-ci -s "$SERVER/sitemap.xml"
