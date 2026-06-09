#!/bin/bash

set -ex

PORT=8080
SERVER="http://localhost:$PORT"

export DOCS_BASEURL="$SERVER/"
rm -fr dist-doc ; sphinx-build -b html ./docs dist-doc
python -m http.server $PORT --directory dist-doc >/dev/null 2>&1 &
SERVER_PID=$!

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

./node_modules/.bin/pa11y-ci -s "$SERVER/sitemap.xml"

kill $SERVER_PID || true
