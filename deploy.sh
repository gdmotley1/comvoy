#!/bin/bash
# Deploy script — works around Vercel's git author issue by deploying from a clean copy
set -e

echo "=== Deploying backend to Vercel ==="
TMPDIR=$(mktemp -d)
cp -r app api requirements.txt vercel.json "$TMPDIR/"
cp -r .vercel "$TMPDIR/" 2>/dev/null || true
(cd "$TMPDIR" && vercel deploy --prod --yes)
rm -rf "$TMPDIR"

echo ""
echo "=== Deploying frontend to Netlify ==="
netlify deploy --prod --dir=static

echo ""
echo "=== Done ==="
