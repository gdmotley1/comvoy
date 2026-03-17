#!/bin/bash
# Deploy script — works around Vercel's git author issue by deploying from a clean copy
set -e

echo "=== Deploying backend to Vercel ==="
TMPDIR=$(mktemp -d)
cp -r app api requirements.txt vercel.json "$TMPDIR/"
cp -r .vercel "$TMPDIR/" 2>/dev/null || true
(cd "$TMPDIR" && vercel deploy --prod --yes)
rm -rf "$TMPDIR" 2>/dev/null || true

echo ""
echo "=== Pushing to GitHub (triggers GitHub Pages deploy) ==="
git push origin master

echo ""
echo "=== Done ==="
