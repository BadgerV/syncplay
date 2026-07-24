#!/usr/bin/env bash
# Package the release binary into a macOS .app bundle.
#
# Core Audio process taps (--capture-tap) require the audio-capture TCC
# permission, which macOS only grants to a bundled, code-signed app that
# LaunchServices knows about — a bare CLI binary is denied *silently* (capture
# returns zeroed buffers rather than an error). Launch the built bundle with
# `open` so LaunchServices registers it and the permission prompt can appear.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP="${1:-$ROOT/dist/SyncPlay.app}"
BIN="$ROOT/target/release/syncplay"

[ -x "$BIN" ] || { echo "error: build first (cargo build --release)" >&2; exit 1; }

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS"
cp "$BIN" "$APP/Contents/MacOS/SyncPlay"

cat > "$APP/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key><string>SyncPlay</string>
  <key>CFBundleIdentifier</key><string>com.syncplay.app</string>
  <key>CFBundleName</key><string>SyncPlay</string>
  <key>CFBundleDisplayName</key><string>SyncPlay</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>0.1.0</string>
  <key>CFBundleVersion</key><string>1</string>
  <key>LSMinimumSystemVersion</key><string>14.4</string>
  <key>NSAudioCaptureUsageDescription</key>
  <string>SyncPlay captures this Mac's audio so it can be streamed to other Macs and played back in sync.</string>
  <key>NSMicrophoneUsageDescription</key>
  <string>SyncPlay captures system audio for synchronized playback across Macs.</string>
  <key>NSLocalNetworkUsageDescription</key>
  <string>SyncPlay discovers and streams to other Macs on your local network.</string>
</dict>
PLIST
echo '</plist>' >> "$APP/Contents/Info.plist"

# Keep entitlements OUTSIDE the bundle — a stray plist inside Contents/ is
# treated as an unsigned subcomponent and breaks signing.
ENTS="$(dirname "$APP")/entitlements.plist"
cat > "$ENTS" <<'ENT'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>com.apple.security.device.audio-input</key><true/>
  <key>com.apple.security.network.client</key><true/>
  <key>com.apple.security.network.server</key><true/>
</dict>
</plist>
ENT

# Ad-hoc sign with entitlements. A Developer ID identity is better (stable
# identity across rebuilds); set SYNCPLAY_SIGN_ID to use one.
SIGN_ID="${SYNCPLAY_SIGN_ID:--}"
codesign --force --options runtime \
  --entitlements "$ENTS" \
  --sign "$SIGN_ID" "$APP"

echo "Built: $APP"
echo "Run:   open -a \"$APP\" --args --sender --headless --capture-tap"
