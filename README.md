# SyncPlay

Stream system audio from one Mac to one or more Macs on the same LAN, with
synchronized playback. Native macOS app written in Rust (egui GUI, CoreAudio via
[cpal], mDNS/Bonjour discovery, UDP transport, PI-controlled resampling for
drift correction).

## How it works

```
┌─────────── Sender Mac ───────────┐          ┌────────── Receiver Mac(s) ─────────┐
│ CoreAudio capture (cpal)         │  UDP     │ recv loop → jitter buffer          │
│   → i16 stereo packets           │ ───────► │   → linear resampler (sync ratio)  │
│   → UDP unicast to subscribers   │  audio   │   → CoreAudio playback (cpal)      │
│ mDNS: advertises _syncplay._udp  │ ◄─────── │ mDNS: browses + subscribes         │
└──────────────────────────────────┘  control └────────────────────────────────────┘
```

A PI controller on each receiver watches its jitter-buffer fill level and nudges
the playback resample ratio by ±0.2% to hold latency steady and absorb clock
drift between machines.

## Capturing system audio

macOS has no built-in loopback device, so to stream *system* output (rather than
a microphone) you need a virtual loopback device such as [BlackHole]:

1. Install BlackHole (2ch): `brew install blackhole-2ch`
2. Create a **Multi-Output Device** in *Audio MIDI Setup* combining your speakers
   + BlackHole, and set it as the system output (so you still hear audio locally).
3. In SyncPlay's **Sender** tab, pick **BlackHole 2ch** as the input device.

To stream a microphone or line-in instead, just select that device directly.

## Build & run

```bash
cargo build --release
./target/release/syncplay            # starts in Receiver mode
./target/release/syncplay --sender   # starts in Sender mode
```

You can also switch modes live from the top bar. CLI flags:

| Flag | Description |
|------|-------------|
| `--sender` | Start in sender mode (default: receiver) |
| `--input-device <name>` | Preselect a capture device (partial match) |
| `--output-device <name>` | Preselect an output device (partial match) |

Logging verbosity is controlled with `RUST_LOG`, e.g. `RUST_LOG=syncplay=debug`.

## Usage

**On the sending Mac:** open the Sender tab, choose the input device, click
**Start Streaming**. It advertises itself over Bonjour.

**On each receiving Mac:** open the Receiver tab, wait for the sender to appear
under *Discovered Senders*, select it, click **Connect**. Adjust the buffer-delay
slider (higher = more latency but more robust to packet loss) and volume.

## Networking

- Audio: UDP port **12345**  ·  Control: UDP port **12346**
- Discovery: mDNS service type `_syncplay._udp.local.`

Make sure both Macs are on the same subnet and that the local firewall allows
`syncplay` to receive incoming connections (System Settings → Network → Firewall).

## Project layout

```
src/
├── main.rs            # CLI + GUI bootstrap, mDNS browser
├── engine.rs          # spawns/owns the audio+network pipelines per mode
├── audio/             # cpal capture, playback + resampler, device enumeration
├── net/               # UDP sender/receiver sessions, wire protocol, discovery
├── sync/controller.rs # PI buffer-fill controller
├── state/shared.rs    # shared app state, jitter buffer, constants
└── ui/app.rs          # egui interface
```

[cpal]: https://github.com/RustAudio/cpal
[BlackHole]: https://github.com/ExistentialAudio/BlackHole
