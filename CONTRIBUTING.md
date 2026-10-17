# Contributing

Keep changes focused on the go2rtc fork and its Petlibro camera integration.
Inspect the implementation, tests, and [development guide](docs/development.md)
before changing protocol or streaming behavior.

## Checks

Run the same focused checks as CI:

```bash
go test ./pkg/petlibro ./cmd/petlibro-resolve -count=1
go test ./internal/streams -run '^Test(TimestampContinuity|StreamCorrelationIDs)' -count=1
go test ./pkg/rtsp -run '^Test(PacketWriter|PacketWriters|WriteInterleavedData)' -count=1
go test -race ./pkg/petlibro ./cmd/petlibro-resolve -count=1
go test -race ./internal/streams -run '^Test(TimestampContinuity|StreamCorrelationIDs)' -count=1
go test -race ./pkg/rtsp -run '^Test(PacketWriter|PacketWriters|WriteInterleavedData)' -count=1
go vet ./internal/streams ./pkg/petlibro ./pkg/rtsp ./cmd/petlibro-resolve
go build ./...
```

Run `gofmt` on changed Go files. Add focused synthetic regression tests for
parser, transport, lifecycle, timestamp, stream, or RTSP changes. Do not make
unrelated upstream refactors while changing the camera protocol.

## Protocol evidence and privacy

Do not commit packet captures, camera dumps, media, credentials, UIDs, local
addresses, or generated binaries. Keep raw evidence in an access-controlled
workspace and reduce durable findings to sanitized fixtures, tests, and concise
documentation. Debugging options that can capture decrypted traffic must remain
explicit and bounded.

Use the [debugging guide](docs/debugging.md) to prepare a useful report. State
which behavior came from deterministic tests and which was observed on
authorized hardware.

## Upstream changes

Treat `https://github.com/AlexxIT/go2rtc.git` as upstream. Merge reviewed
upstream revisions into the public `petlibro` branch; do not rebase published
fork releases. Record the old and new upstream commits and review changes in
`internal/streams`, `internal/webrtc`, `pkg/core`, `pkg/creds`, and `pkg/rtsp`
as well as the dedicated Petlibro packages.
