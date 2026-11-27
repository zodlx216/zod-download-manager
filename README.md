# ZOD Download Manager 0.3 (Go)

Icon and version resource: `llvm-windres --target=pe-x86-64 zdm.rc rsrc_windows_amd64.syso`

Build: `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-H=windowsgui" -o ZDM.exe .`
Test: `go test ./...`

Settings and history: `%APPDATA%\ZDM`. No third-party packages.
The window is a local page (127.0.0.1, secret token) shown in Edge or Chrome app mode.
