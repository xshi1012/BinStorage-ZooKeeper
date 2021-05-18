# BinStorage-ZooKeeper

## Build
This project is in Go module mode. Make sure that GO111MODULE is set to "on" or "auto".
```
export GO111MODULE="auto"
```
To build the project binaries, first set GOBIN environment variable under the root directory of the project with
```
export GOBIN=$(pwd)/bin
```
then use
```
go install ./...
```