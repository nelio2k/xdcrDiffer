package dcp

import "github.com/couchbase/gocbcore/v9"

type DcpHandler interface {
	gocbcore.StreamObserver
	Start() error
	Stop()
}
