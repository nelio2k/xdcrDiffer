package dcp

import (
	"github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gomemcached"
	"xdcrDiffer/base"
	"xdcrDiffer/observerHandler"
)

type ObserverEphDcpHandler struct {
	*DcpHandlerCommon
	handler observerHandler.ObserverHandler
}

func NewObserverEphDcpHandler(common *DcpHandlerCommon, handler observerHandler.ObserverHandler) (*ObserverEphDcpHandler, error) {
	return &ObserverEphDcpHandler{
		DcpHandlerCommon: common,
		handler:          handler,
	}, nil
}

func (o *ObserverEphDcpHandler) Start() error {
	o.DcpHandlerCommon.SetMutationProcessor(o.processMutation)
	return o.DcpHandlerCommon.Start()
}

func (o *ObserverEphDcpHandler) Stop() {
	o.DcpHandlerCommon.Stop()
}

func (o *ObserverEphDcpHandler) SnapshotMarker(startSeqno, endSeqno uint64, vbno uint16, streamID uint16, snapshotType gocbcore.SnapshotState) {
	o.dcpClient.dcpDriver.checkpointManager.updateSnapshot(vbno, startSeqno, endSeqno)
}

func (o *ObserverEphDcpHandler) Mutation(seqno, revId uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbno uint16, collectionID uint32, streamID uint16, key, value []byte) {
	o.writeToDataChan(base.CreateMutation(vbno, key, seqno, revId, cas, flags, expiry, gomemcached.UPR_MUTATION, value, datatype, collectionID))
}

func (o *ObserverEphDcpHandler) Deletion(seqno, revId uint64, deleteTime uint32, cas uint64, datatype uint8, vbno uint16, collectionID uint32, streamID uint16, key, value []byte) {
	o.writeToDataChan(base.CreateMutation(vbno, key, seqno, revId, cas, 0, 0, gomemcached.UPR_DELETION, value, datatype, collectionID))
}

func (o *ObserverEphDcpHandler) Expiration(seqno, revId uint64, deleteTime uint32, cas uint64, vbno uint16, collectionID uint32, streamID uint16, key []byte) {
	o.writeToDataChan(base.CreateMutation(vbno, key, seqno, revId, cas, 0, 0, gomemcached.UPR_EXPIRATION, nil, 0, collectionID))
}

func (o *ObserverEphDcpHandler) End(vbno uint16, streamID uint16, err error) {
	o.dcpClient.dcpDriver.handleVbucketCompletion(vbno, err, "dcp stream ended")
}

func (o *ObserverEphDcpHandler) CreateCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, ttl uint32, streamID uint16, key []byte) {
	o.writeToDataChan(base.CreateMutation(vbID, key, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (o *ObserverEphDcpHandler) DeleteCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, streamID uint16) {
	o.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (o *ObserverEphDcpHandler) FlushCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32) {
	// Don't care - not implemented anyway
}

func (o *ObserverEphDcpHandler) CreateScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16, key []byte) {
	// Overloading collectionID field for scopeID because differ doesn't care
	o.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, scopeID))
}

func (o *ObserverEphDcpHandler) DeleteScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16) {
	// Overloading collectionID field for scopeID because differ doesn't care
	o.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, scopeID))
}

func (o *ObserverEphDcpHandler) ModifyCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32, ttl uint32, streamID uint16) {
	// Overloading collectionID field for scopeID because differ doesn't care
	o.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (o *ObserverEphDcpHandler) OSOSnapshot(vbID uint16, snapshotType uint32, streamID uint16) {
	// Don't care
}

func (o *ObserverEphDcpHandler) SeqNoAdvanced(vbID uint16, bySeqno uint64, streamID uint16) {
	// Don't care
}

// END implements StreamObserver

func (o *ObserverEphDcpHandler) processMutation(mut *base.Mutation) {
	o.handler.HandleMutation(mut)
}
