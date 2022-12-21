// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package dcp

import (
	"fmt"
	gocbcore "github.com/couchbase/gocbcore/v9"
	"github.com/couchbase/gomemcached"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"os"
	"xdcrDiffer/base"
	fdp "xdcrDiffer/fileDescriptorPool"
	"xdcrDiffer/utils"
)

type DifferDcpHandler struct {
	*DcpHandlerCommon

	fileDir      string
	index        int
	numberOfBins int
	bucketMap    map[uint16]map[int]*Bucket
	fdPool       fdp.FdPoolIface
	bufferCap    int
}

func NewDifferDcpHandler(fileDir string, index, numberOfBins int, fdPool fdp.FdPoolIface, bufferCap int, common *DcpHandlerCommon) (*DifferDcpHandler, error) {
	return &DifferDcpHandler{
		DcpHandlerCommon: common,
		fileDir:          fileDir,
		index:            index,
		numberOfBins:     numberOfBins,
		bucketMap:        make(map[uint16]map[int]*Bucket),
		fdPool:           fdPool,
		bufferCap:        bufferCap,
	}, nil
}

// implements StreamObserver
func (dh *DifferDcpHandler) SnapshotMarker(startSeqno, endSeqno uint64, vbno uint16, streamID uint16, snapshotType gocbcore.SnapshotState) {
	dh.dcpClient.dcpDriver.checkpointManager.updateSnapshot(vbno, startSeqno, endSeqno)
}

func (dh *DifferDcpHandler) Mutation(seqno, revId uint64, flags, expiry, lockTime uint32, cas uint64, datatype uint8, vbno uint16, collectionID uint32, streamID uint16, key, value []byte) {
	dh.writeToDataChan(base.CreateMutation(vbno, key, seqno, revId, cas, flags, expiry, gomemcached.UPR_MUTATION, value, datatype, collectionID))
}

func (dh *DifferDcpHandler) Deletion(seqno, revId uint64, deleteTime uint32, cas uint64, datatype uint8, vbno uint16, collectionID uint32, streamID uint16, key, value []byte) {
	dh.writeToDataChan(base.CreateMutation(vbno, key, seqno, revId, cas, 0, 0, gomemcached.UPR_DELETION, value, datatype, collectionID))
}

func (dh *DifferDcpHandler) Expiration(seqno, revId uint64, deleteTime uint32, cas uint64, vbno uint16, collectionID uint32, streamID uint16, key []byte) {
	dh.writeToDataChan(base.CreateMutation(vbno, key, seqno, revId, cas, 0, 0, gomemcached.UPR_EXPIRATION, nil, 0, collectionID))
}

func (dh *DifferDcpHandler) End(vbno uint16, streamID uint16, err error) {
	dh.dcpClient.dcpDriver.handleVbucketCompletion(vbno, err, "dcp stream ended")
}

func (dh *DifferDcpHandler) CreateCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, ttl uint32, streamID uint16, key []byte) {
	dh.writeToDataChan(base.CreateMutation(vbID, key, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (dh *DifferDcpHandler) DeleteCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, collectionID uint32, streamID uint16) {
	dh.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (dh *DifferDcpHandler) FlushCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32) {
	// Don't care - not implemented anyway
}

func (dh *DifferDcpHandler) CreateScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16, key []byte) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, scopeID))
}

func (dh *DifferDcpHandler) DeleteScope(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, scopeID uint32, streamID uint16) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, scopeID))
}

func (dh *DifferDcpHandler) ModifyCollection(seqNo uint64, version uint8, vbID uint16, manifestUID uint64, collectionID uint32, ttl uint32, streamID uint16) {
	// Overloading collectionID field for scopeID because differ doesn't care
	dh.writeToDataChan(base.CreateMutation(vbID, nil, seqNo, 0, 0, 0, 0, gomemcached.DCP_SYSTEM_EVENT, nil, 0, collectionID))
}

func (dh *DifferDcpHandler) OSOSnapshot(vbID uint16, snapshotType uint32, streamID uint16) {
	// Don't care
}

func (dh *DifferDcpHandler) SeqNoAdvanced(vbID uint16, bySeqno uint64, streamID uint16) {
	// Don't care
}

// END implements StreamObserver

func (dh *DifferDcpHandler) Start() error {
	err := dh.initialize()
	if err != nil {
		return err
	}

	dh.DcpHandlerCommon.SetMutationProcessor(dh.processMutation)
	return dh.DcpHandlerCommon.Start()
}

func (dh *DifferDcpHandler) Stop() {
	dh.DcpHandlerCommon.Stop()
	dh.cleanup()
}

func (dh *DifferDcpHandler) initialize() error {
	for _, vbno := range dh.vbList {
		innerMap := make(map[int]*Bucket)
		dh.bucketMap[vbno] = innerMap
		for i := 0; i < dh.numberOfBins; i++ {
			bucket, err := NewBucket(dh.fileDir, vbno, i, dh.fdPool, dh.logger, dh.bufferCap)
			if err != nil {
				return err
			}
			innerMap[i] = bucket
		}
	}

	return nil
}

func (dh *DifferDcpHandler) cleanup() {
	for _, vbno := range dh.vbList {
		innerMap := dh.bucketMap[vbno]
		if innerMap == nil {
			dh.logger.Warnf("Cannot find innerMap for Vbno %v at cleanup\n", vbno)
			continue
		}
		for i := 0; i < dh.numberOfBins; i++ {
			bucket := innerMap[i]
			if bucket == nil {
				dh.logger.Warnf("Cannot find bucket for Vbno %v and index %v at cleanup\n", vbno, i)
				continue
			}
			//fmt.Printf("%v DifferDcpHandler closing bucket %v\n", dh.dcpClient.Name, i)
			bucket.close()
		}
	}
}

func (dh *DifferDcpHandler) processMutation(mut *base.Mutation) {
	vbno := mut.Vbno
	index := utils.GetBucketIndexFromKey(mut.Key, dh.numberOfBins)
	innerMap := dh.bucketMap[vbno]
	if innerMap == nil {
		panic(fmt.Sprintf("cannot find bucketMap for Vbno %v", vbno))
	}
	bucket := innerMap[index]
	if bucket == nil {
		panic(fmt.Sprintf("cannot find bucket for index %v", index))
	}

	bucket.write(mut.Serialize())
}

type Bucket struct {
	data []byte
	// current index in data for next write
	index    int
	file     *os.File
	fileName string

	fdPoolCb fdp.FileOp
	closeOp  func() error

	logger *xdcrLog.CommonLogger

	bufferCap int
}

func NewBucket(fileDir string, vbno uint16, bucketIndex int, fdPool fdp.FdPoolIface, logger *xdcrLog.CommonLogger, bufferCap int) (*Bucket, error) {
	fileName := utils.GetFileName(fileDir, vbno, bucketIndex)
	var cb fdp.FileOp
	var closeOp func() error
	var err error
	var file *os.File

	if fdPool == nil {
		file, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, base.FileModeReadWrite)
		if err != nil {
			return nil, err
		}
	} else {
		_, cb, err = fdPool.RegisterFileHandle(fileName)
		if err != nil {
			return nil, err
		}
		closeOp = func() error {
			return fdPool.DeRegisterFileHandle(fileName)
		}
	}
	return &Bucket{
		data:      make([]byte, bufferCap),
		index:     0,
		file:      file,
		fileName:  fileName,
		fdPoolCb:  cb,
		closeOp:   closeOp,
		logger:    logger,
		bufferCap: bufferCap,
	}, nil
}

func (b *Bucket) write(item []byte) error {
	if b.index+len(item) > b.bufferCap {
		err := b.flushToFile()
		if err != nil {
			return err
		}
	}

	copy(b.data[b.index:], item)
	b.index += len(item)
	return nil
}

func (b *Bucket) flushToFile() error {
	var numOfBytes int
	var err error

	if b.fdPoolCb != nil {
		numOfBytes, err = b.fdPoolCb(b.data[:b.index])
	} else {
		numOfBytes, err = b.file.Write(b.data[:b.index])
	}
	if err != nil {
		return err
	}
	if numOfBytes != b.index {
		return fmt.Errorf("Incomplete write. expected=%v, actual=%v", b.index, numOfBytes)
	}
	b.index = 0
	return nil
}

func (b *Bucket) close() {
	err := b.flushToFile()
	if err != nil {
		b.logger.Errorf("Error flushing to file %v at bucket close err=%v\n", b.fileName, err)
	}
	if b.fdPoolCb != nil {
		err = b.closeOp()
		if err != nil {
			b.logger.Errorf("Error closing file %v.  err=%v\n", b.fileName, err)
		}
	} else {
		err = b.file.Close()
		if err != nil {
			b.logger.Errorf("Error closing file %v.  err=%v\n", b.fileName, err)
		}
	}
}
