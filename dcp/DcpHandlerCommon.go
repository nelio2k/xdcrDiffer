package dcp

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrParts "github.com/couchbase/goxdcr/base/filter"
	xdcrLog "github.com/couchbase/goxdcr/log"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
	"strings"
	"sync"
	"xdcrDiffer/base"
)

type DcpHandlerCommon struct {
	dcpClient               *DcpClient
	vbList                  []uint16
	dataChan                chan *Mutation
	incrementCounter        func()
	incrementSysCounter     func()
	colMigrationFilters     []string
	colMigrationFiltersOn   bool // shortcut to avoid len() check
	colMigrationFiltersImpl []xdcrParts.Filter
	utils                   xdcrUtils.UtilsIface
	isSource                bool
	filter                  xdcrParts.Filter
	logger                  *xdcrLog.CommonLogger
	finChan                 chan bool
	waitGrp                 sync.WaitGroup
	mutationProcessor       func(mut *Mutation)
}

func NewDcpHandlerCommon(dcpClient *DcpClient, vbList []uint16, dataChanSize int, incReceivedCounter func(),
	incSysEvtCounter func(), colMigrationFilters []string, utils xdcrUtils.UtilsIface) (*DcpHandlerCommon, error) {
	common := &DcpHandlerCommon{
		dcpClient:             dcpClient,
		vbList:                vbList,
		dataChan:              make(chan *Mutation, dataChanSize),
		incrementCounter:      incReceivedCounter,
		incrementSysCounter:   incSysEvtCounter,
		colMigrationFilters:   colMigrationFilters,
		colMigrationFiltersOn: len(colMigrationFilters) > 0,
		utils:                 utils,
		isSource:              strings.Contains(dcpClient.Name, base.SourceClusterName),
		filter:                dcpClient.dcpDriver.filter,
		logger:                dcpClient.logger,
		finChan:               make(chan bool),
	}

	if err := common.compileMigrCollectionFiltersIfNeeded(); err != nil {
		return nil, err
	}
	return common, nil
}

func (d *DcpHandlerCommon) SetMutationProcessor(processor func(mutation *Mutation)) {
	d.mutationProcessor = processor
}

func (d *DcpHandlerCommon) compileMigrCollectionFiltersIfNeeded() error {
	if len(d.colMigrationFilters) == 0 {
		return nil
	}

	for i, filterStr := range d.colMigrationFilters {
		filter, err := xdcrParts.NewFilter(fmt.Sprintf("%d", i), filterStr, d.utils, true)
		if err != nil {
			return fmt.Errorf("compiling %v resulted in: %v", filterStr, err)
		}
		d.colMigrationFiltersImpl = append(d.colMigrationFiltersImpl, filter)
	}
	return nil
}

func (d *DcpHandlerCommon) checkColMigrationFilters(mut *Mutation) []uint8 {
	var filterIdsMatched []uint8
	for i, filter := range d.colMigrationFiltersImpl {
		// If at least one passed, let it through
		matched, _, _, _ := filter.FilterUprEvent(mut.ToUprEvent())
		if matched {
			filterIdsMatched = append(filterIdsMatched, uint8(i))
		}
	}
	return filterIdsMatched
}

func (d *DcpHandlerCommon) replicationFilter(mut *Mutation, matched bool, filterResult base.FilterResultType) base.FilterResultType {
	var err error
	var errStr string
	if d.filter != nil && mut.IsMutation() {
		matched, err, errStr, _ = d.filter.FilterUprEvent(mut.ToUprEvent())
		if !matched {
			filterResult = base.Filtered
		}
		if err != nil {
			filterResult = base.UnableToFilter
			d.logger.Warnf("Err %v - (%v) when filtering mutation %v", err, errStr, mut)
		}
	}
	return filterResult
}

// Returns true if needs to skip processing
func (d *DcpHandlerCommon) preProcessMutation(mut *Mutation) bool {
	var matched bool
	var replicationFilterResult base.FilterResultType

	replicationFilterResult = d.replicationFilter(mut, matched, replicationFilterResult)
	valid := d.dcpClient.dcpDriver.checkpointManager.HandleMutationEvent(mut, replicationFilterResult)
	if !valid {
		// if mutation is out of range, ignore it
		return true
	}

	d.incrementCounter()

	// Ignore system events - we only care about actual data
	if mut.IsSystemEvent() {
		d.incrementSysCounter()
		return true
	}

	var filterIdsMatched []uint8
	if d.colMigrationFiltersOn && d.isSource {
		filterIdsMatched = d.checkColMigrationFilters(mut)
		if len(filterIdsMatched) == 0 {
			return true
		}
	}
	if d.colMigrationFiltersOn && len(filterIdsMatched) > 0 {
		mut.ColFiltersMatched = filterIdsMatched
	}
	return false
}

func (d *DcpHandlerCommon) Start() error {
	if d.mutationProcessor == nil {
		return fmt.Errorf("Mutation processor not set")
	}

	d.waitGrp.Add(1)
	go d.processData()
	return nil
}

func (d *DcpHandlerCommon) Stop() {
	close(d.finChan)
}

func (d *DcpHandlerCommon) processData() {
	defer d.waitGrp.Done()

	for {
		select {
		case <-d.finChan:
			goto done
		case mut := <-d.dataChan:
			skipProcessing := d.preProcessMutation(mut)
			if skipProcessing {
				continue
			}
			d.mutationProcessor(mut)
		}
	}
done:
}

func (d *DcpHandlerCommon) writeToDataChan(mut *Mutation) {
	select {
	case d.dataChan <- mut:
	// provides an alternative exit path when dh stops
	case <-d.finChan:
	}
}

type Mutation struct {
	Vbno              uint16
	Key               []byte
	Seqno             uint64
	RevId             uint64
	Cas               uint64
	Flags             uint32
	Expiry            uint32
	OpCode            gomemcached.CommandCode
	Value             []byte
	Datatype          uint8
	ColId             uint32
	ColFiltersMatched []uint8 // Given a ordered list of filters, this list contains indexes of the ordered list of filter that matched
}

func CreateMutation(vbno uint16, key []byte, seqno, revId, cas uint64, flags, expiry uint32, opCode gomemcached.CommandCode, value []byte, datatype uint8, collectionId uint32) *Mutation {
	return &Mutation{
		Vbno:     vbno,
		Key:      key,
		Seqno:    seqno,
		RevId:    revId,
		Cas:      cas,
		Flags:    flags,
		Expiry:   expiry,
		OpCode:   opCode,
		Value:    value,
		Datatype: datatype,
		ColId:    collectionId,
	}
}

func (m *Mutation) IsExpiration() bool {
	return m.OpCode == gomemcached.UPR_EXPIRATION
}

func (m *Mutation) IsDeletion() bool {
	return m.OpCode == gomemcached.UPR_DELETION
}

func (m *Mutation) IsMutation() bool {
	return m.OpCode == gomemcached.UPR_MUTATION
}

func (m *Mutation) IsSystemEvent() bool {
	return m.OpCode == gomemcached.DCP_SYSTEM_EVENT
}

func (m *Mutation) ToUprEvent() *xdcrBase.WrappedUprEvent {
	uprEvent := &mcc.UprEvent{
		Opcode:       m.OpCode,
		VBucket:      m.Vbno,
		DataType:     m.Datatype,
		Flags:        m.Flags,
		Expiry:       m.Expiry,
		Key:          m.Key,
		Value:        m.Value,
		Cas:          m.Cas,
		Seqno:        m.Seqno,
		CollectionId: m.ColId,
	}

	return &xdcrBase.WrappedUprEvent{
		UprEvent:     uprEvent,
		ColNamespace: nil,
		Flags:        0,
		ByteSliceGetter: func(size uint64) ([]byte, error) {
			return make([]byte, int(size)), nil
		},
	}
}

// serialize mutation into []byte
// format:
//
//	keyLen   - 2 bytes
//	Key  - length specified by keyLen
//	Seqno    - 8 bytes
//	RevId    - 8 bytes
//	Cas      - 8 bytes
//	Flags    - 4 bytes
//	Expiry   - 4 bytes
//	opType   - 2 byte
//	Datatype - 2 byte
//	hash     - 64 bytes
//	collectionId - 4 bytes
//	colFiltersLen - 1 byte (number of collection migration filters)
//	(per col filter) - 1 byte
func (mut *Mutation) Serialize() []byte {
	keyLen := len(mut.Key)
	ret := make([]byte, base.GetFixedSizeMutationLen(keyLen, mut.ColFiltersMatched))
	bodyHash := sha512.Sum512(mut.Value)

	pos := 0
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(keyLen))
	pos += 2
	copy(ret[pos:pos+keyLen], mut.Key)
	pos += keyLen
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.Seqno)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.RevId)
	pos += 8
	binary.BigEndian.PutUint64(ret[pos:pos+8], mut.Cas)
	pos += 8
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.Flags)
	pos += 4
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.Expiry)
	pos += 4
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(mut.OpCode))
	pos += 2
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(mut.Datatype))
	pos += 2
	copy(ret[pos:], bodyHash[:])
	pos += 64
	binary.BigEndian.PutUint32(ret[pos:pos+4], mut.ColId)
	pos += 4
	binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(len(mut.ColFiltersMatched)))
	pos += 2
	for _, colFilterId := range mut.ColFiltersMatched {
		binary.BigEndian.PutUint16(ret[pos:pos+2], uint16(colFilterId))
		pos += 2
	}
	return ret
}
