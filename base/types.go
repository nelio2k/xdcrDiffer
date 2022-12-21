package base

import (
	"crypto/sha512"
	"encoding/binary"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/gomemcached/client"
	xdcrBase "github.com/couchbase/goxdcr/base"
)

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
	uprEvent := &memcached.UprEvent{
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
	ret := make([]byte, GetFixedSizeMutationLen(keyLen, mut.ColFiltersMatched))
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
