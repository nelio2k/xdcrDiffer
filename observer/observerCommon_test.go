package observer

import (
	xdcrBase "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
	"testing"
	"xdcrDiffer/base"
)

func TestNewObserveKeysMap(t *testing.T) {
	type args struct {
		logger         *log.CommonLogger
		srcToTgtColIds map[uint32][]uint32
	}
	tests := []struct {
		name    string
		args    args
		want    *ObserveKeysMap
		wantErr bool
	}{
		{
			name: "[NEG] N to 1 mapping",
			args: args{
				logger: log.NewLogger("unit test", nil),
				srcToTgtColIds: map[uint32][]uint32{
					0: []uint32{1},
					1: []uint32{1},
				},
			},
			wantErr: true,
		},
		{
			name: "[POS] 1 to N mapping",
			args: args{
				logger: log.NewLogger("unit test", nil),
				srcToTgtColIds: map[uint32][]uint32{
					0: []uint32{1, 2},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewObserveKeysMap(tt.args.logger, tt.args.srcToTgtColIds)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewObserveKeysMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestObserveKeysMap_MutationIsObserved(t *testing.T) {
	type fields struct {
		srcKeysMap      map[uint32]KeysLookupMap
		tgtKeysMap      map[uint32]KeysLookupMap
		srcNamespaceMap map[uint32]*xdcrBase.CollectionNamespace
		tgtNamespaceMap map[uint32]*xdcrBase.CollectionNamespace
		manifestsPair   *metadata.CollectionsManifestPair
		mtx             sync.RWMutex
		srcToTgtColIds  map[uint32][]uint32
		logger          *log.CommonLogger
	}
	type args struct {
		mut      *base.Mutation
		isSource bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &ObserveKeysMap{
				srcKeysMap:      tt.fields.srcKeysMap,
				tgtKeysMap:      tt.fields.tgtKeysMap,
				srcNamespaceMap: tt.fields.srcNamespaceMap,
				tgtNamespaceMap: tt.fields.tgtNamespaceMap,
				manifestsPair:   tt.fields.manifestsPair,
				mtx:             tt.fields.mtx,
				srcToTgtColIds:  tt.fields.srcToTgtColIds,
				logger:          tt.fields.logger,
			}
			if got := m.MutationIsObserved(tt.args.mut, tt.args.isSource); got != tt.want {
				t.Errorf("MutationIsObserved() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestObserveKeysMap_MutationIsObserved1(t *testing.T) {
	type fields struct {
		srcKeysMap      map[uint32]KeysLookupMap
		tgtKeysMap      map[uint32]KeysLookupMap
		srcNamespaceMap map[uint32]*xdcrBase.CollectionNamespace
		tgtNamespaceMap map[uint32]*xdcrBase.CollectionNamespace
		manifestsPair   *metadata.CollectionsManifestPair
		mtx             sync.RWMutex
		srcToTgtColIds  map[uint32][]uint32
		logger          *log.CommonLogger
	}
	type args struct {
		mut      *base.Mutation
		isSource bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "[POS] source in the list",
			want: true,
			fields: fields{
				srcKeysMap: map[uint32]KeysLookupMap{
					0: {"a": &KeysHistoryEphemeral{}},
				},
				tgtKeysMap: map[uint32]KeysLookupMap{
					0: {"a": &KeysHistoryEphemeral{}},
				},
				srcNamespaceMap: nil,
				tgtNamespaceMap: nil,
				manifestsPair:   nil,
				mtx:             sync.RWMutex{},
				srcToTgtColIds:  nil,
				logger:          nil,
			},
			args: args{
				mut: &base.Mutation{
					Vbno:              0,
					Key:               []byte("a"),
					Seqno:             0,
					RevId:             0,
					Cas:               0,
					Flags:             0,
					Expiry:            0,
					OpCode:            0,
					Value:             nil,
					Datatype:          0,
					ColId:             0,
					ColFiltersMatched: nil,
				},
				isSource: true,
			},
		},
		{
			name: "[POS] target in the list",
			want: true,
			fields: fields{
				srcKeysMap: map[uint32]KeysLookupMap{
					0: {"a": &KeysHistoryEphemeral{}},
				},
				tgtKeysMap: map[uint32]KeysLookupMap{
					0: {"a": &KeysHistoryEphemeral{}},
				},
				srcNamespaceMap: nil,
				tgtNamespaceMap: nil,
				manifestsPair:   nil,
				mtx:             sync.RWMutex{},
				srcToTgtColIds:  nil,
				logger:          nil,
			},
			args: args{
				mut: &base.Mutation{
					Vbno:              0,
					Key:               []byte("a"),
					Seqno:             0,
					RevId:             0,
					Cas:               0,
					Flags:             0,
					Expiry:            0,
					OpCode:            0,
					Value:             nil,
					Datatype:          0,
					ColId:             0,
					ColFiltersMatched: nil,
				},
				isSource: false,
			},
		},
		{
			name: "[NEG] target NOT in the list",
			want: false,
			fields: fields{
				srcKeysMap: map[uint32]KeysLookupMap{
					0: {"a": &KeysHistoryEphemeral{}},
				},
				tgtKeysMap: map[uint32]KeysLookupMap{
					0: {"a": &KeysHistoryEphemeral{}},
				},
				srcNamespaceMap: nil,
				tgtNamespaceMap: nil,
				manifestsPair:   nil,
				mtx:             sync.RWMutex{},
				srcToTgtColIds:  nil,
				logger:          nil,
			},
			args: args{
				mut: &base.Mutation{
					Vbno:              0,
					Key:               []byte("b"),
					Seqno:             0,
					RevId:             0,
					Cas:               0,
					Flags:             0,
					Expiry:            0,
					OpCode:            0,
					Value:             nil,
					Datatype:          0,
					ColId:             0,
					ColFiltersMatched: nil,
				},
				isSource: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &ObserveKeysMap{
				srcKeysMap:      tt.fields.srcKeysMap,
				tgtKeysMap:      tt.fields.tgtKeysMap,
				srcNamespaceMap: tt.fields.srcNamespaceMap,
				tgtNamespaceMap: tt.fields.tgtNamespaceMap,
				manifestsPair:   tt.fields.manifestsPair,
				mtx:             tt.fields.mtx,
				srcToTgtColIds:  tt.fields.srcToTgtColIds,
				logger:          tt.fields.logger,
			}
			if got := m.MutationIsObserved(tt.args.mut, tt.args.isSource); got != tt.want {
				t.Errorf("MutationIsObserved() = %v, want %v", got, tt.want)
			}
		})
	}
}
