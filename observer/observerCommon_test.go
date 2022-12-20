package observer

import (
	"github.com/couchbase/goxdcr/log"
	"testing"
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
