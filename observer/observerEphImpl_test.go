package observer

import (
	"fmt"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"testing"
	"xdcrDiffer/base"
)

var defaultManifest = metadata.NewDefaultCollectionsManifest()

var newObserveKeysMapWrapper = func(logger *log.CommonLogger, srcToTgtColIds map[uint32][]uint32) *ObserveKeysMap {
	keysMap, err := NewObserveKeysMap(logger, srcToTgtColIds)
	if err != nil {
		panic(fmt.Sprintf("%v is not expected", err))
	}
	return keysMap
}

func TestObserverEphImpl_TranslateObserverKeysList(t *testing.T) {
	type fields struct {
		observeKeysMap    *ObserveKeysMap
		observeKeysGetter func() map[string]interface{}
		manifestPair      *metadata.CollectionsManifestPair
	}
	tests := []struct {
		name               string
		fields             fields
		wantErr            error
		wantObserveKeysMap map[uint32]map[string]bool
	}{
		{
			name: "[NEG] ObserverKeysList translate empty list",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{}
				},
			},
			wantErr: base.ErrorNoKeySpecified,
		},
		{
			name: "[NEG] ObserverKeysList translate incorrect format",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": nil,
					}
				},
			},
			wantErr: base.ErrorInvalidObserveKeysFormat,
		},
		{
			name: "[NEG] ObserverKeysList translate incorrect format2",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": []string{},
					}
				},
			},
			wantErr: base.ErrorInvalidObserveKeysFormat,
		},
		{
			name: "[NEG] ObserverKeysList translate incorrect format2",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": map[string][]string{
							"_default": nil,
						},
					}
				},
			},
			wantErr: base.ErrorInvalidObserveKeysFormat,
		},
		{
			name: "[NEG] ObserverKeysList translate empty keys",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": map[string][]string{
							"_default": []string{"", "a"},
						},
					}
				},
			},
			wantErr: base.ErrorInvalidObserveKeysFormat,
		},
		{
			name: "[NEG] ObserverKeysList translate empty keys",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": map[string][]string{
							"_default": []string{"", "a"},
						},
					}
				},
			},
			wantErr: base.ErrorInvalidObserveKeysFormat,
		},
		{
			name: "[POS] ObserverKeysList translate valid keys default collection",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": map[string]interface{}{
							"_default": []interface{}{
								"a",
								"b",
							},
						},
					}
				},
			},
			wantErr: nil,
			wantObserveKeysMap: map[uint32]map[string]bool{
				0: map[string]bool{
					"a": true,
					"b": true,
				},
			},
		},
		{
			name: "[NEG] ObserverKeysList translate valid keys non-default collection no manifest provided",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": map[string]interface{}{
							"nonDefault": []interface{}{"a", "b"},
						},
					}
				},
			},
			wantErr: base.ErrorNoManifestProvided,
		},
		{
			name: "[NEG] ObserverKeysList translate valid keys non-default collection manifest provided doesn't match",
			fields: fields{
				observeKeysMap: newObserveKeysMapWrapper(nil, nil),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": map[string]interface{}{
							"nonDefault": []interface{}{"a", "b"},
						},
					}
				},
				manifestPair: &metadata.CollectionsManifestPair{
					Source: &defaultManifest,
					Target: &defaultManifest,
				},
			},
			wantErr: base.ErrorScopeCollectionNotFoundInManifest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &ObserveCommon{
				observeKeysMap:    tt.fields.observeKeysMap,
				observeKeysGetter: tt.fields.observeKeysGetter,
				manifestsPair:     tt.fields.manifestPair,
			}
			if err := o.TranslateObserverKeysList(); err != tt.wantErr {
				t.Errorf("TranslateObserverKeysList() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantObserveKeysMap != nil {
				for cid, innerMap := range tt.wantObserveKeysMap {
					wantInnerMap := tt.wantObserveKeysMap[cid]
					for k, _ := range wantInnerMap {
						if _, ok := innerMap[k]; !ok {
							t.Errorf("Key %v is not found", k)
						}
					}
				}
			}
		})
	}
}
