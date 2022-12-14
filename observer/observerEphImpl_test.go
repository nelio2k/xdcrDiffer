package observer

import (
	"testing"
	"xdcrDiffer/base"
)

func TestObserverEphImpl_TranslateObserverKeysList(t *testing.T) {
	type fields struct {
		observeKeysMap    *ObserveKeysMap
		observeKeysGetter func() map[string]interface{}
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
				observeKeysMap: NewObserveKeysMap(),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{}
				},
			},
			wantErr: base.ErrorNoKeySpecified,
		},
		{
			name: "[NEG] ObserverKeysList translate incorrect format",
			fields: fields{
				observeKeysMap: NewObserveKeysMap(),
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
				observeKeysMap: NewObserveKeysMap(),
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
				observeKeysMap: NewObserveKeysMap(),
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
				observeKeysMap: NewObserveKeysMap(),
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
				observeKeysMap: NewObserveKeysMap(),
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
			name: "[POS] ObserverKeysList translate empty keys",
			fields: fields{
				observeKeysMap: NewObserveKeysMap(),
				observeKeysGetter: func() map[string]interface{} {
					return map[string]interface{}{
						"_default": map[string][]string{
							"_default": []string{"a", "b"},
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &ObserveCommon{
				observeKeysMap:    tt.fields.observeKeysMap,
				observeKeysGetter: tt.fields.observeKeysGetter,
			}
			if err := o.TranslateObserverKeysList(); err != tt.wantErr {
				t.Errorf("TranslateObserverKeysList() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantObserveKeysMap != nil {
				for cid, innerMap := range tt.wantObserveKeysMap {
					_, ok := tt.fields.observeKeysMap.keysMap[cid]
					if !ok {
						t.Errorf("CID %v is not there", cid)
					}
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
