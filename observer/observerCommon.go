package observer

import (
	"fmt"
	xdcrBase "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"reflect"
	"sync"
	"xdcrDiffer/base"
)

type ObserveKeysMap struct {
	keysMap      map[uint32]map[string]bool               // Use map of map to dedup keys and for faster lookup
	namespaceMap map[uint32]*xdcrBase.CollectionNamespace // for lookup
	manifest     *metadata.CollectionsManifest

	mtx sync.RWMutex
}

func (m *ObserveKeysMap) SetManifest(manifest *metadata.CollectionsManifest) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.manifest = manifest
}

func (m *ObserveKeysMap) Add(ns *xdcrBase.CollectionNamespace, key string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.manifest == nil {
		return fmt.Errorf("Manifest has not been set yet")
	}

	cid, err := m.manifest.GetCollectionId(ns.ScopeName, ns.CollectionName)
	if err != nil {
		return err
	}

	if _, exists := m.keysMap[cid]; !exists {
		m.keysMap[cid] = make(map[string]bool)
	}

	m.keysMap[cid][key] = true
	m.namespaceMap[cid] = ns
	return nil
}

func NewObserveKeysMap() *ObserveKeysMap {
	return &ObserveKeysMap{
		keysMap:      make(map[uint32]map[string]bool),
		manifest:     nil,
		namespaceMap: map[uint32]*xdcrBase.CollectionNamespace{},
		mtx:          sync.RWMutex{},
	}
}

type ObserveCommon struct {
	observeKeysMap *ObserveKeysMap
	logger         *log.CommonLogger

	observeKeysGetter func() map[string]interface{}
}

func NewObserveCommon(logger *log.CommonLogger, observeKeysGetter func() map[string]interface{}) *ObserveCommon {
	return &ObserveCommon{
		observeKeysMap:    NewObserveKeysMap(),
		logger:            logger,
		observeKeysGetter: observeKeysGetter,
	}
}

// Given the manifest, translate the list of keys that needs to be observed into something that is
// based off of collection IDs
func (o *ObserveCommon) TranslateObserverKeysList() error {
	scopesCollectionMap := o.observeKeysGetter()
	if scopesCollectionMap == nil {
		return fmt.Errorf("Unable to retrieve list of keys to observe based on %v", base.ObserveKeysKey)
	}

	if len(scopesCollectionMap) == 0 {
		return base.ErrorNoKeySpecified
	}

	if err := o.checkFormat(scopesCollectionMap); err != nil {
		return err
	}

	// Only default scope exists
	if len(scopesCollectionMap) == 1 {
		if scopesCollectionMap[xdcrBase.DefaultScopeCollectionName] == nil {
			return base.ErrorInvalidObserveKeysFormat
		}
		collectionsKeysMap, ok := scopesCollectionMap[xdcrBase.DefaultScopeCollectionName].(map[string][]string)
		if !ok {
			return base.ErrorInvalidObserveKeysFormat
		}
		if len(collectionsKeysMap) == 0 || collectionsKeysMap == nil {
			return base.ErrorInvalidObserveKeysFormat
		}
		// Only default collection exists within a single default scope
		if len(collectionsKeysMap) == 1 && collectionsKeysMap[xdcrBase.DefaultScopeCollectionName] != nil {
			keysList := collectionsKeysMap[xdcrBase.DefaultScopeCollectionName]
			if len(keysList) == 0 {
				return base.ErrorNoKeySpecified
			}
			// Translate everything to collection ID 0
			defaultManifest := metadata.NewDefaultCollectionsManifest()
			o.observeKeysMap.SetManifest(&defaultManifest)
			collectionNs := &xdcrBase.DefaultCollectionNamespace
			for _, key := range keysList {
				if err := o.observeKeysMap.Add(collectionNs, key); err != nil {
					return fmt.Errorf("Adding key %v to %v resulted in %v", key, collectionNs.ToIndexString(), err)
				}
			}
			// All done
			return nil
		}
	}

	return nil
}

func (o *ObserveCommon) checkFormat(scopesCollectionsMap map[string]interface{}) error {
	//"observeKeys": {
	//	"_default": {
	//		"_default": []
	//   }
	//}

	for k, v := range scopesCollectionsMap {
		if v == nil {
			o.logger.Errorf("Scope %s contains nil value", k)
			return base.ErrorInvalidObserveKeysFormat
		}
		collectionsKeysMap, ok := v.(map[string][]string)
		if !ok {
			o.logger.Errorf("Scope %s contains invalid type: %v", k, reflect.TypeOf(v))
			return base.ErrorInvalidObserveKeysFormat
		}
		if len(collectionsKeysMap) == 0 || collectionsKeysMap == nil {
			o.logger.Errorf("Scope %s does not contain any valid value listing collection names and keys", k)
			return base.ErrorInvalidObserveKeysFormat
		}
		for col, keysList := range collectionsKeysMap {
			if len(keysList) == 0 {
				o.logger.Errorf("Scope %v collection %v contains no keys", k, col)
				return base.ErrorInvalidObserveKeysFormat
			}
			for _, key := range keysList {
				if key == "" {
					o.logger.Errorf("Scope %v collection %v contains an empty key", k, col)
					return base.ErrorInvalidObserveKeysFormat
				}
			}
		}
	}
	return nil
}
