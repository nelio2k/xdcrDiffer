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

type KeysHistory struct {
	// TODO - add more history as part of listener
}

func NewKeysHistory() KeysHistory {
	return KeysHistory{}
}

type KeysLookupMap map[string]KeysHistory

func (k *KeysLookupMap) AddNewKey(key string) error {
	if _, ok := (*k)[key]; !ok {
		(*k)[key] = NewKeysHistory()
		return nil
	}

	return base.ErrorKeyAlreadyExists
}

type ObserveKeysMap struct {
	srcKeysMap      map[uint32]KeysLookupMap
	tgtKeysMap      map[uint32]KeysLookupMap
	srcNamespaceMap map[uint32]*xdcrBase.CollectionNamespace // for lookup
	tgtNamespaceMap map[uint32]*xdcrBase.CollectionNamespace // for lookup
	manifestsPair   *metadata.CollectionsManifestPair
	mtx             sync.RWMutex

	// Not protected
	srcToTgtColIds map[uint32][]uint32

	logger *log.CommonLogger
}

func (m *ObserveKeysMap) SetManifestsPair(manifestPair *metadata.CollectionsManifestPair) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.manifestsPair = manifestPair
}

func (m *ObserveKeysMap) Add(ns *xdcrBase.CollectionNamespace, key string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.manifestsPair == nil || m.manifestsPair.Source == nil || m.manifestsPair.Target == nil {
		return base.ErrorNoManifestProvided
	}

	scid, err := m.manifestsPair.Source.GetCollectionId(ns.ScopeName, ns.CollectionName)
	if err != nil {
		return err
	}

	if _, ok := m.srcToTgtColIds[scid]; !ok {
		m.logger.Errorf("Unable to find target cid given source cid %v namespace %v", scid, ns.ToIndexString())
		return base.ErrorUnableToMap
	}
	for _, tcid := range m.srcToTgtColIds[scid] {
		if _, _, err := m.manifestsPair.Target.GetScopeAndCollectionName(tcid); err != nil {
			m.logger.Errorf("Unable to find target collection ID in manifest version %v", tcid, m.manifestsPair.Target.Uid())
			return base.ErrorUnableToMap
		}
	}

	lookupMap, ok := m.srcKeysMap[scid]
	if !ok {
		m.srcKeysMap[scid] = make(KeysLookupMap)
		lookupMap = m.srcKeysMap[scid]
	}
	if err = lookupMap.AddNewKey(key); err != nil {
		return err
	}

	for _, tcid := range m.srcToTgtColIds[scid] {
		lookupMap, ok = m.tgtKeysMap[tcid]
		if !ok {
			lookupMap = make(KeysLookupMap)
			m.tgtKeysMap[tcid] = lookupMap
		}
		if err = lookupMap.AddNewKey(key); err != nil {
			return err
		}
	}
	return nil
}

func NewObserveKeysMap(logger *log.CommonLogger, srcToTgtColIds map[uint32][]uint32) (*ObserveKeysMap, error) {
	// For now, unable to handle 2->1 implementation - check now
	dedupCheckMap := make(map[uint32]bool)
	for _, tgtColIds := range srcToTgtColIds {
		for _, tcid := range tgtColIds {
			if _, ok := dedupCheckMap[tcid]; ok {
				logger.Errorf("Target collection ID %v is mapped to more than once", tcid)
				return nil, base.ErrorUnableToMap
			} else {
				dedupCheckMap[tcid] = true
			}
		}
	}

	if srcToTgtColIds == nil {
		// Default to default mapping
		srcToTgtColIds = make(map[uint32][]uint32)
		srcToTgtColIds[0] = []uint32{0}
	}

	return &ObserveKeysMap{
		srcKeysMap:      map[uint32]KeysLookupMap{},
		tgtKeysMap:      map[uint32]KeysLookupMap{},
		srcNamespaceMap: map[uint32]*xdcrBase.CollectionNamespace{},
		tgtNamespaceMap: map[uint32]*xdcrBase.CollectionNamespace{},
		mtx:             sync.RWMutex{},
		srcToTgtColIds:  srcToTgtColIds,
		logger:          logger,
	}, nil
}

type ObserveCommon struct {
	observeKeysMap *ObserveKeysMap
	logger         *log.CommonLogger

	observeKeysGetter func() map[string]interface{}
	manifestsPair     *metadata.CollectionsManifestPair
}

func NewObserveCommon(logger *log.CommonLogger, observeKeysGetter func() map[string]interface{},
	srcToTgtColIdMap map[uint32][]uint32,
	manifests *metadata.CollectionsManifestPair) (*ObserveCommon, error) {
	observeKeysMap, err := NewObserveKeysMap(logger, srcToTgtColIdMap)
	if err != nil {
		return nil, err
	}
	return &ObserveCommon{
		observeKeysMap:    observeKeysMap,
		logger:            logger,
		observeKeysGetter: observeKeysGetter,
		manifestsPair:     manifests,
	}, nil
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

	_, ok := scopesCollectionMap[xdcrBase.DefaultScopeCollectionName].(map[string][]string)
	// Only default scope exists
	if len(scopesCollectionMap) == 1 && ok {
		collectionsKeysMap := scopesCollectionMap[xdcrBase.DefaultScopeCollectionName].(map[string][]string)
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
			o.observeKeysMap.SetManifestsPair(&metadata.CollectionsManifestPair{
				Source: &defaultManifest,
				Target: &defaultManifest,
			})
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

	if o.manifestsPair == nil || o.manifestsPair.Source == nil || o.manifestsPair.Target == nil {
		return base.ErrorNoManifestProvided
	}

	for scopeName, collectionsMapRaw := range scopesCollectionMap {
		for collectionName, keys := range collectionsMapRaw.(map[string][]string) {
			ns := &xdcrBase.CollectionNamespace{
				ScopeName:      scopeName,
				CollectionName: collectionName,
			}
			for _, key := range keys {
				if err := o.observeKeysMap.Add(ns, key); err != nil {
					return base.ErrorScopeCollectionNotFoundInManifest
				}
			}
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
