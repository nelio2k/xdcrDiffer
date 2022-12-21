package observer

import (
	"fmt"
	"sync"
	"xdcrDiffer/observerHandler"

	"github.com/spf13/viper"

	"xdcrDiffer/base"
	"xdcrDiffer/dcp"
	"xdcrDiffer/differCommon"
	"xdcrDiffer/fileDescriptorPool"
)

// ObserveEphImpl stands for Observer Ephemeral Implementation
// This observer will keep all things in memory for observation
type ObserveEphImpl struct {
	*differCommon.XdcrDependencies
	*ObserveCommon

	sourceDcpDriver *dcp.DcpDriver
	targetDcpDriver *dcp.DcpDriver

	keysGetter func() map[string]interface{}
}

func NewObserverTool(observeKeysGetter func() map[string]interface{}) (*ObserveEphImpl, error) {
	var err error
	observer := &ObserveEphImpl{
		keysGetter: observeKeysGetter,
	}
	observer.XdcrDependencies, err = differCommon.NewXdcrDependencies()
	if err != nil {
		return nil, err
	}

	return observer, nil
}

func (o *ObserveEphImpl) Run() error {
	var fileDescPool fileDescriptorPool.FdPoolIface
	if viper.GetInt(base.NumberOfFileDescKey) > 0 {
		fileDescPool = fileDescriptorPool.NewFileDescriptorPool(viper.GetInt(base.NumberOfFileDescKey))
	}

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

	// Prior to observe, need to establish collection links
	manifestsPair, err := o.GetManifestsPair()
	if err != nil {
		return err
	}
	o.ObserveCommon, err = NewObserveCommon(o.Logger(), o.keysGetter, o.SrcToTgtColIdsMap, manifestsPair)
	if err != nil {
		return err
	}
	if err = o.TranslateObserverKeysList(); err != nil {
		return err
	}

	o.sourceDcpDriver = dcp.StartDcpDriver(o.Logger(), base.SourceClusterName, viper.GetString(base.SourceUrlKey),
		o.SpecifiedSpec.SourceBucketName,
		o.SelfRef, viper.GetString(base.SourceFileDirKey), viper.GetString(base.CheckpointFileDirKey),
		viper.GetString(base.OldSourceCheckpointFileNameKey), viper.GetString(base.NewCheckpointFileNameKey),
		viper.GetUint64(base.NumberOfSourceDcpClientsKey),
		viper.GetUint64(base.NumberOfWorkersPerSourceDcpClientKey), viper.GetUint64(base.NumberOfBinsKey),
		viper.GetUint64(base.SourceDcpHandlerChanSizeKey),
		viper.GetUint64(base.BucketOpTimeoutKey), viper.GetUint64(base.MaxNumOfGetStatsRetryKey),
		viper.GetUint64(base.GetStatsRetryIntervalKey),
		viper.GetUint64(base.GetStatsMaxBackoffKey), viper.GetUint64(base.CheckpointIntervalKey), errChan, waitGroup,
		false /*completeBySeqno*/, fileDescPool, o.Filter,
		o.SrcCapabilities, o.SrcCollectionIds, o.ColFilterOrderedKeys, o.Utils,
		viper.GetInt(base.BucketBufferCapacityKey), o.ConstructObserverDcpHandlerSource)

	o.targetDcpDriver = dcp.StartDcpDriver(o.Logger(), base.TargetClusterName, o.SpecifiedRef.HostName_,
		o.SpecifiedSpec.TargetBucketName, o.SpecifiedRef,
		viper.GetString(base.TargetFileDirKey), viper.GetString(base.CheckpointFileDirKey),
		viper.GetString(base.OldTargetCheckpointFileNameKey), viper.GetString(base.NewCheckpointFileNameKey),
		viper.GetUint64(base.NumberOfTargetDcpClientsKey), viper.GetUint64(base.NumberOfWorkersPerTargetDcpClientKey),
		viper.GetUint64(base.NumberOfBinsKey), viper.GetUint64(base.TargetDcpHandlerChanSizeKey),
		viper.GetUint64(base.BucketOpTimeoutKey), viper.GetUint64(base.MaxNumOfGetStatsRetryKey),
		viper.GetUint64(base.GetStatsRetryIntervalKey), viper.GetUint64(base.GetStatsMaxBackoffKey),
		viper.GetUint64(base.CheckpointIntervalKey), errChan, waitGroup,
		viper.GetBool(base.CompleteBySeqnoKey), fileDescPool, o.Filter,
		o.TgtCapabilities, o.TgtCollectionIds, o.ColFilterOrderedKeys, o.Utils,
		viper.GetInt(base.BucketBufferCapacityKey), o.ConstructObserverDcpHandlerTarget)

	fmt.Printf("NEIL DEBUG not implemented yet\n")
	return fmt.Errorf("Not implemented yet")
}

func (o *ObserveEphImpl) ConstructObserverDcpHandlerSource(dcpClient *dcp.DcpClient, d *dcp.DcpDriver, j int, handlerVBList []uint16) (dcp.DcpHandler, error) {
	common, err := dcp.ConstructDcpHandlerCommon(dcpClient, d, handlerVBList)
	if err != nil {
		return nil, err
	}
	observerHandler, err := o.GetDcpHandlerSource()
	if err != nil {
		return nil, err
	}

	return dcp.NewObserverEphDcpHandler(common, observerHandler)
}

func (o *ObserveEphImpl) ConstructObserverDcpHandlerTarget(dcpClient *dcp.DcpClient, d *dcp.DcpDriver, j int, handlerVBList []uint16) (dcp.DcpHandler, error) {
	common, err := dcp.ConstructDcpHandlerCommon(dcpClient, d, handlerVBList)
	if err != nil {
		return nil, err
	}
	observerHandler, err := o.GetDcpHandlerTarget()
	if err != nil {
		return nil, err
	}

	return dcp.NewObserverEphDcpHandler(common, observerHandler)
}

func (o *ObserveEphImpl) GetDcpHandlerSource() (observerHandler.ObserverHandler, error) {
	handler := newInternalHandler(o, true)
	return handler, nil
}

func (o *ObserveEphImpl) GetDcpHandlerTarget() (observerHandler.ObserverHandler, error) {
	handler := newInternalHandler(o, false)
	return handler, nil
}

func newInternalHandler(o *ObserveEphImpl, isSource bool) *internalHandler {
	return &internalHandler{
		o:        o,
		isSource: isSource,
	}
}

type internalHandler struct {
	o        *ObserveEphImpl
	isSource bool
}

func (i *internalHandler) HandleMutation(mut *base.Mutation) {
	if !i.o.observeKeysMap.MutationIsObserved(mut, i.isSource) {
		return
	}

}
