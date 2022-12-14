package observer

import (
	"fmt"
	"github.com/spf13/viper"
	"sync"
	"xdcrDiffer/base"
	"xdcrDiffer/dcp"
	"xdcrDiffer/differCommon"
	"xdcrDiffer/fileDescriptorPool"
)

// ObserverEphImpl stands for Observer Ephemeral Implementation
// This observer will keep all things in memory for observation
type ObserverEphImpl struct {
	*differCommon.XdcrDependencies

	sourceDcpDriver *dcp.DcpDriver
	targetDcpDriver *dcp.DcpDriver
}

func NewObserverTool() (*ObserverEphImpl, error) {
	var err error
	observer := &ObserverEphImpl{}
	observer.XdcrDependencies, err = differCommon.NewXdcrDependencies()
	if err != nil {
		return nil, err
	}

	return observer, nil
}

func (o *ObserverEphImpl) Run() error {
	var fileDescPool fileDescriptorPool.FdPoolIface
	if viper.GetInt(base.NumberOfFileDescKey) > 0 {
		fileDescPool = fileDescriptorPool.NewFileDescriptorPool(viper.GetInt(base.NumberOfFileDescKey))
	}

	errChan := make(chan error, 1)
	waitGroup := &sync.WaitGroup{}

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
		viper.GetInt(base.BucketBufferCapacityKey), dcp.ConstructObserverDcpHandler)

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
		viper.GetInt(base.BucketBufferCapacityKey), dcp.ConstructObserverDcpHandler)

	fmt.Printf("NEIL DEBUG not implemented yet\n")
	return fmt.Errorf("Not implemented yet")
}
