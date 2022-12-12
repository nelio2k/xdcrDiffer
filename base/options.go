package base

type Options struct {
	SourceUrl                         string
	SourceUsername                    string
	SourcePassword                    string
	SourceBucketName                  string
	RemoteClusterName                 string
	SourceFileDir                     string
	TargetUrl                         string
	TargetUsername                    string
	TargetPassword                    string
	TargetBucketName                  string
	TargetFileDir                     string
	NumberOfSourceDcpClients          uint64
	NumberOfWorkersPerSourceDcpClient uint64
	NumberOfTargetDcpClients          uint64
	NumberOfWorkersPerTargetDcpClient uint64
	NumberOfWorkersForFileDiffer      uint64
	NumberOfWorkersForMutationDiffer  uint64
	NumberOfBins                      uint64
	NumberOfFileDesc                  uint64
	// the duration that the tools should be run, in minutes
	CompleteByDuration uint64
	// whether tool should complete after processing all mutations at tool start time
	CompleteBySeqno bool
	// directory for checkpoint files
	CheckpointFileDir string
	// name of source cluster checkpoint file to load from when tool starts
	// if not specified, source cluster will start from 0
	OldSourceCheckpointFileName string
	// name of target cluster checkpoint file to load from when tool starts
	// if not specified, target cluster will start from 0
	OldTargetCheckpointFileName string
	// name of new checkpoint file to write to when tool shuts down
	// if not specified, tool will not save checkpoint files
	NewCheckpointFileName string
	// directory for storing diffs generated by file differ
	FileDifferDir string
	// output directory for mutation differ
	MutationDifferDir string
	// size of batch used by mutation differ
	MutationDifferBatchSize uint64
	// timeout, in seconds, used by mutation differ
	MutationDifferTimeout uint64
	// size of source dcp handler channel
	SourceDcpHandlerChanSize uint64
	// size of target dcp handler channel
	TargetDcpHandlerChanSize uint64
	// timeout for bucket for stats collection, in seconds
	BucketOpTimeout uint64
	// max number of retry for get stats
	MaxNumOfGetStatsRetry uint64
	// max number of retry for send batch
	MaxNumOfSendBatchRetry uint64
	// retry interval for get stats, in seconds
	GetStatsRetryInterval uint64
	// retry interval for send batch, in milliseconds
	SendBatchRetryInterval uint64
	// max backoff for get stats, in seconds
	GetStatsMaxBackoff uint64
	// max backoff for send batch, in seconds
	SendBatchMaxBackoff uint64
	// delay between source cluster start up and target cluster start up, in seconds
	DelayBetweenSourceAndTarget uint64
	//interval for periodical checkpointing, in seconds
	// value of 0 indicates no periodical checkpointing
	CheckpointInterval uint64
	// whether to run data generation
	RunDataGeneration bool
	// whether to run file differ
	RunFileDiffer bool
	// whether to verify diff keys through aysnc Get on clusters
	RunMutationDiffer bool
	// Whether or not to enforce secure communications for data retrieval
	EnforceTLS bool
	// Number of items kept in memory per binary buffer bucket
	BucketBufferCapacity int
	// Use Get instead of GetMeta to compare document body
	CompareBody bool
	// Number of times for mutationsDiffer to retry to resolve doc differences
	MutationDifferRetries int
	// Number of secs to wait between retries
	MutationDifferRetriesWaitSecs int
}