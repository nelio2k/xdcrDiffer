package dcp

import (
	"fmt"
	xdcrParts "github.com/couchbase/goxdcr/base/filter"
	xdcrLog "github.com/couchbase/goxdcr/log"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
	"strings"
	"sync"
	"xdcrDiffer/base"
)

type DcpHandlerCommon struct {
	dcpClient               *DcpClient
	vbList                  []uint16
	dataChan                chan *base.Mutation
	incrementCounter        func()
	incrementSysCounter     func()
	colMigrationFilters     []string
	colMigrationFiltersOn   bool // shortcut to avoid len() check
	colMigrationFiltersImpl []xdcrParts.Filter
	utils                   xdcrUtils.UtilsIface
	isSource                bool
	filter                  xdcrParts.Filter
	logger                  *xdcrLog.CommonLogger
	finChan                 chan bool
	waitGrp                 sync.WaitGroup
	mutationProcessor       func(mut *base.Mutation)
}

func NewDcpHandlerCommon(dcpClient *DcpClient, vbList []uint16, dataChanSize int, incReceivedCounter func(),
	incSysEvtCounter func(), colMigrationFilters []string, utils xdcrUtils.UtilsIface) (*DcpHandlerCommon, error) {
	common := &DcpHandlerCommon{
		dcpClient:             dcpClient,
		vbList:                vbList,
		dataChan:              make(chan *base.Mutation, dataChanSize),
		incrementCounter:      incReceivedCounter,
		incrementSysCounter:   incSysEvtCounter,
		colMigrationFilters:   colMigrationFilters,
		colMigrationFiltersOn: len(colMigrationFilters) > 0,
		utils:                 utils,
		isSource:              strings.Contains(dcpClient.Name, base.SourceClusterName),
		filter:                dcpClient.dcpDriver.filter,
		logger:                dcpClient.logger,
		finChan:               make(chan bool),
	}

	if err := common.compileMigrCollectionFiltersIfNeeded(); err != nil {
		return nil, err
	}
	return common, nil
}

func (d *DcpHandlerCommon) SetMutationProcessor(processor func(mutation *base.Mutation)) {
	d.mutationProcessor = processor
}

func (d *DcpHandlerCommon) compileMigrCollectionFiltersIfNeeded() error {
	if len(d.colMigrationFilters) == 0 {
		return nil
	}

	for i, filterStr := range d.colMigrationFilters {
		filter, err := xdcrParts.NewFilter(fmt.Sprintf("%d", i), filterStr, d.utils, true)
		if err != nil {
			return fmt.Errorf("compiling %v resulted in: %v", filterStr, err)
		}
		d.colMigrationFiltersImpl = append(d.colMigrationFiltersImpl, filter)
	}
	return nil
}

func (d *DcpHandlerCommon) checkColMigrationFilters(mut *base.Mutation) []uint8 {
	var filterIdsMatched []uint8
	for i, filter := range d.colMigrationFiltersImpl {
		// If at least one passed, let it through
		matched, _, _, _ := filter.FilterUprEvent(mut.ToUprEvent())
		if matched {
			filterIdsMatched = append(filterIdsMatched, uint8(i))
		}
	}
	return filterIdsMatched
}

func (d *DcpHandlerCommon) replicationFilter(mut *base.Mutation, matched bool, filterResult base.FilterResultType) base.FilterResultType {
	var err error
	var errStr string
	if d.filter != nil && mut.IsMutation() {
		matched, err, errStr, _ = d.filter.FilterUprEvent(mut.ToUprEvent())
		if !matched {
			filterResult = base.Filtered
		}
		if err != nil {
			filterResult = base.UnableToFilter
			d.logger.Warnf("Err %v - (%v) when filtering mutation %v", err, errStr, mut)
		}
	}
	return filterResult
}

// Returns true if needs to skip processing
func (d *DcpHandlerCommon) preProcessMutation(mut *base.Mutation) bool {
	var matched bool
	var replicationFilterResult base.FilterResultType

	replicationFilterResult = d.replicationFilter(mut, matched, replicationFilterResult)
	valid := d.dcpClient.dcpDriver.checkpointManager.HandleMutationEvent(mut, replicationFilterResult)
	if !valid {
		// if mutation is out of range, ignore it
		return true
	}

	d.incrementCounter()

	// Ignore system events - we only care about actual data
	if mut.IsSystemEvent() {
		d.incrementSysCounter()
		return true
	}

	var filterIdsMatched []uint8
	if d.colMigrationFiltersOn && d.isSource {
		filterIdsMatched = d.checkColMigrationFilters(mut)
		if len(filterIdsMatched) == 0 {
			return true
		}
	}
	if d.colMigrationFiltersOn && len(filterIdsMatched) > 0 {
		mut.ColFiltersMatched = filterIdsMatched
	}
	return false
}

func (d *DcpHandlerCommon) Start() error {
	if d.mutationProcessor == nil {
		return fmt.Errorf("Mutation processor not set")
	}

	d.waitGrp.Add(1)
	go d.processData()
	return nil
}

func (d *DcpHandlerCommon) Stop() {
	close(d.finChan)
}

func (d *DcpHandlerCommon) processData() {
	defer d.waitGrp.Done()

	for {
		select {
		case <-d.finChan:
			goto done
		case mut := <-d.dataChan:
			skipProcessing := d.preProcessMutation(mut)
			if skipProcessing {
				continue
			}
			d.mutationProcessor(mut)
		}
	}
done:
}

func (d *DcpHandlerCommon) writeToDataChan(mut *base.Mutation) {
	select {
	case d.dataChan <- mut:
	// provides an alternative exit path when dh stops
	case <-d.finChan:
	}
}
