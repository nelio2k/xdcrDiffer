package dcp

type ObserverEphDcpHandler struct {
	*DcpHandlerCommon
}

func NewObserverEphDcpHandler(common *DcpHandlerCommon) (*ObserverEphDcpHandler, error) {
	return &ObserverEphDcpHandler{
		DcpHandlerCommon: common,
	}, nil
}
