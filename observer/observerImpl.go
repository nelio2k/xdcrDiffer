package observer

import "fmt"

type ObserverImpl struct {
}

func NewObserverTool() (*ObserverImpl, error) {
	observer := &ObserverImpl{}

	return observer, nil
}

func (o *ObserverImpl) Run() error {

	return fmt.Errorf("Not implemented yet")
}
