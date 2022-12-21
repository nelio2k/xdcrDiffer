package observerHandler

import (
	"xdcrDiffer/base"
)

type ObserverHandler interface {
	HandleMutation(mut *base.Mutation)
}
