package observer

import "xdcrDiffer/base"

type Observer interface {
	Run() error
}

// History is meant to keep track of all the document's histories
type History interface {
	MarkMutation(mut *base.Mutation)
}
