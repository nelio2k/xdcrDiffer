package observer

type Observer interface {
	Run() error
}

// History is meant to keep track of all the document's histories
type History interface {
}
