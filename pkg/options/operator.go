package options

import (
	"github.com/rerost/red-blocks-go/pkg/store"
)

type Operator struct {
	Store  []store.Store
	Weight []float64
}
