package options

import (
	"github.com/rerost/redblocks-go/pkg/store"
)

type Operator struct {
	Store  []store.Store
	Weight []float64
}
