# redblocks-go
This package is Go implemntation of [Altech/red_blocks](https://github.com/Altech/red_blocks).

[![CircleCI](https://circleci.com/gh/rerost/redblocks-go/tree/master.svg?style=svg)](https://circleci.com/gh/rerost/redblocks-go/tree/master) [![codecov](https://codecov.io/gh/rerost/redblocks-go/branch/master/graph/badge.svg)](https://codecov.io/gh/rerost/redblocks-go)

## install
```bash
go get -u /Users/user/.go/src/github.com/rerost/redblocks-go
```

## Example
```go
func NewRegionSet(region string) set.Set {
	return regionSetImp{region}
}

type regionSetImp struct {
	region string
}

func (r regionSetImp) KeySuffix() string {
	return r.region
}

func (r regionSetImp) Get(ctx context.Context) ([]set.IDWithScore, error) {
  ...
}

func (r regionSetImp) CacheTime() time.Duration {
	return time.Second * 10
}

store := store.NewRedisStore(newPool())
tokyo := compose.Compose(NewRegionSet("tokyo"), store)
osaka := compose.Compose(NewRegionSet("osaka"), store)

set := operator.NewIntersecionSet(store, time.Second*100, tokyo, osaka)
set.IDs(ctx, options.WithPagenation(0, -1))
```

[Full version](https://github.com/rerost/redblocks-go/blob/master/example/simple.go)
