package redblocks

type Order int

const (
	Asc = iota
	Desc
)

func (o Order) String() string {
	switch o {
	case Asc:
		return "Asc"
	case Desc:
		return "Desc"
	default:
		return ""
	}
}
