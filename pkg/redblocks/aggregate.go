package redblocks

type Aggregate int

const (
	Min = iota
	Max
	Sum
)

func (a Aggregate) String() string {
	switch a {
	case Min:
		return "MIN"
	case Max:
		return "MAX"
	case Sum:
		return "SUM"
	default:
		return ""
	}
}
