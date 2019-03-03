package redblocks

type PagenationOption struct {
	Head  int64
	Tail  int64
	Order Order
}

func PagenationOptionsToPagenationOption(opts []PagenationOption) (PagenationOption, error) {
	opt := PagenationOption{
		Head:  0,
		Tail:  -1,
		Order: Asc,
	}
	for _, o := range opts {
		if o.Head != 0 {
			opt.Head = o.Head
		}
		if o.Tail != 0 {
			opt.Tail = o.Tail
		}
		if opt.Order == Asc && o.Order != Asc {
			opt.Order = o.Order
		}
	}

	return opt, nil
}

func WithPagenation(head int64, tail int64) PagenationOption {
	return PagenationOption{
		Head: head,
		Tail: tail,
	}
}

func WithOrder(order Order) PagenationOption {
	return PagenationOption{
		Order: order,
	}
}
