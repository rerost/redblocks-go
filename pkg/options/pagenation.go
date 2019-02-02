package options

type PagenationOption struct {
	Head int64
	Tail int64
}

func PagenationOptionsToPagenationOption(opts []PagenationOption) (PagenationOption, error) {
	opt := PagenationOption{}
	for _, o := range opts {
		if o.Head != 0 {
			opt.Head = o.Head
		}
		if o.Tail != 0 {
			opt.Tail = o.Tail
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
