package options

type Option struct {
	Head int64
	Tail int64
}

func OptsToOption(opts []Option) (Option, error) {
	opt := Option{}
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

func WithPagenation(head int64, tail int64) Option {
	return Option{
		Head: head,
		Tail: tail,
	}
}
