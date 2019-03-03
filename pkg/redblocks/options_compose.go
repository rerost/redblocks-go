package redblocks

type ComposeOption struct {
	Key string
}

func ComposeOptionsToComposeOption(opts []ComposeOption) (ComposeOption, error) {
	opt := ComposeOption{}
	for _, o := range opts {
		if o.Key == "" {
			opt.Key = o.Key
		}
	}

	return opt, nil
}

func WithKey(key string) ComposeOption {
	return ComposeOption{
		Key: key,
	}
}
