package fluxline

import "time"

type options struct {
	time time.Time
}

type Option func(*options)

// WithTime overrides the default timestamp (time.Now)
func WithTime(t time.Time) Option {
	return func(o *options) {
		o.time = t
	}
}
