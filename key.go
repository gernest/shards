package shards

import "fmt"

type Key struct {
	Field uint64
	View  string
}

func (k Key) Prefix() string {
	return fmt.Sprintf("~%d;%s", k.Field, k.View)
}

func (k Key) FieldPrefix() string {
	return fmt.Sprintf("~%d;", k.Field)
}
