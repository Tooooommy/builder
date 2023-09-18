package errors

import "fmt"

type Error struct {
	err string
}

func New(message string, args ...any) error {
	return Error{err: "builder: " + fmt.Sprintf(message, args...)}
}

func NewEncodeError(t any) error {
	return Error{err: "builder_encode_error: " + fmt.Sprintf("Unable to encode value %+v", t)}
}

func (e Error) Error() string {
	return e.err
}
