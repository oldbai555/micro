package middleware

type Validator interface {
	Validate() error
}
