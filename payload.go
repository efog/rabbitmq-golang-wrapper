package rmq

// Payload defines a publish payload
type Payload struct {
	contentType string
	content     string
}