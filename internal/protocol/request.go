package protocol

type Request struct {
	Action string `json:"action"`
	Topic string `json:"topic"`
	Value string `json:"value"`
	Offset int64 `json:"offset"`
	Partition int `json:"partition"`
}