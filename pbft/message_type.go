package pbft

type PrePrepareMsg struct {
	Data         []byte `json:"data"`
	SignResult   []byte `json:"sign_result"`
	CryptoMethod string `json:"crypto_method"`
	PubKey       []byte `json:"pub_key"`
}
