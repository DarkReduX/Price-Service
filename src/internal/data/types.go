package data

type SymbolPrice struct {
	Uuid   int64   `json:"uuid"`
	Symbol string  `json:"symbol"`
	Bid    float32 `json:"bid"`
	Ask    float32 `json:"ask"`
}
