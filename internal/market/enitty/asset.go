package enitty

type Asset struct {
	ID string
	Name string
	MarketVolume int
}

func NewAsset(id string, name string, marketVolume int) *Asset {
	return &Asset{
		ID: id,
		Name: name,
		MarketVolume: marketVolume,
	}
}
