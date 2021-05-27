package bin_back

type ServerDataRequest struct {
	Addr    string
	Members []string
	Delete  bool
}

type ServerData struct {
	D map[string][]string
}