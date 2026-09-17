package app

// rawdataResponse is the shape of GET /api/rawdata from a downstream Traefik
// instance.  Only the routers field is consumed; services and middlewares are
// ignored.
type rawdataResponse struct {
	Routers map[string]*rawRouter `json:"routers"`
}

type rawRouter struct {
	EntryPoints []string `json:"entryPoints"`
	Service     string   `json:"service"`
	Rule        string   `json:"rule"`
	// Status is "enabled", "disabled", or "warning".
	Status   string `json:"status"`
	Provider string `json:"provider"`
	Priority int    `json:"priority"`
}
