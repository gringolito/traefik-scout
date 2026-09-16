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
}

// The types below mirror github.com/traefik/genconf dynamic.Configuration,
// restricted to the http.routers + http.services subset that traefik-scout
// emits.

type configOutput struct {
	HTTP *httpConfig `json:"http"`
}

type httpConfig struct {
	Routers  map[string]*outRouter  `json:"routers"`
	Services map[string]*outService `json:"services"`
}

type outRouter struct {
	EntryPoints []string `json:"entryPoints"`
	Service     string   `json:"service"`
	Rule        string   `json:"rule"`
}

type outService struct {
	LoadBalancer *loadBalancer `json:"loadBalancer"`
}

type loadBalancer struct {
	Servers        []server `json:"servers"`
	PassHostHeader bool     `json:"passHostHeader"`
}

type server struct {
	URL string `json:"url"`
}
