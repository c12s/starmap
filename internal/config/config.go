package config

import (
	"fmt"
	"os"
)

type Conifg struct {
	ServicePort string
	NEO4J_uri   string
	NEO4J_user  string
	NEO4J_pass  string
}

func GetConfig() Conifg {
	return Conifg{
		ServicePort: fmt.Sprintf(":%s", os.Getenv("REGISTRY_SERVICE_PORT")),
		NEO4J_uri:   os.Getenv("NEO4J_URI"),
		NEO4J_user:  os.Getenv("NEO4J_USER"),
		NEO4J_pass:  os.Getenv("NEO4J_PASS"),
	}
}
