package repos

import (
	"context"
	"fmt"
	"log"

	"github.com/c12s/starmap/internal/config"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type RegistryRepo struct {
	driver neo4j.DriverWithContext
}

func NewRegistryRepo() (*RegistryRepo, error) {
	cfg := config.GetConfig()

	driver, err := neo4j.NewDriverWithContext(cfg.NEO4J_uri, neo4j.BasicAuth(cfg.NEO4J_user, cfg.NEO4J_pass, ""))
	if err != nil {
		return nil, fmt.Errorf("failed to create Neo4j driver: %w", err)
	}

	err = driver.VerifyConnectivity(context.Background())
	if err != nil {
		_ = driver.Close(context.Background())
		return nil, fmt.Errorf("failed to connect to Neo4j: %w", err)
	}

	log.Println("Connected to Neo4j:", cfg.NEO4J_uri)

	return &RegistryRepo{driver: driver}, nil
}

func (r *RegistryRepo) Close() {
	if r.driver != nil {
		_ = r.driver.Close(context.Background())
		log.Println("Closed Neo4j connection")
	}
}
