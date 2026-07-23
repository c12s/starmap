package repos

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) DeleteLayer(ctx context.Context, image, pull, command string) error {
	var hash string
	if image != "" {
		hash = computeHash(stripTag(image))
	} else {
		hash = computeHash(pull + command)
	}

	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		checkUsage := `
			MATCH (l:Layer {hash: $hash})
			OPTIONAL MATCH (v:Version)-[:HAS_PROCEDURE|HAS_TRIGGER]->(l)
			RETURN l IS NOT NULL AS exists, count(v) AS usedBy
		`
		res, err := tx.Run(ctx, checkUsage, map[string]any{"hash": hash})
		if err != nil {
			return nil, err
		}
		rec, err := res.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("layer not found")
		}
		if exists, _ := rec.Get("exists"); exists != true {
			return nil, fmt.Errorf("layer not found")
		}
		if usedBy, _ := rec.Get("usedBy"); usedBy.(int64) > 0 {
			return nil, fmt.Errorf("layer is used by %d chart version(s), cannot delete", usedBy)
		}

		// old: OPTIONAL MATCH (l)-[:HAS_LAYER_VERSION]->(v)
		deleteLayer := `
			MATCH (l:Layer {hash: $hash})
			OPTIONAL MATCH (l)-[:HAS_LATEST]->(head:LayerVersion)
			OPTIONAL MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion)
			DETACH DELETE l, v
		`
		if _, err := tx.Run(ctx, deleteLayer, map[string]any{"hash": hash}); err != nil {
			return nil, fmt.Errorf("failed to delete layer: %w", err)
		}
		return nil, nil
	})
	return err
}
