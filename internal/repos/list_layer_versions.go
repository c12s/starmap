package repos

import (
	"context"
	"fmt"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) ListLayerVersions(ctx context.Context, sourceType, image, pull, command string) (*domain.ListLayerVersionsResult, error) {
	var hash string
	if image != "" {
		hash = computeHash(stripTag(image))
	} else {
		hash = computeHash(pull + command)
	}

	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// old: OPTIONAL MATCH (l)-[:HAS_LAYER_VERSION]->(v)
		query := `
			MATCH (l:Layer {hash: $hash})
			OPTIONAL MATCH (l)-[:HAS_LATEST]->(latest:LayerVersion)
			OPTIONAL MATCH (latest)-[:PREVIOUS*0..]->(v:LayerVersion)
			RETURN l.sourceType AS sourceType,
				collect({
					sha: v.sha,
					semver: v.semver,
					createdAt: v.createdAt,
					arch: v.arch,
					isLatest: (latest IS NOT NULL AND v.sha = latest.sha)
				}) AS versions
		`
		res, err := tx.Run(ctx, query, map[string]any{"hash": hash})
		if err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
		if !res.Next(ctx) {
			return nil, fmt.Errorf("layer not found")
		}
		rec := res.Record()

		out := &domain.ListLayerVersionsResult{Versions: []domain.LayerVersion{}}
		if st, ok := rec.Get("sourceType"); ok {
			out.SourceType, _ = st.(string)
		}
		if raw, ok := rec.Get("versions"); ok {
			if arr, ok := raw.([]any); ok {
				for _, item := range arr {
					m, ok := item.(map[string]any)
					if !ok || m["sha"] == nil {
						continue
					}
					lv := domain.LayerVersion{}
					lv.Sha, _ = m["sha"].(string)
					lv.Semver, _ = m["semver"].(string)
					lv.Arch, _ = m["arch"].(string)
					if c, ok := m["createdAt"].(int64); ok {
						lv.CreatedAt = c
					}
					lv.IsLatest, _ = m["isLatest"].(bool)
					out.Versions = append(out.Versions, lv)
				}
			}
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*domain.ListLayerVersionsResult), nil
}
