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
		query := `
			MATCH (l:Layer {hash: $hash})
			OPTIONAL MATCH (l)-[:HAS_LATEST]->(latest:LayerVersion)
			OPTIONAL MATCH (latest)-[:PREVIOUS*0..]->(v:LayerVersion)
			OPTIONAL MATCH (v)-[:HAS_BUILD]->(b:Build)
			WITH l, latest, v, collect({arch: b.arch, sha: b.sha}) AS builds
			RETURN l.sourceType AS sourceType,
				collect({
					semver: v.semver,
					sha: v.sha,
					createdAt: v.createdAt,
					isLatest: (latest IS NOT NULL AND v.semver = latest.semver),
					builds: builds
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
					if !ok || m["semver"] == nil {
						continue
					}
					lv := domain.LayerVersion{}
					lv.Semver, _ = m["semver"].(string)
					lv.Sha, _ = m["sha"].(string)
					if c, ok := m["createdAt"].(int64); ok {
						lv.CreatedAt = c
					}
					lv.IsLatest, _ = m["isLatest"].(bool)
					if braw, ok := m["builds"].([]any); ok {
						for _, bi := range braw {
							bm, ok := bi.(map[string]any)
							if !ok || bm["sha"] == nil {
								continue
							}
							lb := domain.LayerBuild{}
							lb.Arch, _ = bm["arch"].(string)
							lb.Sha, _ = bm["sha"].(string)
							lv.Builds = append(lv.Builds, lb)
						}
					}
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
