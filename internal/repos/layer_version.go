package repos

import (
	"context"
	"fmt"
	"time"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func ensureLayerVersionFromMetadata(ctx context.Context, tx neo4j.ManagedTransaction, hash string, m domain.Metadata) error {
	if m.Semver == "" || m.Arch == "" || m.Sha == "" {
		return nil
	}
	return ensureLayerVersion(ctx, tx, hash, m.Semver, m.Sha, []domain.LayerBuild{{Arch: m.Arch, Sha: m.Sha}})
}

func ensureLayerVersion(ctx context.Context, tx neo4j.ManagedTransaction, hash, semver, listSha string, builds []domain.LayerBuild) error {
	findVersion := `
		MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(head:LayerVersion)
		MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion {semver: $semver})
		RETURN v.semver AS semver LIMIT 1
	`
	res, err := tx.Run(ctx, findVersion, map[string]any{"hash": hash, "semver": semver})
	if err != nil {
		return fmt.Errorf("find version: %w", err)
	}
	versionExists := res.Next(ctx)

	if !versionExists {
		createVersion := `
			MATCH (l:Layer {hash: $hash})
			CREATE (v:LayerVersion {semver: $semver, sha: $sha, createdAt: $now})
			WITH l, v
			OPTIONAL MATCH (l)-[old:HAS_LATEST]->(prev:LayerVersion)
			FOREACH (_ IN CASE WHEN prev IS NOT NULL THEN [1] ELSE [] END |
				MERGE (v)-[:PREVIOUS]->(prev)
				DELETE old
			)
			MERGE (l)-[:HAS_LATEST]->(v)
		`
		if _, err := tx.Run(ctx, createVersion, map[string]any{
			"hash": hash, "semver": semver, "sha": listSha, "now": time.Now().Unix(),
		}); err != nil {
			return fmt.Errorf("create version: %w", err)
		}
	} else if listSha != "" {
		setSha := `
			MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(head:LayerVersion)
			MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion {semver: $semver})
			SET v.sha = $sha
		`
		if _, err := tx.Run(ctx, setSha, map[string]any{
			"hash": hash, "semver": semver, "sha": listSha,
		}); err != nil {
			return fmt.Errorf("update list sha: %w", err)
		}
	}

	for _, b := range builds {
		checkBuild := `
			MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(head:LayerVersion)
			MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion {semver: $semver})-[:HAS_BUILD]->(bld:Build {arch: $arch})
			RETURN bld.sha AS sha LIMIT 1
		`
		cres, err := tx.Run(ctx, checkBuild, map[string]any{"hash": hash, "semver": semver, "arch": b.Arch})
		if err != nil {
			return fmt.Errorf("check build: %w", err)
		}
		if cres.Next(ctx) {
			existing, _ := cres.Record().Get("sha")
			if es, _ := existing.(string); es != b.Sha {
				return fmt.Errorf("version %q arch %q already exists with a different sha", semver, b.Arch)
			}
			continue
		}
		addBuild := `
			MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(head:LayerVersion)
			MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion {semver: $semver})
			MERGE (v)-[:HAS_BUILD]->(b:Build {arch: $arch})
			SET b.sha = $sha
		`
		if _, err := tx.Run(ctx, addBuild, map[string]any{
			"hash": hash, "semver": semver, "arch": b.Arch, "sha": b.Sha,
		}); err != nil {
			return fmt.Errorf("add build: %w", err)
		}
	}

	return nil
}
