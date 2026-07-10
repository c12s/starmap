package repos

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// stripTag drops the OCI tag but keeps a registry port (host:port/path).
func stripTag(image string) string {
	idx := strings.LastIndex(image, ":")
	if idx == -1 {
		return image
	}
	if strings.Contains(image[idx+1:], "/") {
		return image
	}
	return image[:idx]
}

func layerIdentityHash(sourceType, image, pull, command string) string {
	if sourceType == "oci" {
		return computeHash(stripTag(image))
	}
	return computeHash(pull + command)
}

func (r *RegistryRepo) PushLayer(ctx context.Context, in domain.PushLayerInput) (*domain.PushLayerResult, error) {
	if in.Sha == "" {
		return nil, fmt.Errorf("sha is required")
	}
	if in.SourceType != "oci" && in.SourceType != "git" {
		return nil, fmt.Errorf("sourceType must be 'oci' or 'git'")
	}
	if in.SourceType == "oci" && in.Image == "" {
		return nil, fmt.Errorf("image is required for oci layer")
	}
	if in.SourceType == "git" && in.Pull == "" {
		return nil, fmt.Errorf("pull is required for git layer")
	}

	image := stripTag(in.Image)
	hash := layerIdentityHash(in.SourceType, in.Image, in.Pull, in.Command)

	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	typeLabel := ""
	switch in.NodeType {
	case "StoredProcedure", "Trigger":
		typeLabel = ", l:" + in.NodeType
	case "":
	default:
		return nil, fmt.Errorf("nodeType must be 'StoredProcedure', 'Trigger' or empty")
	}

	result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		mergeLayer := `
			MERGE (l:Layer {hash: $hash})
			ON CREATE SET
				l.sourceType = $sourceType,
				l.image = $image,
				l.pull = $pull,
				l.command = $command,
				l.managed = true` + typeLabel + `
			RETURN l.sourceType AS existingType
		`
		res, err := tx.Run(ctx, mergeLayer, map[string]any{
			"hash":       hash,
			"sourceType": in.SourceType,
			"image":      image,
			"pull":       in.Pull,
			"command":    in.Command,
		})
		if err != nil {
			return nil, fmt.Errorf("merge layer: %w", err)
		}
		rec, err := res.Single(ctx)
		if err != nil {
			return nil, fmt.Errorf("merge layer single: %w", err)
		}
		if existingType, ok := rec.Get("existingType"); ok {
			if s, _ := existingType.(string); s != "" && s != in.SourceType {
				return nil, fmt.Errorf("sourceType mismatch: layer exists as %q, got %q", s, in.SourceType)
			}
		}

		checkSha := `
			MATCH (l:Layer {hash: $hash})-[:HAS_LAYER_VERSION]->(v:LayerVersion {sha: $sha})
			RETURN v.semver AS semver
		`
		res, err = tx.Run(ctx, checkSha, map[string]any{"hash": hash, "sha": in.Sha})
		if err != nil {
			return nil, fmt.Errorf("check sha: %w", err)
		}
		if res.Next(ctx) {
			semver, _ := res.Record().Get("semver")
			s, _ := semver.(string)
			return &domain.PushLayerResult{Sha: in.Sha, Semver: s}, nil
		}

		findLatest := `
			MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(v:LayerVersion)
			RETURN v.semver AS semver
		`
		res, err = tx.Run(ctx, findLatest, map[string]any{"hash": hash})
		if err != nil {
			return nil, fmt.Errorf("find latest: %w", err)
		}
		prevSemver := ""
		if res.Next(ctx) {
			s, _ := res.Record().Get("semver")
			prevSemver, _ = s.(string)
		}

		semver := in.Semver
		if semver == "" {
			if prevSemver == "" {
				semver = "v1.0.0"
			} else {
				semver = incrementVersion(prevSemver)
			}
		} else {
			checkSemver := `
				MATCH (l:Layer {hash: $hash})-[:HAS_LAYER_VERSION]->(v:LayerVersion {semver: $semver})
				RETURN v.sha AS sha
			`
			res, err = tx.Run(ctx, checkSemver, map[string]any{"hash": hash, "semver": semver})
			if err != nil {
				return nil, fmt.Errorf("check semver: %w", err)
			}
			if res.Next(ctx) {
				return nil, fmt.Errorf("semver %q already exists with a different sha", semver)
			}
		}

		createVersion := `
			MATCH (l:Layer {hash: $hash})
			CREATE (v:LayerVersion {sha: $sha, semver: $semver, createdAt: $now})
			MERGE (l)-[:HAS_LAYER_VERSION]->(v)
			WITH l, v
			OPTIONAL MATCH (l)-[old:HAS_LATEST]->(prev:LayerVersion)
			FOREACH (_ IN CASE WHEN prev IS NOT NULL THEN [1] ELSE [] END |
				MERGE (v)-[:PREVIOUS]->(prev)
				DELETE old
			)
			MERGE (l)-[:HAS_LATEST]->(v)
		`
		if _, err := tx.Run(ctx, createVersion, map[string]any{
			"hash":   hash,
			"sha":    in.Sha,
			"semver": semver,
			"now":    time.Now().Unix(),
		}); err != nil {
			return nil, fmt.Errorf("create version: %w", err)
		}

		return &domain.PushLayerResult{
			Sha:            in.Sha,
			Semver:         semver,
			PreviousSemver: prevSemver,
		}, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(*domain.PushLayerResult), nil
}
