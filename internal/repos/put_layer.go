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
	if len(in.Builds) == 0 {
		return nil, fmt.Errorf("at least one build (arch + sha) is required")
	}
	for _, b := range in.Builds {
		if b.Sha == "" || b.Arch == "" {
			return nil, fmt.Errorf("each build needs arch and sha")
		}
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
	case "StoredProcedure", "Trigger", "Event", "Entrypoint":
		typeLabel = ", l:" + in.NodeType
	case "":
	default:
		return nil, fmt.Errorf("nodeType must be 'StoredProcedure', 'Trigger', 'Event', 'Entrypoint' or empty")
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

		// current latest semver (for auto-increment + PREVIOUS)
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
		}

		// does this semver already exist (anywhere in the PREVIOUS chain)?
		findVersion := `
			MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(head:LayerVersion)
			MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion {semver: $semver})
			RETURN v.semver AS semver LIMIT 1
		`
		res, err = tx.Run(ctx, findVersion, map[string]any{"hash": hash, "semver": semver})
		if err != nil {
			return nil, fmt.Errorf("find version: %w", err)
		}
		versionExists := res.Next(ctx)

		if !versionExists {
			// new version node + move PREVIOUS/HAS_LATEST
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
				"hash": hash, "semver": semver, "sha": in.Sha, "now": time.Now().Unix(),
			}); err != nil {
				return nil, fmt.Errorf("create version: %w", err)
			}
		} else if in.Sha != "" {
			// version exists (adding a new arch build) — keep list digest current
			setSha := `
				MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(head:LayerVersion)
				MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion {semver: $semver})
				SET v.sha = $sha
			`
			if _, err := tx.Run(ctx, setSha, map[string]any{
				"hash": hash, "semver": semver, "sha": in.Sha,
			}); err != nil {
				return nil, fmt.Errorf("update list sha: %w", err)
			}
		}

		// attach builds (per arch). same arch + different sha = immutable error.
		for _, b := range in.Builds {
			checkBuild := `
				MATCH (l:Layer {hash: $hash})-[:HAS_LATEST]->(head:LayerVersion)
				MATCH (head)-[:PREVIOUS*0..]->(v:LayerVersion {semver: $semver})-[:HAS_BUILD]->(bld:Build {arch: $arch})
				RETURN bld.sha AS sha LIMIT 1
			`
			cres, err := tx.Run(ctx, checkBuild, map[string]any{"hash": hash, "semver": semver, "arch": b.Arch})
			if err != nil {
				return nil, fmt.Errorf("check build: %w", err)
			}
			if cres.Next(ctx) {
				existing, _ := cres.Record().Get("sha")
				if es, _ := existing.(string); es != b.Sha {
					return nil, fmt.Errorf("version %q arch %q already exists with a different sha", semver, b.Arch)
				}
				continue // same arch + same sha = no-op
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
				return nil, fmt.Errorf("add build: %w", err)
			}
		}

		return &domain.PushLayerResult{
			Semver:         semver,
			PreviousSemver: prevSemver,
		}, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(*domain.PushLayerResult), nil
}
