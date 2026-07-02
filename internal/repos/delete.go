package repos

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) DeleteChart(ctx context.Context, id, name, namespace, maintainer, schemaVersion, kind string) error {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})

	if namespace == "" {
		namespace = "default"
	}

	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		checkExists := `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})
			MATCH (n)-[:HAS_CHART]->(c:Chart {id: $chartId})

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(direct:Version {schemaVersion: $schemaVersion})

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(extended:Version {schemaVersion: $schemaVersion})

			WITH c, direct, extended
			RETURN
				c IS NOT NULL AS chartExists,
				(direct IS NOT NULL OR extended IS NOT NULL) AS versionExists,
				direct IS NOT NULL AS isDirect
		`

		res, err := tx.Run(ctx, checkExists, map[string]any{
			"chartId":       id,
			"namespace":     namespace,
			"maintainer":    maintainer,
			"schemaVersion": schemaVersion,
		})
		if err != nil {
			return nil, err
		}

		if !res.Next(ctx) {
			return nil, fmt.Errorf("chart with id %s not found in namespace %s for maintainer %s", id, namespace, maintainer)
		}

		record := res.Record()

		versionExists, _ := record.Get("versionExists")
		if versionExists == nil || !versionExists.(bool) {
			return nil, fmt.Errorf("version %s not found for chart %s", schemaVersion, id)
		}

		isDirect, _ := record.Get("isDirect")
		isDirectBool := isDirect != nil && isDirect.(bool)

		checkExtend := `
			MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(root:Version {schemaVersion: $schemaVersion})
			OPTIONAL MATCH (root)<-[:EXTEND*1..]-(v:Version)
			RETURN count(v) AS cnt
		`

		res, err = tx.Run(ctx, checkExtend, map[string]any{
			"chartId":       id,
			"schemaVersion": schemaVersion,
		})
		if err != nil {
			return nil, err
		}

		if res.Next(ctx) {
			cnt, _ := res.Record().Get("cnt")
			if cnt.(int64) > 0 {
				return nil, fmt.Errorf(
					"version %s cannot be deleted because it is extended by another version",
					schemaVersion,
				)
			}
		}

		checkExtend2 := `
			MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(root:Version)
			OPTIONAL MATCH (root)<-[:EXTEND*0..]-(v:Version {schemaVersion: $schemaVersion})
			OPTIONAL MATCH (v)<-[:EXTEND*1..]-(child:Version)
			RETURN count(child) AS cnt
		`

		res, err = tx.Run(ctx, checkExtend2, map[string]any{
			"chartId":       id,
			"schemaVersion": schemaVersion,
		})
		if err != nil {
			return nil, err
		}

		if res.Next(ctx) {
			cnt, _ := res.Record().Get("cnt")
			if cnt.(int64) > 0 {
				return nil, fmt.Errorf(
					"version %s cannot be deleted because it is extended by another version",
					schemaVersion,
				)
			}
		}

		if isDirectBool {
			queryDeleteVersion := `
				MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})
				MATCH (n)-[hc:HAS_CHART]->(c:Chart {id: $chartId})

				SET hc.versions = [v IN hc.versions WHERE v <> $schemaVersion]

				WITH c, hc

				OPTIONAL MATCH (c)-[r:HAS_VERSION]->(v:Version {schemaVersion: $schemaVersion})
				DELETE r
				WITH c, hc

				WITH c, hc
				WHERE size(hc.versions) = 0
				DELETE hc
				WITH c

				OPTIONAL MATCH (c)-[:HAS_VERSION]->(otherV)
				WITH c, collect(otherV) AS versions
				WHERE size(versions) = 0
				DETACH DELETE c
			`

			_, err = tx.Run(ctx, queryDeleteVersion, map[string]any{
				"chartId":       id,
				"name":          name,
				"namespace":     namespace,
				"schemaVersion": schemaVersion,
				"kind":          kind,
				"maintainer":    maintainer,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to delete Version: %w", err)
			}
		} else {
			queryDeleteExtended := `
				MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v:Version {schemaVersion: $schemaVersion})

				MATCH (v)-[:EXTEND]->(parent:Version)

				MATCH (n:Namespace {name: $namespace})-[hc:HAS_CHART]->(c)
				SET hc.versions = 
					CASE 
						WHEN NOT parent.schemaVersion IN hc.versions 
						THEN [x IN hc.versions WHERE x <> $schemaVersion] + parent.schemaVersion 
						ELSE [x IN hc.versions WHERE x <> $schemaVersion]
					END

				WITH v
				DETACH DELETE v
			`

			_, err = tx.Run(ctx, queryDeleteExtended, map[string]any{
				"chartId":       id,
				"schemaVersion": schemaVersion,
				"namespace":     namespace,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to delete extended Version: %w", err)
			}
		}

		// Version
		queryVersion := `
			MATCH (v:Version)
			WHERE NOT (:Chart)-[:HAS_VERSION]->(v)
			AND NOT (v)<-[:EXTEND]-(:Version)
			AND NOT (v)-[:EXTEND]->(:Version)
			DETACH DELETE v
		`
		_, err = tx.Run(ctx, queryVersion, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Version: %w", err)
		}
		// Namespace
		queryNamespace := `
			MATCH (n:Namespace {name: $namespace})
			WHERE NOT (n)-[:HAS_CHART]->(:Chart)
			DETACH DELETE n
		`
		_, err = tx.Run(ctx, queryNamespace, map[string]any{
			"namespace": namespace,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to delete Namespace: %w", err)
		}

		// User
		queryUser := `
			MATCH (u:User {name: $maintainer})
			WHERE NOT (u)-[:HAS_NAMESPACE]->(:Namespace)
			DETACH DELETE u
		`
		_, err = tx.Run(ctx, queryUser, map[string]any{
			"maintainer": maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to delete User: %w", err)
		}

		// StoredProcedures
		querySP := `
			MATCH (s:StoredProcedure)
			WHERE NOT (:Version)-[:HAS_PROCEDURE]->(s)
			DETACH DELETE s
		`
		_, err = tx.Run(ctx, querySP, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan StoredProcedures: %w", err)
		}

		// Triggers
		queryTriggers := `
			MATCH (t:Trigger)
			WHERE NOT (:Version)-[:HAS_TRIGGER]->(t)
			DETACH DELETE t
		`
		_, err = tx.Run(ctx, queryTriggers, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Triggers: %w", err)
		}

		// DataSources
		queryDS := `
			MATCH (d:DataSource)
			WHERE NOT (d)<-[:HARD_LINK|:SOFT_LINK]-(:StoredProcedure)
			AND NOT (d)<-[:HARD_LINK|:SOFT_LINK]-(:Trigger)
			DETACH DELETE d
		`
		_, err = tx.Run(ctx, queryDS, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan DataSources: %w", err)
		}

		// Events
		queryEv := `
			MATCH (e:Event)
			WHERE NOT (:Trigger)-[:EVENT_LINK]->(e)
			DETACH DELETE e
		`
		_, err = tx.Run(ctx, queryEv, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Events: %w", err)
		}

		// Entrypoints
		queryEntrypoints := `
			MATCH (ep:Entrypoint)
			WHERE NOT (ep)-[:DESTINATION]->(:StoredProcedure)
			AND NOT (ep)-[:DESTINATION]->(:Trigger)
			DETACH DELETE ep
		`
		_, err = tx.Run(ctx, queryEntrypoints, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Entrypoints: %w", err)
		}

		// Labels
		queryOtherLabels := `
			MATCH (l:Label)
			WHERE NOT (l)<-[:HAS_LABEL]-(:StoredProcedure)
			AND NOT (l)<-[:HAS_LABEL]-(:DataSource)
			AND NOT (l)<-[:HAS_LABEL]-(:Trigger)
			AND NOT (l)<-[:HAS_LABEL]-(:Event)
			AND NOT (l)<-[:HAS_LABEL]-(:Chart)
			AND NOT (l)<-[:HAS_LABEL]-(:Entrypoint)
			DETACH DELETE l
		`
		_, err = tx.Run(ctx, queryOtherLabels, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete other Labels: %w", err)
		}

		return nil, nil
	})

	return err
}
