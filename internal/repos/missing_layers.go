package repos

import (
	"context"
	"fmt"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) GetMissingLayers(ctx context.Context, schemaVersion, namespace, maintainer, chartId string, layers []string) (*domain.GetMissingLayers, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	if namespace == "" {
		namespace = "default"
	}
	useLatest := schemaVersion == ""

	var versionMatch string

	if useLatest {
		versionMatch = `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[nsRel:HAS_CHART]->(c)
			WITH c, nsRel.versions AS allowedVersions
			WHERE size(allowedVersions) > 0

			OPTIONAL MATCH (c)-[r:HAS_VERSION]->(root:Version)
			WHERE root.schemaVersion IN allowedVersions
			OPTIONAL MATCH (root)<-[re:EXTEND*0..]-(v:Version)
			WHERE v.schemaVersion IN allowedVersions

			WITH c, v,
				CASE
					WHEN re IS NULL OR size(re) = 0 THEN r.createdAt
					ELSE last(re).createdAt
				END AS versionCreatedAt

			ORDER BY versionCreatedAt DESC
			LIMIT 1

			OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
			WITH c, v, collect(DISTINCT base) AS versions
		`
	} else {
		versionMatch = `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[nsRel:HAS_CHART]->(c)
			WHERE $schemaVersion IN nsRel.versions

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v_direct:Version {schemaVersion: $schemaVersion})
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v_extend:Version {schemaVersion: $schemaVersion})

			WITH c,
				CASE WHEN v_direct IS NOT NULL THEN v_direct ELSE v_extend END AS v

			OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
			WITH c, v, collect(DISTINCT base) AS versions
		`
	}

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		query := fmt.Sprintf(`
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $chartId})

			%s

			UNWIND versions AS ver
			OPTIONAL MATCH (ver)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, v, versions,
				collect(DISTINCT {
					nodeProps: properties(sp),
					relProps: properties(spRels)
				}) AS storedProcedures

			UNWIND versions AS ver
			OPTIONAL MATCH (ver)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, v, versions, storedProcedures,
				collect(DISTINCT {
					nodeProps: properties(t),
					relProps: properties(tRels)
				}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (trNode:Trigger {id: tr.nodeProps.id})-[eRels:EVENT_LINK]->(e:Event)
			WITH c, v, storedProcedures, triggers,
				collect({
					nodeProps: properties(e),
					relProps: properties(eRels)
				}) AS events

			UNWIND (storedProcedures + triggers) AS tgt
			OPTIONAL MATCH (ep:Entrypoint)-[destRel:DESTINATION]->(tgtN {id: tgt.nodeProps.id})
			WITH c, v, storedProcedures, triggers, events,
				collect(DISTINCT CASE WHEN ep IS NOT NULL THEN {
					nodeProps: properties(ep),
					relProps: properties(destRel),
					destination: tgt.relProps.name
				} END) AS entrypoints

			WITH c, v, entrypoints,
				[sp IN storedProcedures WHERE NOT sp.nodeProps.hash IN $layers] AS missingStoredProcedures,
				[t IN triggers WHERE NOT t.nodeProps.triggerEventHash IN $layers] AS missingTriggers,
				[e IN events WHERE NOT e.nodeProps.hash IN $layers] AS missingEvents

			OPTIONAL MATCH (ent)-[:HAS_LABEL]->(lab)
			WHERE ent.id IN (
				[sp IN missingStoredProcedures | sp.nodeProps.id] +
				[tr IN missingTriggers | tr.nodeProps.id] +
				[ev IN missingEvents | ev.nodeProps.id] +
				[ep IN entrypoints | ep.nodeProps.id]
			)
			WITH c, v, missingStoredProcedures, missingTriggers, missingEvents, entrypoints,
				collect(CASE
					WHEN ent.id IN [sp IN missingStoredProcedures | sp.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS spLabels,
				
				collect(CASE 
					WHEN ent.id IN [tr IN missingTriggers | tr.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS triggerLabels,

				collect(CASE 
					WHEN ent.id IN [ev IN missingEvents | ev.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS eventLabels,
				collect(CASE WHEN ent.id IN [ep IN entrypoints | ep.nodeProps.id] THEN { id: ent.id, key: lab.key, value: lab.value } END) AS entrypointLabels

			RETURN c, v,
				missingStoredProcedures AS storedProcedures,
				missingEvents AS events,
				missingTriggers AS triggers, entrypoints,
				spLabels, triggerLabels, eventLabels, entrypointLabels
		`, versionMatch)

		rec, err := tx.Run(ctx, query, map[string]any{
			"maintainer":    maintainer,
			"namespace":     namespace,
			"chartId":       chartId,
			"layers":        layers,
			"schemaVersion": schemaVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		if !rec.Next(ctx) {
			return nil, fmt.Errorf("chart not found")
		}

		record := rec.Record()

		resp := &domain.GetMissingLayers{
			Metadata: struct {
				Id            string
				Name          string
				Namespace     string
				ApiVersion    string
				SchemaVersion string
			}{
				Id:        chartId,
				Name:      maintainer,
				Namespace: namespace,
			},

			DataSources:      nil,
			StoredProcedures: make(map[string]*domain.StoredProcedure),
			EventTriggers:    make(map[string]*domain.EventTrigger),
			Events:           make(map[string]*domain.Event),
			Entrypoint:       make(map[string]*domain.Entrypoint),
		}

		// apiVersion
		if v, ok := record.Get("c"); ok {
			if node, ok := v.(neo4j.Node); ok {
				resp.Metadata.ApiVersion = getStringProp(node, "apiVersion")
			}
		}

		// Version node
		if v, ok := record.Get("v"); ok {
			if node, ok := v.(neo4j.Node); ok {
				resp.Metadata.SchemaVersion = getStringProp(node, "schemaVersion")
			}
		}

		// StoredProcedures
		if v, ok := record.Get("storedProcedures"); ok {
			labels, _ := record.Get("spLabels")
			spParsed := parseStoredProcedures(ctx, tx, v, parseLabelsIntoMap(labels))
			for key, sp := range spParsed {
				resp.StoredProcedures[key] = sp
			}
		}

		// Events
		if v, ok := record.Get("events"); ok {
			labels, _ := record.Get("eventLabels")
			evParsed := parseEvents(v, parseLabelsIntoMap(labels))
			for key, ev := range evParsed {
				resp.Events[key] = ev
			}
		}

		// Triggers
		if v, ok := record.Get("triggers"); ok {
			labels, _ := record.Get("triggerLabels")
			trParsed := parseTriggers(ctx, tx, v, parseLabelsIntoMap(labels))
			for key, tr := range trParsed {
				resp.EventTriggers[key] = tr
			}
		}

		// Entrypoints
		if v, ok := record.Get("entrypoints"); ok {
			labels, _ := record.Get("entrypointLabels")
			epParsed := parseEntrypoints(v, parseLabelsIntoMap(labels))
			for key, ep := range epParsed {
				resp.Entrypoint[key] = ep
			}
		}

		return resp, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetMissingLayers), nil
}

// GET Missing Layers with Data Source Nodes

// func (r *RegistryRepo) GetMissingLayers(ctx context.Context, namespace, maintainer, chartId string, layers []string) (*domain.GetMissingLayers, error) {
// 	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
// 		AccessMode: neo4j.AccessModeRead,
// 	})
// 	defer session.Close(ctx)

// 	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

// 		query := `
// 			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $chartId})

// 			OPTIONAL MATCH (c)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
// 			WITH c, collect({
// 				nodeProps: properties(sp),
// 				relProps: properties(spRels)
// 			}) AS storedProcedures

// 			UNWIND storedProcedures AS sp
// 			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
// 			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
// 			WITH c, storedProcedures, sp, [ds1, ds2] AS spDataSources

// 			OPTIONAL MATCH (c)-[tRels:HAS_TRIGGER]->(t:Trigger)
// 			WITH c, storedProcedures, collect(spDataSources) AS spDataSourcesList,
// 				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

// 			UNWIND triggers AS tr
// 			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
// 			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
// 			WITH c, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

// 			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
// 			WITH c, storedProcedures, triggers,
// 				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
// 				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events

// 			WITH c,
// 				[ds IN allDataSources WHERE NOT ds.hash IN $layers] AS missingDataSources,
// 				[sp IN storedProcedures WHERE NOT sp.nodeProps.hash IN $layers] AS missingStoredProcedures,
// 				[t IN triggers WHERE NOT t.nodeProps.hash IN $layers] AS missingTriggers,
// 				[e IN events WHERE NOT e.nodeProps.hash IN $layers] AS missingEvents
// 			RETURN c,
// 				missingDataSources AS dataSources,
// 				missingStoredProcedures AS storedProcedures,
// 				missingEvents AS events,
// 				missingTriggers AS triggers
// 		`

// 		rec, err := tx.Run(ctx, query, map[string]any{
// 			"maintainer": maintainer,
// 			"namespace":  namespace,
// 			"chartId":    chartId,
// 			"layers":     layers,
// 		})
// 		if err != nil {
// 			return nil, fmt.Errorf("query failed: %w", err)
// 		}

// 		if !rec.Next(ctx) {
// 			return nil, fmt.Errorf("chart not found")
// 		}

// 		record := rec.Record()

// 		resp := &domain.GetMissingLayers{
// 			Metadata: struct {
// 				Id        string
// 				Name      string
// 				Namespace string
// 			}{
// 				Id:        chartId,
// 				Name:      maintainer,
// 				Namespace: namespace,
// 			},
// 			DataSources:      make(map[string]*domain.DataSource),
// 			StoredProcedures: make(map[string]*domain.StoredProcedure),
// 			EventTriggers:    make(map[string]*domain.EventTrigger),
// 			Events:           make(map[string]*domain.Event),
// 		}

// 		// DataSources
// 		if v, ok := record.Get("dataSources"); ok {
// 			dsParsed := parseDataSources(v)
// 			for key, ds := range dsParsed {
// 				resp.DataSources[key] = ds
// 			}
// 		}

// 		// StoredProcedures
// 		if v, ok := record.Get("storedProcedures"); ok {
// 			spParsed := parseStoredProcedures(ctx, tx, v)
// 			for key, sp := range spParsed {
// 				resp.StoredProcedures[key] = sp
// 			}
// 		}

// 		// Events
// 		if v, ok := record.Get("events"); ok {
// 			evParsed := parseEvents(v)
// 			for key, ev := range evParsed {
// 				resp.Events[key] = ev
// 			}
// 		}

// 		// EventTriggers
// 		if v, ok := record.Get("triggers"); ok {
// 			trParsed := parseTriggers(ctx, tx, v)
// 			for key, tr := range trParsed {
// 				resp.EventTriggers[key] = tr
// 			}
// 		}

// 		return resp, nil
// 	})

// 	if err != nil {
// 		return nil, err
// 	}

// 	return result.(*domain.GetMissingLayers), nil
// }
