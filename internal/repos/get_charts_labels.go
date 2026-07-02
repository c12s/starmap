package repos

import (
	"context"
	"fmt"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) GetChartsLabels(ctx context.Context, schemaVersion, namespace, maintainer string, labels map[string]string) (*domain.GetChartsLabelsResp, error) {
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
			WITH c, labels, nsRel.versions AS allowedVersions
			WHERE size(allowedVersions) > 0

			WITH c, labels, last(apoc.coll.sort(allowedVersions)) AS latestAllowed

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(root:Version {schemaVersion: latestAllowed})
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(extended:Version {schemaVersion: latestAllowed})

			WITH c, labels, latestAllowed,
				CASE WHEN root IS NOT NULL THEN root ELSE extended END AS v

			OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
			WITH c, labels, v, collect(DISTINCT base) AS versions
		`
	} else {
		versionMatch = `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[nsRel:HAS_CHART]->(c)
			WHERE $schemaVersion IN nsRel.versions

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v_direct:Version {schemaVersion: $schemaVersion})
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v_extend:Version {schemaVersion: $schemaVersion})

			WITH c, labels,
				CASE WHEN v_direct IS NOT NULL THEN v_direct ELSE v_extend END AS v

			OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
			WITH c, labels, v, collect(DISTINCT base) AS versions
		`
	}

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		params := map[string]any{
			"namespace":     namespace,
			"maintainer":    maintainer,
			"schemaVersion": schemaVersion,
		}

		labelMatch := ""
		if len(labels) > 0 {
			i := 0
			for k, v := range labels {
				paramKey := fmt.Sprintf("key%d", i)
				paramVal := fmt.Sprintf("val%d", i)
				labelMatch += fmt.Sprintf(`
					MATCH (c)-[:HAS_LABEL]->(l%d:Label {key: $%s, value: $%s})
				`, i, paramKey, paramVal)
				params[paramKey] = k
				params[paramVal] = v
				i++
			}
		}

		query := fmt.Sprintf(`
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart)
			%s
			OPTIONAL MATCH (c)-[:HAS_LABEL]->(l:Label)
			WITH c, collect({key: l.key, value: l.value}) AS labels
			%s

			UNWIND versions AS ver
			OPTIONAL MATCH (ver)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, v, labels, versions,
				collect(DISTINCT {
					nodeProps: properties(sp),
					relProps: properties(spRels)
				}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, v, labels, storedProcedures, sp, versions, 
				collect(DISTINCT ds1) + collect(DISTINCT ds2) AS spDataSources

			WITH c, v, labels, storedProcedures, versions, 
				collect({
					trigger: sp,
					dataSources: spDataSources
				}) AS spWithDataSources

			WITH c, v, labels, storedProcedures, versions,
				[sp IN spWithDataSources | sp.dataSources] AS spDataSourcesList

			UNWIND versions AS ver
			OPTIONAL MATCH (ver)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, v, labels, versions, storedProcedures, spDataSourcesList,
				collect(DISTINCT {
					nodeProps: properties(t),
					relProps: properties(tRels)
				}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds3:DataSource)<-[hlRel3:HARD_LINK]-(trNode)
			OPTIONAL MATCH (ds4:DataSource)<-[slRel4:SOFT_LINK]-(trNode)
			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, v, labels, storedProcedures, spDataSourcesList, triggers, tr, trNode,
				collect(DISTINCT {node: ds3, relProps: properties(hlRel3)}) +
				collect(DISTINCT {node: ds4, relProps: properties(slRel4)}) AS triggerDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS triggerEvents

			WITH c, v, labels, storedProcedures, spDataSourcesList,
				collect({
					trigger: tr,
					dataSources: triggerDataSources,
					events: triggerEvents
				}) AS triggersWithData

			WITH c, v, labels, storedProcedures,
				[tr IN triggersWithData | tr.trigger] AS triggers,
				apoc.coll.flatten([tr IN triggersWithData | tr.events]) AS allEvents,
				apoc.coll.flatten([tr IN triggersWithData | tr.dataSources] + spDataSourcesList) AS allDataSources
			
			UNWIND (storedProcedures + triggers) AS tgt
			OPTIONAL MATCH (ep:Entrypoint)-[destRel:DESTINATION]->(tgtN {id: tgt.nodeProps.id})
			WITH c, v, labels, storedProcedures, triggers, allEvents, allDataSources,
				collect(DISTINCT CASE WHEN ep IS NOT NULL THEN {
					nodeProps: properties(ep),
					relProps: properties(destRel),
					destination: tgt.relProps.name
				} END) AS entrypoints

			OPTIONAL MATCH (ent)-[:HAS_LABEL]->(lab)
			WHERE ent.id IN (
				[sp IN storedProcedures | sp.nodeProps.id] +
				[tr IN triggers | tr.nodeProps.id] +
				[ev IN allEvents | ev.nodeProps.id] +
				[ds IN allDataSources | ds.node.id] +
				[ep IN entrypoints | ep.nodeProps.id]
			)
			WITH c, v, labels, storedProcedures, triggers, allEvents, allDataSources, entrypoints,
				collect(CASE 
					WHEN ent.id IN [sp IN storedProcedures | sp.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS spLabels,
				
				collect(CASE 
					WHEN ent.id IN [tr IN triggers | tr.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS triggerLabels,

				collect(CASE 
					WHEN ent.id IN [ev IN allEvents | ev.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS eventLabels,

				collect(CASE 
					WHEN ent.id IN [ds IN allDataSources | ds.node.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value }
					END) AS dataSourceLabels,

				collect(CASE 
					WHEN ent.id IN [ep IN entrypoints | ep.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS entrypointLabels

			RETURN c, v, labels, spLabels, triggerLabels, eventLabels, dataSourceLabels, entrypointLabels,
			storedProcedures, triggers, allEvents AS events, allDataSources AS dataSources, entrypoints
		`, labelMatch, versionMatch)

		rec, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		resp := &domain.GetChartsLabelsResp{
			Charts: []domain.GetChartMetadataResp{},
		}

		for rec.Next(ctx) {
			record := rec.Record()

			if !useLatest {
				if v, ok := record.Get("v"); !ok || v == nil {
					return nil, fmt.Errorf("requested version %s not found", schemaVersion)
				}
			}

			chart := domain.GetChartMetadataResp{
				DataSources:      make(map[string]*domain.DataSource),
				StoredProcedures: make(map[string]*domain.StoredProcedure),
				EventTriggers:    make(map[string]*domain.EventTrigger),
				Events:           make(map[string]*domain.Event),
			}

			// Chart metadata
			if v, ok := record.Get("c"); ok {
				if node, ok := v.(neo4j.Node); ok {
					chart.Metadata.Id = getStringProp(node, "id")
					chart.Metadata.Name = getStringProp(node, "name")
					chart.Metadata.Namespace = namespace
					chart.Metadata.Maintainer = maintainer
					chart.ApiVersion = getStringProp(node, "apiVersion")
					chart.Metadata.Description = getStringProp(node, "description")
					chart.Metadata.Visibility = getStringProp(node, "visibility")
					chart.Metadata.Engine = getStringProp(node, "engine")
				}
			}

			// Version node
			if v, ok := record.Get("v"); ok {
				if node, ok := v.(neo4j.Node); ok {
					chart.SchemaVersion = getStringProp(node, "schemaVersion")
					chart.Metadata.Tags = parseLabels(getStringProp(node, "tags"))
					if chart.Metadata.Tags == nil {
						chart.Metadata.Tags = map[string]string{}
					}
				}
			}

			if v, ok := record.Get("labels"); ok {
				chart.Metadata.Labels = parseLabelList(v)
			}

			if chart.Metadata.Labels == nil {
				chart.Metadata.Labels = map[string]string{}
			}

			if v, ok := record.Get("dataSources"); ok {
				labels, _ := record.Get("dataSourceLabels")
				chart.DataSources = parseDataSources(v, parseLabelsIntoMap(labels))
			}

			if v, ok := record.Get("storedProcedures"); ok {
				labels, _ := record.Get("spLabels")
				chart.StoredProcedures = parseStoredProcedures(ctx, tx, v, parseLabelsIntoMap(labels))
			}

			if v, ok := record.Get("events"); ok {
				labels, _ := record.Get("eventLabels")
				chart.Events = parseEvents(v, parseLabelsIntoMap(labels))
			}

			if v, ok := record.Get("triggers"); ok {
				labels, _ := record.Get("triggerLabels")
				chart.EventTriggers = parseTriggers(ctx, tx, v, parseLabelsIntoMap(labels))
			}

			if v, ok := record.Get("entrypoints"); ok {
				labels, _ := record.Get("entrypointLabels")
				chart.Entrypoints = parseEntrypoints(v, parseLabelsIntoMap(labels))
			}

			resp.Charts = append(resp.Charts, chart)
		}

		if err := rec.Err(); err != nil {
			return nil, fmt.Errorf("iteration error: %w", err)
		}

		return resp, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetChartsLabelsResp), nil
}
