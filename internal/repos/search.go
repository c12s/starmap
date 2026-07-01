package repos

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) Search(ctx context.Context, name, description string, tags map[string]string, deepSearch bool, componentTags map[string]string) (*domain.GetChartsLabelsResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		params := map[string]any{
			"name":        name,
			"description": description,
		}

		// Chart tag filter (na v.tags JSON - per version)
		chartTagFilter := ""
		if len(tags) > 0 {
			tagsBytes, _ := json.Marshal(tags)
			params["chartTags"] = string(tagsBytes)

			chartTagFilter = `
			AND ALL(key IN keys(apoc.convert.fromJsonMap($chartTags))
				WHERE apoc.convert.fromJsonMap(v.tags)[key] = apoc.convert.fromJsonMap($chartTags)[key]
			)`
		}

		// Component tag filter
		spCompFilter := ""
		trigCompFilter := ""
		evCompFilter := ""
		dsCompFilter := ""

		if deepSearch && len(componentTags) > 0 {
			compTagsBytes, _ := json.Marshal(componentTags)
			params["compTags"] = string(compTagsBytes)

			spCompFilter = `WHERE sp IS NULL OR ($compTags = '{}' OR ALL(key IN keys(apoc.convert.fromJsonMap($compTags)) WHERE apoc.convert.fromJsonMap(spRels.tags)[key] = apoc.convert.fromJsonMap($compTags)[key]))`
			trigCompFilter = `WHERE t IS NULL OR ($compTags = '{}' OR ALL(key IN keys(apoc.convert.fromJsonMap($compTags)) WHERE apoc.convert.fromJsonMap(tRels.tags)[key] = apoc.convert.fromJsonMap($compTags)[key]))`
			evCompFilter = `WHERE e IS NULL OR ($compTags = '{}' OR ALL(key IN keys(apoc.convert.fromJsonMap($compTags)) WHERE apoc.convert.fromJsonMap(eRels.tags)[key] = apoc.convert.fromJsonMap($compTags)[key]))`

			dsCompFilter = `WHERE ds IS NULL OR ($compTags = '{}' OR ALL(key IN keys(apoc.convert.fromJsonMap($compTags)) WHERE apoc.convert.fromJsonMap(ds.tags)[key] = apoc.convert.fromJsonMap($compTags)[key]))`
		}

		query := fmt.Sprintf(`
			MATCH (u:User)-[:HAS_NAMESPACE]->(n:Namespace)-[nsRel:HAS_CHART]->(c:Chart)
			WHERE ($name = '' OR toLower(c.name) CONTAINS toLower($name))
			AND ($description = '' OR toLower(c.description) CONTAINS toLower($description))
			WITH u.name AS maintainer, n.name AS namespace, c, nsRel.versions AS userVersions
			WHERE size(userVersions) > 0

			OPTIONAL MATCH (c)-[:HAS_LABEL]->(l:Label)
			WITH maintainer, namespace, c, userVersions,
				collect({key: l.key, value: l.value}) AS labels

			UNWIND userVersions AS versionStr

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v_direct:Version {schemaVersion: versionStr})
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v_extend:Version {schemaVersion: versionStr})
			WITH maintainer, namespace, c, labels,
				CASE WHEN v_direct IS NOT NULL THEN v_direct ELSE v_extend END AS v

			WHERE v IS NOT NULL
			%s

			OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
			WITH maintainer, namespace, c, labels, v, collect(DISTINCT base) AS versions

			UNWIND versions AS ver
			OPTIONAL MATCH (ver)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			%s
			WITH maintainer, namespace, c, labels, v, versions,
				collect(DISTINCT {nodeProps: properties(sp), relProps: properties(spRels)}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds:DataSource)<-[rel:HARD_LINK|SOFT_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			%s
			WITH maintainer, namespace, c, labels, v, versions, storedProcedures,
				collect(DISTINCT {node: ds, relProps: properties(rel)}) AS spDataSources

			WITH maintainer, namespace, c, labels, v, versions, storedProcedures,
				collect({dataSources: spDataSources}) AS spDataSourcesList

			UNWIND versions AS ver
			OPTIONAL MATCH (ver)-[tRels:HAS_TRIGGER]->(t:Trigger)
			%s
			WITH maintainer, namespace, c, labels, v, versions, storedProcedures, spDataSourcesList,
				collect(DISTINCT {nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds:DataSource)<-[rel:HARD_LINK|SOFT_LINK]-(trNode)
			%s
			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			%s
			WITH maintainer, namespace, c, v, labels, storedProcedures, spDataSourcesList, triggers, tr, trNode,
				collect(DISTINCT {node: ds, relProps: properties(rel)}) AS triggerDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS triggerEvents

			WITH maintainer, namespace, c, labels, storedProcedures, v, spDataSourcesList,
				collect({trigger: tr, dataSources: triggerDataSources, events: triggerEvents}) AS triggersWithData

			WITH maintainer, namespace, c, labels, storedProcedures, v,
				[tr IN triggersWithData | tr.trigger] AS triggers,
				apoc.coll.flatten([tr IN triggersWithData | tr.events]) AS allEvents,
				apoc.coll.flatten([tr IN triggersWithData | tr.dataSources] + spDataSourcesList) AS allDataSources

			UNWIND (storedProcedures + triggers) AS tgt
			OPTIONAL MATCH (ep:Entrypoint)-[destRel:DESTINATION]->(tgtN {id: tgt.nodeProps.id})
			WITH maintainer, namespace, c, labels, storedProcedures, triggers, allEvents, allDataSources, v,
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
			WITH maintainer, namespace, c, labels, storedProcedures, triggers, allEvents, allDataSources, v, entrypoints,
				collect(CASE WHEN ent.id IN [sp IN storedProcedures | sp.nodeProps.id]
					THEN {id: ent.id, key: lab.key, value: lab.value} END) AS spLabels,
				collect(CASE WHEN ent.id IN [tr IN triggers | tr.nodeProps.id]
					THEN {id: ent.id, key: lab.key, value: lab.value} END) AS triggerLabels,
				collect(CASE WHEN ent.id IN [ev IN allEvents | ev.nodeProps.id]
					THEN {id: ent.id, key: lab.key, value: lab.value} END) AS eventLabels,
				collect(CASE WHEN ent.id IN [ds IN allDataSources | ds.node.id]
					THEN {id: ent.id, key: lab.key, value: lab.value} END) AS dataSourceLabels,
				collect(CASE WHEN ent.id IN [ep IN entrypoints | ep.nodeProps.id] THEN { id: ent.id, key: lab.key, value: lab.value } END) AS entrypointLabels

			RETURN maintainer, namespace, c, labels, v,
				storedProcedures, triggers,
				allEvents AS events,
				allDataSources AS dataSources,
				spLabels, triggerLabels, eventLabels, dataSourceLabels, entrypointLabels, entrypoints
		`, chartTagFilter, spCompFilter, dsCompFilter, trigCompFilter, dsCompFilter, evCompFilter)

		rec, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("search query failed: %w", err)
		}

		charts := &domain.GetChartsLabelsResp{
			Charts: []domain.GetChartMetadataResp{},
		}

		for rec.Next(ctx) {
			record := rec.Record()

			chart := domain.GetChartMetadataResp{
				DataSources:      make(map[string]*domain.DataSource),
				StoredProcedures: make(map[string]*domain.StoredProcedure),
				EventTriggers:    make(map[string]*domain.EventTrigger),
				Events:           make(map[string]*domain.Event),
			}

			if v, ok := record.Get("c"); ok {
				if node, ok := v.(neo4j.Node); ok {
					chart.Metadata.Id = getStringProp(node, "id")
					chart.Metadata.Name = getStringProp(node, "name")
					chart.ApiVersion = getStringProp(node, "apiVersion")
					chart.Metadata.Description = getStringProp(node, "description")
					chart.Metadata.Visibility = getStringProp(node, "visibility")
					chart.Metadata.Engine = getStringProp(node, "engine")
				}
			}

			if v, ok := record.Get("maintainer"); ok {
				if m, ok := v.(string); ok {
					chart.Metadata.Maintainer = m
				}
			}

			if v, ok := record.Get("namespace"); ok {
				if ns, ok := v.(string); ok {
					chart.Metadata.Namespace = ns
				}
			}

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

			charts.Charts = append(charts.Charts, chart)
		}

		if err := rec.Err(); err != nil {
			return nil, fmt.Errorf("iteration error: %w", err)
		}

		return charts, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetChartsLabelsResp), nil
}
