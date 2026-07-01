package repos

import (
	"context"
	"fmt"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) GetAllCharts(ctx context.Context) (*domain.GetChartsLabelsResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `
			MATCH (u:User)-[:HAS_NAMESPACE]->(n:Namespace)-[r:HAS_CHART]->(c:Chart {visibility: "public"})
			WITH u.name AS maintainer, n.name AS namespace, c, r.versions AS userVersions

			OPTIONAL MATCH (c)-[:HAS_LABEL]->(l:Label)
			WITH maintainer, namespace, c, userVersions,
				collect({key: l.key, value: l.value}) AS labels

			UNWIND userVersions AS sv

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v:Version)
			WHERE v.schemaVersion = sv
			WITH maintainer, namespace, c, sv, labels, v

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(root:Version)
			OPTIONAL MATCH (root)<-[:EXTEND*1..]-(v2:Version)
			WHERE v2.schemaVersion = sv
			WITH maintainer, namespace, c, sv, labels, v, collect(v2) AS extendedVersions

			WITH maintainer, namespace, c, labels, sv,
				CASE WHEN v IS NOT NULL THEN v ELSE head(extendedVersions) END AS ver

			OPTIONAL MATCH (ver)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH maintainer, namespace, c, labels, ver,
				collect(DISTINCT {
				nodeProps: properties(sp),
				relProps: properties(spRels)
				}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[hlRel1:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[slRel2:SOFT_LINK]-(spNode)
			WITH c, ver, labels, storedProcedures, sp, maintainer, namespace,
				collect(DISTINCT {node: ds1, relProps: properties(hlRel1)}) +
				collect(DISTINCT {node: ds2, relProps: properties(slRel2)}) AS spDataSources

			WITH maintainer, namespace, c, labels, storedProcedures, ver,
				collect({ trigger: sp, dataSources: spDataSources }) AS spWithDataSources

			WITH maintainer, namespace, c, labels, storedProcedures, ver,
				[sp IN spWithDataSources | sp.dataSources] AS spDataSourcesList

			OPTIONAL MATCH (ver)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH maintainer, namespace, c, labels, ver, storedProcedures, spDataSourcesList,
				collect(DISTINCT {
				nodeProps: properties(t),
				relProps: properties(tRels)
				}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds3:DataSource)<-[hlRel3:HARD_LINK]-(trNode)
			OPTIONAL MATCH (ds4:DataSource)<-[slRel4:SOFT_LINK]-(trNode)
			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH maintainer, namespace, c, ver, labels, storedProcedures, spDataSourcesList, triggers, tr, trNode,
				collect(DISTINCT {node: ds3, relProps: properties(hlRel3)}) +
				collect(DISTINCT {node: ds4, relProps: properties(slRel4)}) AS triggerDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS triggerEvents

			WITH maintainer, namespace, c, labels, storedProcedures, ver, spDataSourcesList,
				collect({ trigger: tr, dataSources: triggerDataSources, events: triggerEvents }) AS triggersWithData

			WITH maintainer, namespace, c, labels, storedProcedures, ver,
				[tr IN triggersWithData | tr.trigger] AS triggers,
				apoc.coll.flatten([tr IN triggersWithData | tr.events]) AS allEvents,
				apoc.coll.flatten([tr IN triggersWithData | tr.dataSources] + spDataSourcesList) AS allDataSources

			UNWIND (storedProcedures + triggers) AS tgt
			OPTIONAL MATCH (ep:Entrypoint)-[destRel:DESTINATION]->(tgtN {id: tgt.nodeProps.id})
			WITH maintainer, namespace, c, labels, storedProcedures, triggers, allEvents, allDataSources, ver,
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

			WITH maintainer, namespace, c, labels, storedProcedures, triggers, allEvents, allDataSources, ver, entrypoints,
				collect(CASE WHEN ent.id IN [sp IN storedProcedures | sp.nodeProps.id]
				THEN { id: ent.id, key: lab.key, value: lab.value } END) AS spLabels,
				collect(CASE WHEN ent.id IN [tr IN triggers | tr.nodeProps.id]
				THEN { id: ent.id, key: lab.key, value: lab.value } END) AS triggerLabels,
				collect(CASE WHEN ent.id IN [ev IN allEvents | ev.nodeProps.id]
				THEN { id: ent.id, key: lab.key, value: lab.value } END) AS eventLabels,
				collect(CASE WHEN ent.id IN [ds IN allDataSources | ds.node.id]
				THEN { id: ent.id, key: lab.key, value: lab.value } END) AS dataSourceLabels,
				collect(CASE WHEN ent.id IN [ep IN entrypoints | ep.nodeProps.id] THEN { id: ent.id, key: lab.key, value: lab.value } END) AS entrypointLabels

			RETURN maintainer, namespace, c, labels, ver,
				storedProcedures, triggers,
				allEvents AS events,
				allDataSources AS dataSources,
				spLabels, triggerLabels, eventLabels, dataSourceLabels, entrypointLabels, entrypoints
		`

		rec, err := tx.Run(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		charts := &domain.GetChartsLabelsResp{}

		for rec.Next(ctx) {
			record := rec.Record()

			chart := domain.GetChartMetadataResp{
				DataSources:      make(map[string]*domain.DataSource),
				StoredProcedures: make(map[string]*domain.StoredProcedure),
				EventTriggers:    make(map[string]*domain.EventTrigger),
				Events:           make(map[string]*domain.Event),
			}

			// Chart node (metadata)
			if v, ok := record.Get("c"); ok {
				if node, ok := v.(neo4j.Node); ok {
					chart.Metadata.Name = getStringProp(node, "name")
					chart.Metadata.Id = getStringProp(node, "id")
					chart.ApiVersion = getStringProp(node, "apiVersion")
					chart.Metadata.Description = getStringProp(node, "description")
					chart.Metadata.Visibility = getStringProp(node, "visibility")
					chart.Metadata.Engine = getStringProp(node, "engine")
					chart.Metadata.Labels = parseLabels(getStringProp(node, "labels"))
					if chart.Metadata.Labels == nil {
						chart.Metadata.Labels = map[string]string{}
					}
				}
			}

			// Maintainer
			if v, ok := record.Get("maintainer"); ok {
				if maintainer, ok := v.(string); ok {
					chart.Metadata.Maintainer = maintainer
				}
			}

			// Namespace
			if v, ok := record.Get("namespace"); ok {
				if namespace, ok := v.(string); ok {
					chart.Metadata.Namespace = namespace
				}
			}

			// Version
			if v, ok := record.Get("ver"); ok {
				if node, ok := v.(neo4j.Node); ok {
					chart.SchemaVersion = getStringProp(node, "schemaVersion")
					chart.Metadata.Tags = parseLabels(getStringProp(node, "tags"))
					if chart.Metadata.Tags == nil {
						chart.Metadata.Tags = map[string]string{}
					}
				}
			}

			// Labels
			if v, ok := record.Get("labels"); ok {
				chart.Metadata.Labels = parseLabelList(v)
			}
			if chart.Metadata.Labels == nil {
				chart.Metadata.Labels = map[string]string{}
			}

			// DataSources
			if v, ok := record.Get("dataSources"); ok {
				labels, _ := record.Get("dataSourceLabels")
				chart.DataSources = parseDataSources(v, parseLabelsIntoMap(labels))
			}

			// StoredProcedures
			if v, ok := record.Get("storedProcedures"); ok {
				labels, _ := record.Get("spLabels")
				chart.StoredProcedures = parseStoredProcedures(ctx, tx, v, parseLabelsIntoMap(labels))
			}

			// Events
			if v, ok := record.Get("events"); ok {
				labels, _ := record.Get("eventLabels")
				chart.Events = parseEvents(v, parseLabelsIntoMap(labels))
			}

			// Triggers
			if v, ok := record.Get("triggers"); ok {
				labels, _ := record.Get("triggerLabels")
				chart.EventTriggers = parseTriggers(ctx, tx, v, parseLabelsIntoMap(labels))
			}

			// Entrypoints
			if v, ok := record.Get("entrypoints"); ok {
				labels, _ := record.Get("entrypointLabels")
				chart.Entrypoints = parseEntrypoints(v, parseLabelsIntoMap(labels))
			}

			charts.Charts = append(charts.Charts, chart)
		}

		if err := rec.Err(); err != nil {
			return nil, fmt.Errorf("error iterating records: %w", err)
		}

		return charts, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetChartsLabelsResp), nil
}
