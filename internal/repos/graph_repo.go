package repos

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/c12s/starmap/internal/config"
	"github.com/c12s/starmap/internal/domain"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type RegistryRepo struct {
	driver neo4j.DriverWithContext
}

func NewRegistryRepo() (*RegistryRepo, error) {
	cfg := config.GetConfig()

	driver, err := neo4j.NewDriverWithContext(cfg.NEO4J_uri, neo4j.BasicAuth(cfg.NEO4J_user, cfg.NEO4J_pass, ""))
	if err != nil {
		return nil, fmt.Errorf("failed to create Neo4j driver: %w", err)
	}

	err = driver.VerifyConnectivity(context.Background())
	if err != nil {
		_ = driver.Close(context.Background())
		return nil, fmt.Errorf("failed to connect to Neo4j: %w", err)
	}

	log.Println("Connected to Neo4j:", cfg.NEO4J_uri)

	return &RegistryRepo{driver: driver}, nil
}

func (r *RegistryRepo) Close() {
	if r.driver != nil {
		_ = r.driver.Close(context.Background())
		log.Println("Closed Neo4j connection")
	}
}

func (r *RegistryRepo) PutChart(ctx context.Context, chart domain.StarChart) (*domain.MetadataResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	if chart.ApiVersion == "" {
		chart.ApiVersion = "v1.0.0"
	}
	if chart.SchemaVersion == "" {
		chart.SchemaVersion = "v1.0.0"
	}

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		// Namespace Node
		queryNamespace := `
			MERGE (n:Namespace {name: $namespace})
		`
		_, err := tx.Run(ctx, queryNamespace, map[string]any{
			"namespace": chart.Metadata.Namespace,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Namespace node: %w", err)
		}

		// User Node
		queryUser := `
			MERGE (u:User {name: $maintainer})
			WITH u
			MATCH (n:Namespace {name: $namespace})
			MERGE (u)-[:HAS_NAMESPACE]->(n)
		`
		_, err = tx.Run(ctx, queryUser, map[string]any{
			"maintainer": chart.Metadata.Maintainer,
			"namespace":  chart.Metadata.Namespace,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create User node: %w", err)
		}

		// Chart Node
		queryChart := `
			MERGE (c:Chart {id: $id})
			SET c.name = $name,
				c.kind = $kind,
				c.description = $description,
				c.visibility = $visibility,
				c.engine = $engine
			WITH c
			MATCH (n:Namespace {name: $namespace})
			MERGE (n)-[:HAS_CHART]->(c)
		`
		_, err = tx.Run(ctx, queryChart, map[string]any{
			"id":          chart.Metadata.Id,
			"name":        chart.Metadata.Name,
			"kind":        chart.Kind,
			"description": chart.Metadata.Description,
			"visibility":  chart.Metadata.Visibility,
			"engine":      chart.Metadata.Engine,
			"namespace":   chart.Metadata.Namespace,
			"maintainer":  chart.Metadata.Maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Chart node: %w", err)
		}

		// ChartLabels
		labelsList := convertLabelsToList(chart.Metadata.Labels)
		if len(labelsList) > 0 {
			queryLabels := `
				MATCH (c:Chart {id: $id})
				UNWIND $labels AS lbl
				MERGE (l:Label {key: lbl.key, value: lbl.value})
				MERGE (c)-[:HAS_LABEL]->(l)
			`
			_, err = tx.Run(ctx, queryLabels, map[string]any{
				"id":     chart.Metadata.Id,
				"labels": labelsList,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to link labels to Chart: %w", err)
			}
		}

		// Version
		queryVersion := `
		MATCH (c:Chart {id: $id})
		MERGE (v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
		MERGE (c)-[r:HAS_VERSION]->(v)
		ON CREATE SET r.createdAt = $now
		ON MATCH SET r.createdAt = $now
		`
		_, err = tx.Run(ctx, queryVersion, map[string]any{
			"id":            chart.Metadata.Id,
			"apiVersion":    chart.ApiVersion,
			"schemaVersion": chart.SchemaVersion,
			"now":           time.Now().Unix(),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to link labels to Chart: %w", err)
		}

		// DataSources
		for key, ds := range chart.Chart.DataSources {

			ds.Hash = computeHash(ds.Type + ds.Path)

			queryDS := `
				MERGE (d:DataSource {hash: $hash})
				ON CREATE SET 
					d.id = $id,
					d.name = $name,
					d.type = $type,
					d.path = $path,
					d.hash = $hash,
					d.resourceName = $resourceName,
					d.description = $description
			`
			_, err := tx.Run(ctx, queryDS, map[string]any{
				"id":           ds.Id,
				"name":         ds.Name,
				"type":         ds.Type,
				"path":         ds.Path,
				"hash":         ds.Hash,
				"resourceName": ds.ResourceName,
				"description":  ds.Description,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create DataSource node for %s: %w", key, err)
			}

			// Data Source Labels
			labelsList := convertLabelsToList(ds.Labels)
			if len(labelsList) > 0 {
				queryLabels := `
				MATCH (ds:DataSource {id: $id})
				UNWIND $labels AS lbl
				MERGE (l:Label {key: lbl.key, value: lbl.value})
				MERGE (ds)-[:HAS_LABEL]->(l)
			`
				_, err = tx.Run(ctx, queryLabels, map[string]any{
					"id":     ds.Id,
					"labels": labelsList,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to link labels to Data Source: %w", err)
				}
			}
		}

		// StoredProcedures
		for key, sp := range chart.Chart.StoredProcedures {

			sp.Metadata.Hash = computeHash(sp.Metadata.Image)

			querySP := `
				MERGE (s:StoredProcedure {hash: $hash})
				ON CREATE SET
					s.id = $id
				WITH s
				MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
				WITH s, v
				MERGE (v)-[r:HAS_PROCEDURE]->(s)
				SET 
					r.name = $name,
					r.image = $image,
					r.prefix = $prefix,
					r.topic = $topic,
					r.disableVirtualization = $disableVirtualization,
					r.runDetached = $runDetached,
					r.removeOnStop = $removeOnStop,
					r.memory = $memory,
					r.kernelArgs = $kernelArgs,
					r.networks = $networks,
					r.ports = $ports,
					r.volumes = $volumes,
					r.targets = $targets,
					r.envVars = $envVars
			`
			_, err := tx.Run(ctx, querySP, map[string]any{
				"id":                    sp.Metadata.Id,
				"hash":                  sp.Metadata.Hash,
				"name":                  sp.Metadata.Name,
				"image":                 sp.Metadata.Image,
				"prefix":                sp.Metadata.Prefix,
				"topic":                 sp.Metadata.Topic,
				"disableVirtualization": sp.Control.DisableVirtualization,
				"runDetached":           sp.Control.RunDetached,
				"removeOnStop":          sp.Control.RemoveOnStop,
				"memory":                sp.Control.Memory,
				"kernelArgs":            sp.Control.KernelArgs,
				"networks":              sp.Features.Networks,
				"ports":                 sp.Features.Ports,
				"volumes":               sp.Features.Volumes,
				"targets":               sp.Features.Targets,
				"envVars":               sp.Features.EnvVars,
				"apiVersion":            chart.ApiVersion,
				"schemaVersion":         chart.SchemaVersion,
				"chartId":               chart.Metadata.Id,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create StoredProcedure relation for %s: %w", key, err)
			}

			for _, hardLink := range sp.Links.HardLinks {
				queryLink := `
					MATCH (sp:StoredProcedure {id: $spId})
					MATCH (ds:DataSource {name: $dsName})
					MERGE (sp)-[:HARD_LINK]->(ds)
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"spId":   sp.Metadata.Id,
					"dsName": hardLink,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create hard link for %s: %w", key, err)
				}
			}

			for _, softLink := range sp.Links.SoftLinks {
				queryLink := `
					MATCH (sp:StoredProcedure {id: $spId})
					MATCH (ds:DataSource {name: $dsName})
					MERGE (sp)-[:SOFT_LINK]->(ds)
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"spId":   sp.Metadata.Id,
					"dsName": softLink,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create soft link for %s: %w", key, err)
				}
			}

			// Stored Procedure Labels
			labelsList := convertLabelsToList(sp.Metadata.Labels)
			if len(labelsList) > 0 {
				queryLabels := `
				MATCH (sp:StoredProcedure {id: $id})
				UNWIND $labels AS lbl
				MERGE (l:Label {key: lbl.key, value: lbl.value})
				MERGE (sp)-[:HAS_LABEL]->(l)
			`
				_, err = tx.Run(ctx, queryLabels, map[string]any{
					"id":     sp.Metadata.Id,
					"labels": labelsList,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to link labels to Stored Procedure: %w", err)
				}
			}
		}

		eventMap := map[string]*domain.Event{}
		for _, ev := range chart.Chart.Events {
			ev.Metadata.Hash = computeHash(ev.Metadata.Image)
			eventMap[ev.Metadata.Name] = ev
		}

		// EventTriggers
		for key, et := range chart.Chart.EventTriggers {

			et.Metadata.Hash = computeHash(et.Metadata.Image)

			queryET := `
				MERGE (t:Trigger {hash: $hash})
				ON CREATE SET
					t.id = $id
				WITH t
				MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
				WITH t, v
				MERGE (v)-[r:HAS_TRIGGER]->(t)
				SET
					r.name = $name,
					r.image = $image,
					r.hash = $hash,
					r.prefix = $prefix,
					r.topic = $topic,
					r.disableVirtualization = $disableVirtualization,
					r.runDetached = $runDetached,
					r.removeOnStop = $removeOnStop,
					r.memory = $memory,
					r.kernelArgs = $kernelArgs,
					r.networks = $networks,
					r.ports = $ports,
					r.volumes = $volumes,
					r.targets = $targets,
					r.envVars = $envVars
			`
			_, err := tx.Run(ctx, queryET, map[string]any{
				"id":                    et.Metadata.Id,
				"name":                  et.Metadata.Name,
				"image":                 et.Metadata.Image,
				"hash":                  et.Metadata.Hash,
				"prefix":                et.Metadata.Prefix,
				"topic":                 et.Metadata.Topic,
				"disableVirtualization": et.Control.DisableVirtualization,
				"runDetached":           et.Control.RunDetached,
				"removeOnStop":          et.Control.RemoveOnStop,
				"memory":                et.Control.Memory,
				"kernelArgs":            et.Control.KernelArgs,
				"networks":              et.Features.Networks,
				"ports":                 et.Features.Ports,
				"volumes":               et.Features.Volumes,
				"targets":               et.Features.Targets,
				"envVars":               et.Features.EnvVars,
				"apiVersion":            chart.ApiVersion,
				"schemaVersion":         chart.SchemaVersion,
				"chartId":               chart.Metadata.Id,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create EventTrigger node for %s: %w", key, err)
			}

			for _, hardLink := range et.Links.HardLinks {
				queryLink := `
					MATCH (t:Trigger {id: $triggerId})
					MATCH (ds:DataSource {name: $dsName})
					MERGE (t)-[:HARD_LINK]->(ds)
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"triggerId": et.Metadata.Id,
					"dsName":    hardLink,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create trigger data hard link for %s: %w", key, err)
				}
			}

			for _, softLink := range et.Links.SoftLinks {
				queryLink := `
					MATCH (t:Trigger {id: $triggerId})
					MATCH (ds:DataSource {name: $dsName})
					MERGE (t)-[:SOFT_LINK]->(ds)
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"triggerId": et.Metadata.Id,
					"dsName":    softLink,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create trigger data soft link for %s: %w", key, err)
				}
			}

			// Trigger Labels
			labelsList := convertLabelsToList(et.Metadata.Labels)
			if len(labelsList) > 0 {
				queryLabels := `
				MATCH (et:Trigger {id: $id})
				UNWIND $labels AS lbl
				MERGE (l:Label {key: lbl.key, value: lbl.value})
				MERGE (et)-[:HAS_LABEL]->(l)
			`
				_, err = tx.Run(ctx, queryLabels, map[string]any{
					"id":     et.Metadata.Id,
					"labels": labelsList,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to link labels to Trigger: %w", err)
				}
			}

			for _, eventName := range et.Links.EventLinks {
				ev := eventMap[eventName]

				queryLink := `
					MERGE (e:Event {hash: $eventHash})
					ON CREATE SET
						e.id = $eventId,
						e.hash = $eventHash
					WITH e
					MATCH (t:Trigger {id: $triggerId})
					MERGE (t)-[r:EVENT_LINK]->(e)
					SET
						r.name = $name,
						r.image = $image,
						r.prefix = $prefix,
						r.topic = $topic,
						r.disableVirtualization = $disableVirtualization,
						r.runDetached = $runDetached,
						r.removeOnStop = $removeOnStop,
						r.memory = $memory,
						r.kernelArgs = $kernelArgs,
						r.networks = $networks,
						r.ports = $ports,
						r.volumes = $volumes,
						r.targets = $targets,
						r.envVars = $envVars
				`

				_, err := tx.Run(ctx, queryLink, map[string]any{
					"triggerId": et.Metadata.Id,
					"eventId":   ev.Metadata.Id,
					"eventHash": ev.Metadata.Hash,

					"name":                  ev.Metadata.Name,
					"image":                 ev.Metadata.Image,
					"hash":                  ev.Metadata.Hash,
					"prefix":                ev.Metadata.Prefix,
					"topic":                 ev.Metadata.Topic,
					"disableVirtualization": ev.Control.DisableVirtualization,
					"runDetached":           ev.Control.RunDetached,
					"removeOnStop":          ev.Control.RemoveOnStop,
					"memory":                ev.Control.Memory,
					"kernelArgs":            ev.Control.KernelArgs,
					"networks":              ev.Features.Networks,
					"ports":                 ev.Features.Ports,
					"volumes":               ev.Features.Volumes,
					"targets":               ev.Features.Targets,
					"envVars":               ev.Features.EnvVars,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create trigger event link for %s: %w", eventName, err)
				}

				// Event Labels
				labelsList := convertLabelsToList(ev.Metadata.Labels)
				if len(labelsList) > 0 {
					queryLabels := `
				MATCH (ev:Event {id: $id})
				UNWIND $labels AS lbl
				MERGE (l:Label {key: lbl.key, value: lbl.value})
				MERGE (ev)-[:HAS_LABEL]->(l)
			`
					_, err = tx.Run(ctx, queryLabels, map[string]any{
						"id":     ev.Metadata.Id,
						"labels": labelsList,
					})
					if err != nil {
						return nil, fmt.Errorf("failed to link labels to Chart: %w", err)
					}
				}
			}

		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	resp := domain.MetadataResp{
		ApiVersion:    chart.ApiVersion,
		SchemaVersion: chart.SchemaVersion,
		Kind:          chart.Kind,
		Metadata: struct {
			Id         string
			Name       string
			Namespace  string
			Maintainer string
		}{
			Id:         chart.Metadata.Id,
			Name:       chart.Metadata.Name,
			Namespace:  chart.Metadata.Namespace,
			Maintainer: chart.Metadata.Maintainer,
		},
	}
	return &resp, nil
}

func (r *RegistryRepo) GetChartMetadata(ctx context.Context, apiVersion, schemaVersion, name, namespace, maintainer string) (*domain.GetChartMetadataResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	useLatest := apiVersion == "" || schemaVersion == ""

	var versionMatch string

	if useLatest {
		versionMatch = `
			OPTIONAL MATCH (c)-[r:HAS_VERSION]->(v:Version)
			WITH c, labels, v, r
			ORDER BY r.createdAt DESC
			LIMIT 1
			WITH c, labels, v
		`
	} else {
		versionMatch = `
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			WITH c, v, labels
		`
	}

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := fmt.Sprintf(`
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {name: $chartName})

			OPTIONAL MATCH (c)-[:HAS_LABEL]->(l:Label)
			WITH c, collect({key: l.key, value: l.value}) AS labels

			%s	

			OPTIONAL MATCH (v)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, v, labels, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, v, labels, storedProcedures, sp, [ds1, ds2] AS spDataSources

			OPTIONAL MATCH (v)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, v, labels, storedProcedures, collect(spDataSources) AS spDataSourcesList,
				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
			WITH c, v, labels, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, v, labels, storedProcedures, triggers,
				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events
			
			OPTIONAL MATCH (ent)-[:HAS_LABEL]->(lab)
			WHERE ent.id IN (
				[sp IN storedProcedures | sp.nodeProps.id] +
				[tr IN triggers | tr.nodeProps.id] +
				[ev IN events | ev.nodeProps.id] +
				[ds IN allDataSources | ds.id]
			)
			WITH c, v, labels, storedProcedures, triggers, events, allDataSources,
				collect(CASE 
					WHEN ent.id IN [sp IN storedProcedures | sp.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS spLabels,
				
				collect(CASE 
					WHEN ent.id IN [tr IN triggers | tr.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS triggerLabels,

				collect(CASE 
					WHEN ent.id IN [ev IN events | ev.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS eventLabels,

				collect(CASE 
					WHEN ent.id IN [ds IN allDataSources | ds.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value }
					END) AS dataSourceLabels

			RETURN c, v, labels, storedProcedures, triggers, events, allDataSources,
				spLabels, triggerLabels, eventLabels, dataSourceLabels
		`, versionMatch)

		rec, err := tx.Run(ctx, query, map[string]any{
			"chartName":     name,
			"namespace":     namespace,
			"maintainer":    maintainer,
			"apiVersion":    apiVersion,
			"schemaVersion": schemaVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		if !rec.Next(ctx) {
			return nil, fmt.Errorf("chart not found")
		}

		record := rec.Record()

		if !useLatest {
			if v, ok := record.Get("v"); !ok || v == nil {
				return nil, fmt.Errorf("requested version %s/%s not found", apiVersion, schemaVersion)
			}
		}

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
				chart.Metadata.Namespace = namespace
				chart.Metadata.Maintainer = maintainer
				chart.Metadata.Description = getStringProp(node, "description")
				chart.Metadata.Visibility = getStringProp(node, "visibility")
				chart.Metadata.Engine = getStringProp(node, "engine")
				chart.Metadata.Labels = parseLabels(getStringProp(node, "labels"))
				if chart.Metadata.Labels == nil {
					chart.Metadata.Labels = map[string]string{}
				}
			}
		}

		// Version node
		if v, ok := record.Get("v"); ok {
			if node, ok := v.(neo4j.Node); ok {
				chart.ApiVersion = getStringProp(node, "apiVersion")
				chart.SchemaVersion = getStringProp(node, "schemaVersion")
			}
		}

		if v, ok := record.Get("labels"); ok {
			chart.Metadata.Labels = parseLabelList(v)
		}
		if chart.Metadata.Labels == nil {
			chart.Metadata.Labels = map[string]string{}
		}

		if v, ok := record.Get("allDataSources"); ok {
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

		return &chart, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetChartMetadataResp), nil
}

func (r *RegistryRepo) GetChartsLabels(ctx context.Context, apiVersion, schemaVersion, namespace, maintainer string, labels map[string]string) (*domain.GetChartsLabelsResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	useLatest := apiVersion == "" || schemaVersion == ""

	var versionMatch string

	if useLatest {
		versionMatch = `
			OPTIONAL MATCH (c)-[r:HAS_VERSION]->(v:Version)
			WITH c, labels, v, r
			ORDER BY r.createdAt DESC
			WITH c, labels, collect(v)[0] AS v
		`
	} else {
		versionMatch = `
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			WITH c, v, labels
		`
	}

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		params := map[string]any{
			"namespace":     namespace,
			"maintainer":    maintainer,
			"apiVersion":    apiVersion,
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

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			WITH c, v, labels	

			OPTIONAL MATCH (v)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, v, labels, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, v, labels, storedProcedures, sp, [ds1, ds2] AS spDataSources

			OPTIONAL MATCH (v)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, v, labels, storedProcedures, collect(spDataSources) AS spDataSourcesList,
				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
			WITH c, v, labels, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, v, labels, storedProcedures, triggers,
				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events
			
			OPTIONAL MATCH (ent)-[:HAS_LABEL]->(lab)
			WHERE ent.id IN (
				[sp IN storedProcedures | sp.nodeProps.id] +
				[tr IN triggers | tr.nodeProps.id] +
				[ev IN events | ev.nodeProps.id] +
				[ds IN allDataSources | ds.id]
			)
			WITH c, v, labels, storedProcedures, triggers, events, allDataSources,
				collect(CASE 
					WHEN ent.id IN [sp IN storedProcedures | sp.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS spLabels,
				
				collect(CASE 
					WHEN ent.id IN [tr IN triggers | tr.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS triggerLabels,

				collect(CASE 
					WHEN ent.id IN [ev IN events | ev.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS eventLabels,

				collect(CASE 
					WHEN ent.id IN [ds IN allDataSources | ds.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value }
					END) AS dataSourceLabels

			RETURN c, v, labels, spLabels, triggerLabels, eventLabels, dataSourceLabels,
			storedProcedures, triggers, events, allDataSources AS dataSources
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
					return nil, fmt.Errorf("requested version %s/%s not found", apiVersion, schemaVersion)
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
					chart.Metadata.Description = getStringProp(node, "description")
					chart.Metadata.Visibility = getStringProp(node, "visibility")
					chart.Metadata.Engine = getStringProp(node, "engine")
				}
			}

			// Version node
			if v, ok := record.Get("v"); ok {
				if node, ok := v.(neo4j.Node); ok {
					chart.ApiVersion = getStringProp(node, "apiVersion")
					chart.SchemaVersion = getStringProp(node, "schemaVersion")
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

func (r *RegistryRepo) GetChartId(ctx context.Context, apiVersion, schemaVersion, namespace, maintainer, chartId string) (*domain.GetChartMetadataResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	useLatest := apiVersion == "" || schemaVersion == ""

	var versionMatch string

	if useLatest {
		versionMatch = `
			OPTIONAL MATCH (c)-[r:HAS_VERSION]->(v:Version)
			WITH c, labels, v, r
			ORDER BY r.createdAt DESC
			LIMIT 1
			WITH c, labels, v
		`
	} else {
		versionMatch = `
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			WITH c, v, labels
		`
	}

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := fmt.Sprintf(`
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $chartId})

			OPTIONAL MATCH (c)-[:HAS_LABEL]->(l:Label)
			WITH c, collect({key: l.key, value: l.value}) AS labels

			%s

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v:Version{apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			WITH c, v, labels	

			OPTIONAL MATCH (v)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, v, labels, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, v, labels, storedProcedures, sp, [ds1, ds2] AS spDataSources

			OPTIONAL MATCH (v)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, v, labels, storedProcedures, collect(spDataSources) AS spDataSourcesList,
				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
			WITH c, v, labels, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, v, labels, storedProcedures, triggers,
				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events

			OPTIONAL MATCH (ent)-[:HAS_LABEL]->(lab)
			WHERE ent.id IN (
				[sp IN storedProcedures | sp.nodeProps.id] +
				[tr IN triggers | tr.nodeProps.id] +
				[ev IN events | ev.nodeProps.id] +
				[ds IN allDataSources | ds.id]
			)
			WITH c, v, labels, storedProcedures, triggers, events, allDataSources,
				collect(CASE 
					WHEN ent.id IN [sp IN storedProcedures | sp.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS spLabels,
				
				collect(CASE 
					WHEN ent.id IN [tr IN triggers | tr.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS triggerLabels,

				collect(CASE 
					WHEN ent.id IN [ev IN events | ev.nodeProps.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value } 
					END) AS eventLabels,

				collect(CASE 
					WHEN ent.id IN [ds IN allDataSources | ds.id] 
					THEN { id: ent.id, key: lab.key, value: lab.value }
					END) AS dataSourceLabels

			RETURN c, v, labels, spLabels, triggerLabels, eventLabels, dataSourceLabels,
			storedProcedures, triggers, events, allDataSources AS dataSources
			`, versionMatch)

		rec, err := tx.Run(ctx, query, map[string]any{
			"chartId":       chartId,
			"namespace":     namespace,
			"maintainer":    maintainer,
			"apiVersion":    apiVersion,
			"schemaVersion": schemaVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		if !rec.Next(ctx) {
			return nil, fmt.Errorf("chart not found")
		}

		record := rec.Record()

		if !useLatest {
			if v, ok := record.Get("v"); !ok || v == nil {
				return nil, fmt.Errorf("requested version %s/%s not found", apiVersion, schemaVersion)
			}
		}

		chart := domain.GetChartMetadataResp{
			DataSources:      make(map[string]*domain.DataSource),
			StoredProcedures: make(map[string]*domain.StoredProcedure),
			EventTriggers:    make(map[string]*domain.EventTrigger),
			Events:           make(map[string]*domain.Event),
		}

		// Chart node (metadata)
		if v, ok := record.Get("c"); ok {
			if node, ok := v.(neo4j.Node); ok {
				chart.Metadata.Id = getStringProp(node, "id")
				chart.Metadata.Name = getStringProp(node, "name")
				chart.Metadata.Namespace = namespace
				chart.Metadata.Maintainer = maintainer
				chart.Metadata.Description = getStringProp(node, "description")
				chart.Metadata.Visibility = getStringProp(node, "visibility")
				chart.Metadata.Engine = getStringProp(node, "engine")
				chart.Metadata.Labels = parseLabels(getStringProp(node, "labels"))
				if chart.Metadata.Labels == nil {
					chart.Metadata.Labels = map[string]string{}
				}
			}
		}

		// Version node
		if v, ok := record.Get("v"); ok {
			if node, ok := v.(neo4j.Node); ok {
				chart.ApiVersion = getStringProp(node, "apiVersion")
				chart.SchemaVersion = getStringProp(node, "schemaVersion")
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

		return &chart, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetChartMetadataResp), nil
}

func (r *RegistryRepo) GetMissingLayers(ctx context.Context, apiVersion, schemaVersion, namespace, maintainer, chartId string, layers []string) (*domain.GetMissingLayers, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	useLatest := apiVersion == "" || schemaVersion == ""

	var versionMatch string

	if useLatest {
		versionMatch = `
			OPTIONAL MATCH (c)-[r:HAS_VERSION]->(v:Version)
			WITH c, v, r
			ORDER BY r.createdAt DESC
			LIMIT 1
			WITH c, v
		`
	} else {
		versionMatch = `
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			WITH c, v
		`
	}

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		query := fmt.Sprintf(`
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $chartId})

			%s

			OPTIONAL MATCH (v)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, v, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			OPTIONAL MATCH (v)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, v, storedProcedures,
				collect({
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

			WITH c, v,
				[sp IN storedProcedures WHERE NOT sp.nodeProps.hash IN $layers] AS missingStoredProcedures,
				[t IN triggers WHERE NOT t.nodeProps.hash IN $layers] AS missingTriggers,
				[e IN events WHERE NOT e.nodeProps.hash IN $layers] AS missingEvents
			
			OPTIONAL MATCH (ent)-[:HAS_LABEL]->(lab)
			WHERE ent.id IN (
				[sp IN missingStoredProcedures | sp.nodeProps.id] +
				[tr IN missingTriggers | tr.nodeProps.id] +
				[ev IN missingEvents | ev.nodeProps.id]
			)
			WITH c, v, missingStoredProcedures, missingTriggers, missingEvents,
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
					END) AS eventLabels

			RETURN c, v,
				missingStoredProcedures AS storedProcedures,
				missingEvents AS events,
				missingTriggers AS triggers,
				spLabels, triggerLabels, eventLabels
		`, versionMatch)

		rec, err := tx.Run(ctx, query, map[string]any{
			"maintainer":    maintainer,
			"namespace":     namespace,
			"chartId":       chartId,
			"layers":        layers,
			"apiVersion":    apiVersion,
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
		}

		// Version node
		if v, ok := record.Get("v"); ok {
			if node, ok := v.(neo4j.Node); ok {
				resp.Metadata.ApiVersion = getStringProp(node, "apiVersion")
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

func (r *RegistryRepo) DeleteChart(ctx context.Context, id, name, namespace, maintainer, apiVersion, schemaVersion, kind string) error {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		// 1. Delete Chart
		queryDeleteVersion := `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})
			MATCH (n)-[:HAS_CHART]->(c:Chart {id: $chartId})
			OPTIONAL MATCH (c)-[r:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			DELETE r
			WITH c

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(otherV)
			WITH c, collect(otherV) AS versions
			WHERE size(versions) = 0
			DETACH DELETE c
		`

		_, err := tx.Run(ctx, queryDeleteVersion, map[string]any{
			"chartId":       id,
			"name":          name,
			"namespace":     namespace,
			"apiVersion":    apiVersion,
			"schemaVersion": schemaVersion,
			"kind":          kind,
			"maintainer":    maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to delete Version: %w", err)
		}

		// Delete Version
		queryVersion := `
			MATCH (v:Version)
			WHERE NOT (:Chart)-[:HAS_VERSION]->(v)
			DETACH DELETE v
		`
		_, err = tx.Run(ctx, queryVersion, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Version: %w", err)
		}

		// Delete Namespace
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

		// Delete User
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

		// Delete StoredProcedures
		querySP := `
			MATCH (s:StoredProcedure)
			WHERE NOT (:Version)-[:HAS_PROCEDURE]->(s)
			DETACH DELETE s
		`
		_, err = tx.Run(ctx, querySP, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan StoredProcedures: %w", err)
		}

		// Delete Triggers
		queryTriggers := `
			MATCH (t:Trigger)
			WHERE NOT (:Version)-[:HAS_TRIGGER]->(t)
			DETACH DELETE t
		`
		_, err = tx.Run(ctx, queryTriggers, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Triggers: %w", err)
		}

		// Delete DataSources
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

		// Delete Events
		queryEv := `
			MATCH (e:Event)
			WHERE NOT (:Trigger)-[:EVENT_LINK]->(e)
			DETACH DELETE e
		`
		_, err = tx.Run(ctx, queryEv, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Events: %w", err)
		}

		// Delete Labels
		queryOtherLabels := `
			MATCH (l:Label)
			WHERE NOT (l)<-[:HAS_LABEL]-(:StoredProcedure)
			AND NOT (l)<-[:HAS_LABEL]-(:DataSource)
			AND NOT (l)<-[:HAS_LABEL]-(:Trigger)
			AND NOT (l)<-[:HAS_LABEL]-(:Event)
			AND NOT (l)<-[:HAS_LABEL]-(:Chart)
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

func (r *RegistryRepo) UpdateChart(ctx context.Context, chart domain.StarChart) (*domain.MetadataResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// Namespace
		queryNamespace := `
			MATCH (n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $id})
			SET n.name = $namespace
		`
		if _, err := tx.Run(ctx, queryNamespace, map[string]any{
			"namespace": chart.Metadata.Namespace,
			"id":        chart.Metadata.Id,
		}); err != nil {
			return nil, err
		}

		// Chart
		queryChart := `
			MATCH (c:Chart {id: $id})
			SET c.name = $name,
				c.kind = $kind,
				c.description = $description,
				c.visibility = $visibility,
				c.engine = $engine
		`
		if _, err := tx.Run(ctx, queryChart, map[string]any{
			"id":          chart.Metadata.Id,
			"name":        chart.Metadata.Name,
			"kind":        chart.Kind,
			"description": chart.Metadata.Description,
			"visibility":  chart.Metadata.Visibility,
			"engine":      chart.Metadata.Engine,
		}); err != nil {
			return nil, err
		}

		// Label
		queryDeleteChartLabels := `
			MATCH (c:Chart {id: $id})-[r:HAS_LABEL]->(:Label)
			DELETE r
		`
		tx.Run(ctx, queryDeleteChartLabels, map[string]any{"id": chart.Metadata.Id})

		if lblList := convertLabelsToList(chart.Metadata.Labels); len(lblList) > 0 {
			queryAddLabels := `
			UNWIND $labels AS lbl
			MATCH (c:Chart {id: $id})
			MERGE (l:Label {key: lbl.key, value: lbl.value})
			MERGE (c)-[:HAS_LABEL]->(l)
			`
			tx.Run(ctx, queryAddLabels, map[string]any{"id": chart.Metadata.Id, "labels": lblList})
		}

		// Version
		apiVersion := chart.ApiVersion
		schemaVersion := chart.SchemaVersion

		if apiVersion == "" || schemaVersion == "" {

			queryLastVersion := `
			MATCH (c:Chart {id: $id})-[r:HAS_VERSION]->(v:Version)
			WITH v, r
			ORDER BY r.createdAt DESC
			LIMIT 1
			RETURN v.apiVersion AS apiVersion, v.schemaVersion AS schemaVersion
				`
			res, err := tx.Run(ctx, queryLastVersion, map[string]any{
				"id": chart.Metadata.Id,
			})
			if err != nil {
				return nil, err
			}

			hasVersion := res.Next(ctx)

			if hasVersion {
				lastApi, _ := res.Record().Get("apiVersion")
				lastSchema, _ := res.Record().Get("schemaVersion")

				lastApiStr := lastApi.(string)
				lastSchemaStr := lastSchema.(string)

				apiVersion = incrementVersion(lastApiStr)
				schemaVersion = incrementVersion(lastSchemaStr)

			} else {
				apiVersion = "v1.0.0"
				schemaVersion = "v1.0.0"
			}
		}

		queryMergeVersion := `
			MATCH (c:Chart {id: $id})
			MERGE (c)-[r:HAS_VERSION]->(v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			SET r.createdAt = $createdAt
			RETURN v
		`
		result, err := tx.Run(ctx, queryMergeVersion, map[string]any{
			"id":            chart.Metadata.Id,
			"apiVersion":    apiVersion,
			"schemaVersion": schemaVersion,
			"createdAt":     time.Now().Unix(),
		})
		if err != nil {
			return nil, err
		}

		_, err = result.Single(ctx)
		if err != nil {
			return nil, err
		}

		queryDeleteVersion := `
			MATCH (v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
			WHERE NOT (:Chart)-[:HAS_VERSION]->(v)
			DETACH DELETE v
		`
		tx.Run(ctx, queryDeleteVersion, map[string]any{
			"apiVersion":    chart.ApiVersion,
			"schemaVersion": chart.SchemaVersion,
		})

		// DataSource

		for _, ds := range chart.Chart.DataSources {
			ds.Hash = computeHash(ds.Type + ds.Path)

			queryDS := `
				MERGE (d:DataSource {hash: $hash})
				SET d.id = $id,
					d.name = $name,
					d.type = $type,
					d.path = $path,
					d.hash = $hash,
					d.resourceName = $resourceName,
					d.description = $description
			`
			tx.Run(ctx, queryDS, map[string]any{
				"id":           ds.Id,
				"name":         ds.Name,
				"type":         ds.Type,
				"path":         ds.Path,
				"hash":         ds.Hash,
				"resourceName": ds.ResourceName,
				"description":  ds.Description,
			})

			queryDeleteDSLabels := `
				MATCH (d:DataSource {id: $id})-[r:HAS_LABEL]->(:Label)
				DELETE r
			`
			tx.Run(ctx, queryDeleteDSLabels, map[string]any{"id": ds.Id})

			if lblList := convertLabelsToList(ds.Labels); len(lblList) > 0 {
				queryLabels := `
					MATCH (d:DataSource {id: $id})
					UNWIND $labels AS lbl
					MERGE (l:Label {key: lbl.key, value: lbl.value})
					MERGE (d)-[:HAS_LABEL]->(l)
				`
				tx.Run(ctx, queryLabels, map[string]any{"id": ds.Id, "labels": lblList})
			}
		}

		// StoredProcedure
		for _, sp := range chart.Chart.StoredProcedures {

			sp.Metadata.Hash = computeHash(sp.Metadata.Image)

			querySP := `
				MERGE (s:StoredProcedure {hash: $hash})
				SET s.id = $id
			`
			tx.Run(ctx, querySP, map[string]any{
				"id":   sp.Metadata.Id,
				"hash": sp.Metadata.Hash,
			})

			queryDeleteSP := `
				MATCH (s:StoredProcedure {id: $id})-[r]->()
				DELETE r
			`
			tx.Run(ctx, queryDeleteSP, map[string]any{"id": sp.Metadata.Id})

			queryRel := `
				MATCH (v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
				MATCH (s:StoredProcedure {id: $id})
				MERGE (v)-[r:HAS_PROCEDURE]->(s)
				SET r.name = $name,
					r.image = $image,
					r.prefix = $prefix,
					r.topic = $topic,
					r.disableVirtualization = $disableVirtualization,
					r.runDetached = $runDetached,
					r.removeOnStop = $removeOnStop,
					r.memory = $memory,
					r.kernelArgs = $kernelArgs,
					r.networks = $networks,
					r.ports = $ports,
					r.volumes = $volumes,
					r.targets = $targets,
					r.envVars = $envVars
			`
			tx.Run(ctx, queryRel, map[string]any{
				"id":                    sp.Metadata.Id,
				"name":                  sp.Metadata.Name,
				"image":                 sp.Metadata.Image,
				"prefix":                sp.Metadata.Prefix,
				"topic":                 sp.Metadata.Topic,
				"disableVirtualization": sp.Control.DisableVirtualization,
				"runDetached":           sp.Control.RunDetached,
				"removeOnStop":          sp.Control.RemoveOnStop,
				"memory":                sp.Control.Memory,
				"kernelArgs":            sp.Control.KernelArgs,
				"networks":              sp.Features.Networks,
				"ports":                 sp.Features.Ports,
				"volumes":               sp.Features.Volumes,
				"targets":               sp.Features.Targets,
				"envVars":               sp.Features.EnvVars,
				"apiVersion":            apiVersion,
				"schemaVersion":         schemaVersion,
			})

			for _, hl := range sp.Links.HardLinks {
				tx.Run(ctx, `
					MATCH (s:StoredProcedure {id: $spId})
					MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[:HARD_LINK]->(d)
				`, map[string]any{"spId": sp.Metadata.Id, "dsName": hl})
			}

			for _, sl := range sp.Links.SoftLinks {
				tx.Run(ctx, `
					MATCH (s:StoredProcedure {id: $spId})
					MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[:SOFT_LINK]->(d)
				`, map[string]any{"spId": sp.Metadata.Id, "dsName": sl})
			}

			queryDeleteSPLabels := `
				MATCH (s:StoredProcedure {id: $id})-[r:HAS_LABEL]->(:Label)
				DELETE r
			`
			tx.Run(ctx, queryDeleteSPLabels, map[string]any{"id": sp.Metadata.Id})

			if lblList := convertLabelsToList(sp.Metadata.Labels); len(lblList) > 0 {
				queryLabels := `
					MATCH (d:StoredProcedure {id: $id})
					UNWIND $labels AS lbl
					MERGE (l:Label {key: lbl.key, value: lbl.value})
					MERGE (d)-[:HAS_LABEL]->(l)
				`
				tx.Run(ctx, queryLabels, map[string]any{"id": sp.Metadata.Id, "labels": lblList})
			}
		}

		eventMap := map[string]*domain.Event{}
		for _, ev := range chart.Chart.Events {
			ev.Metadata.Hash = computeHash(ev.Metadata.Image)
			eventMap[ev.Metadata.Name] = ev
		}

		// Trigger
		for _, tr := range chart.Chart.EventTriggers {

			tr.Metadata.Hash = computeHash(tr.Metadata.Image)

			querySP := `
				MERGE (t:Trigger {hash: $hash})
				SET t.id = $id
			`
			tx.Run(ctx, querySP, map[string]any{
				"id":   tr.Metadata.Id,
				"hash": tr.Metadata.Hash,
			})

			queryDeleteSP := `
				MATCH (s:Trigger {id: $id})-[r]->()
				DELETE r
			`
			tx.Run(ctx, queryDeleteSP, map[string]any{"id": tr.Metadata.Id})

			queryRel := `
				MATCH (v:Version {apiVersion: $apiVersion, schemaVersion: $schemaVersion})
				MATCH (s:Trigger {id: $id})
				MERGE (v)-[r:HAS_TRIGGER]->(s)
				SET r.name = $name,
					r.image = $image,
					r.prefix = $prefix,
					r.topic = $topic,
					r.disableVirtualization = $disableVirtualization,
					r.runDetached = $runDetached,
					r.removeOnStop = $removeOnStop,
					r.memory = $memory,
					r.kernelArgs = $kernelArgs,
					r.networks = $networks,
					r.ports = $ports,
					r.volumes = $volumes,
					r.targets = $targets,
					r.envVars = $envVars
			`
			tx.Run(ctx, queryRel, map[string]any{
				"id":                    tr.Metadata.Id,
				"name":                  tr.Metadata.Name,
				"image":                 tr.Metadata.Image,
				"prefix":                tr.Metadata.Prefix,
				"topic":                 tr.Metadata.Topic,
				"disableVirtualization": tr.Control.DisableVirtualization,
				"runDetached":           tr.Control.RunDetached,
				"removeOnStop":          tr.Control.RemoveOnStop,
				"memory":                tr.Control.Memory,
				"kernelArgs":            tr.Control.KernelArgs,
				"networks":              tr.Features.Networks,
				"ports":                 tr.Features.Ports,
				"volumes":               tr.Features.Volumes,
				"targets":               tr.Features.Targets,
				"envVars":               tr.Features.EnvVars,
				"apiVersion":            apiVersion,
				"schemaVersion":         schemaVersion,
			})

			for _, hl := range tr.Links.HardLinks {
				tx.Run(ctx, `
					MATCH (s:Trigger {id: $spId})
					MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[:HARD_LINK]->(d)
				`, map[string]any{"spId": tr.Metadata.Id, "dsName": hl})
			}

			for _, sl := range tr.Links.SoftLinks {
				tx.Run(ctx, `
					MATCH (s:Trigger {id: $spId})
					MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[:SOFT_LINK]->(d)
				`, map[string]any{"spId": tr.Metadata.Id, "dsName": sl})
			}

			queryDeleteSPLabels := `
				MATCH (s:Trigger {id: $id})-[r:HAS_LABEL]->(:Label)
				DELETE r
			`
			tx.Run(ctx, queryDeleteSPLabels, map[string]any{"id": tr.Metadata.Id})

			if lblList := convertLabelsToList(tr.Metadata.Labels); len(lblList) > 0 {
				queryLabels := `
					MATCH (d:Trigger {id: $id})
					UNWIND $labels AS lbl
					MERGE (l:Label {key: lbl.key, value: lbl.value})
					MERGE (d)-[:HAS_LABEL]->(l)
				`
				tx.Run(ctx, queryLabels, map[string]any{"id": tr.Metadata.Id, "labels": lblList})
			}

			for _, eventName := range tr.Links.EventLinks {
				ev := eventMap[eventName]

				ev.Metadata.Hash = computeHash(ev.Metadata.Image)

				// Upsert Event
				_, err = tx.Run(ctx, `
					MERGE (e:Event {hash: $hash})
					ON CREATE SET e.id = $id
					ON MATCH SET  e.id = $id
				`, map[string]any{
					"id":   ev.Metadata.Id,
					"hash": ev.Metadata.Hash,
				})
				if err != nil {
					return nil, fmt.Errorf("failed event upsert: %w", err)
				}

				// LINK Trigger -> Event
				_, err = tx.Run(ctx, `
				MATCH (t:Trigger {id: $triggerId})
				MATCH (e:Event {id: $eventId})
				MERGE (t)-[r:EVENT_LINK]->(e)
				SET
					r.name = $name,
					r.image = $image,
					r.prefix = $prefix,
					r.topic = $topic,
					r.disableVirtualization = $disableVirtualization,
					r.runDetached = $runDetached,
					r.removeOnStop = $removeOnStop,
					r.memory = $memory,
					r.kernelArgs = $kernelArgs,
					r.networks = $networks,
					r.ports = $ports,
					r.volumes = $volumes,
					r.targets = $targets,
					r.envVars = $envVars
				`, map[string]any{
					"triggerId":             tr.Metadata.Id,
					"eventId":               ev.Metadata.Id,
					"name":                  ev.Metadata.Name,
					"image":                 ev.Metadata.Image,
					"prefix":                ev.Metadata.Prefix,
					"topic":                 ev.Metadata.Topic,
					"disableVirtualization": ev.Control.DisableVirtualization,
					"runDetached":           ev.Control.RunDetached,
					"removeOnStop":          ev.Control.RemoveOnStop,
					"memory":                ev.Control.Memory,
					"kernelArgs":            ev.Control.KernelArgs,
					"networks":              ev.Features.Networks,
					"ports":                 ev.Features.Ports,
					"volumes":               ev.Features.Volumes,
					"targets":               ev.Features.Targets,
					"envVars":               ev.Features.EnvVars,
				})
				if err != nil {
					return nil, fmt.Errorf("failed event link: %w", err)
				}

				queryDeleteEvLabels := `
				MATCH (s:Event {id: $id})-[r:HAS_LABEL]->(:Label)
				DELETE r
			`
				tx.Run(ctx, queryDeleteEvLabels, map[string]any{"id": ev.Metadata.Id})

				// Event Labels
				evLbl := convertLabelsToList(ev.Metadata.Labels)
				if len(evLbl) > 0 {
					_, err = tx.Run(ctx, `
				MATCH (e:Event {id: $id})
				UNWIND $labels AS lbl
				MERGE (l:Label {key: lbl.key, value: lbl.value})
				MERGE (e)-[:HAS_LABEL]->(l)
			`, map[string]any{
						"id":     ev.Metadata.Id,
						"labels": evLbl,
					})
					if err != nil {
						return nil, fmt.Errorf("failed event labels: %w", err)
					}
				}
			}
		}

		// Clear Labels
		queryClearLables := `
			MATCH (l:Label)
			WHERE NOT (l)<-[:HAS_LABEL]-(:StoredProcedure)
			AND NOT (l)<-[:HAS_LABEL]-(:DataSource)
			AND NOT (l)<-[:HAS_LABEL]-(:Trigger)
			AND NOT (l)<-[:HAS_LABEL]-(:Event)
			AND NOT (l)<-[:HAS_LABEL]-(:Chart)
			DETACH DELETE l
    `
		_, err = tx.Run(ctx, queryClearLables, map[string]any{
			"id": chart.Metadata.Id,
		})
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	resp := domain.MetadataResp{
		ApiVersion:    chart.ApiVersion,
		SchemaVersion: chart.SchemaVersion,
		Kind:          chart.Kind,
		Metadata: struct {
			Id         string
			Name       string
			Namespace  string
			Maintainer string
		}{
			Id:         chart.Metadata.Id,
			Name:       chart.Metadata.Name,
			Namespace:  chart.Metadata.Namespace,
			Maintainer: chart.Metadata.Maintainer,
		},
	}
	return &resp, nil
}
