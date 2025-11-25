package repos

import (
	"context"
	"fmt"
	"log"

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

func (r *RegistryRepo) PutChart(ctx context.Context, chart domain.StarChart) (*domain.StarChart, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

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
			MERGE (c:Chart {name: $name, apiVersion: $apiVersion, schemaVersion: $schemaVersion, kind: $kind})
			SET c.id = $id,
				c.description = $description,
				c.visibility = $visibility,
				c.engine = $engine
			WITH c
			MATCH (n:Namespace {name: $namespace})
			MERGE (n)-[:HAS_CHART]->(c)
		`
		_, err = tx.Run(ctx, queryChart, map[string]any{
			"id":            chart.Metadata.Id,
			"name":          chart.Metadata.Name,
			"apiVersion":    chart.ApiVersion,
			"schemaVersion": chart.SchemaVersion,
			"kind":          chart.Kind,
			"description":   chart.Metadata.Description,
			"visibility":    chart.Metadata.Visibility,
			"engine":        chart.Metadata.Engine,
			"namespace":     chart.Metadata.Namespace,
			"maintainer":    chart.Metadata.Maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Chart node: %w", err)
		}

		// Labels
		labelsList := convertLabelsToList(chart.Metadata.Labels)
		if len(labelsList) > 0 {
			queryLabels := `
				MATCH (c:Chart {name: $name})
				UNWIND $labels AS lbl
				MERGE (l:Label {key: lbl.key, value: lbl.value})
				MERGE (c)-[:HAS_LABEL]->(l)
			`
			_, err = tx.Run(ctx, queryLabels, map[string]any{
				"name":   chart.Metadata.Name,
				"labels": labelsList,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to link labels to Chart: %w", err)
			}
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
		}

		// StoredProcedures
		for key, sp := range chart.Chart.StoredProcedures {

			sp.Metadata.Hash = computeHash(sp.Metadata.Image)

			querySP := `
				MERGE (s:StoredProcedure {hash: $hash})
				ON CREATE SET
					s.id = $id
				WITH s
				MATCH (c:Chart {id: $chartId})
				MERGE (c)-[r:HAS_PROCEDURE]->(s)
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
				MATCH (c:Chart {id: $chartId})
				MERGE (c)-[r:HAS_TRIGGER]->(t)
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

					// properties for the RELATION
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
			}

		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return &chart, nil
}

func (r *RegistryRepo) GetChartMetadata(ctx context.Context, name, namespace, maintainer string) (*domain.GetChartMetadataResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {name: $chartName})

			OPTIONAL MATCH (c)-[:HAS_LABEL]->(l:Label)
			WITH c, collect({key: l.key, value: l.value}) AS labels

			OPTIONAL MATCH (c)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, labels, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, labels, storedProcedures, sp, [ds1, ds2] AS spDataSources

			OPTIONAL MATCH (c)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, labels, storedProcedures, collect(spDataSources) AS spDataSourcesList,
				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
			WITH c, labels, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, labels, storedProcedures, triggers,
				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events

			RETURN c, labels, storedProcedures, triggers, events, allDataSources AS dataSources
		`

		rec, err := tx.Run(ctx, query, map[string]any{
			"chartName":  name,
			"namespace":  namespace,
			"maintainer": maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		if !rec.Next(ctx) {
			return nil, fmt.Errorf("chart not found")
		}

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

		if v, ok := record.Get("labels"); ok {
			chart.Metadata.Labels = parseLabelList(v)
		}
		if chart.Metadata.Labels == nil {
			chart.Metadata.Labels = map[string]string{}
		}

		if v, ok := record.Get("dataSources"); ok {
			chart.DataSources = parseDataSources(v)
		}

		if v, ok := record.Get("storedProcedures"); ok {
			chart.StoredProcedures = parseStoredProcedures(ctx, tx, v)
		}

		if v, ok := record.Get("events"); ok {
			chart.Events = parseEvents(v)
		}

		if v, ok := record.Get("triggers"); ok {
			chart.EventTriggers = parseTriggers(ctx, tx, v)
		}

		return &chart, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetChartMetadataResp), nil
}

func (r *RegistryRepo) GetChartsLabels(ctx context.Context, namespace, maintainer string, labels map[string]string) (*domain.GetChartsLabelsResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		params := map[string]any{
			"namespace":  namespace,
			"maintainer": maintainer,
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

			OPTIONAL MATCH (c)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, labels, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, labels, storedProcedures, sp, [ds1, ds2] AS spDataSources

			OPTIONAL MATCH (c)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, labels, storedProcedures, collect(spDataSources) AS spDataSourcesList,
				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
			WITH c, labels, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, labels, storedProcedures, triggers,
				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events

			RETURN c, labels, storedProcedures, triggers, events, allDataSources AS dataSources
		`, labelMatch)

		rec, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		resp := &domain.GetChartsLabelsResp{
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

			if v, ok := record.Get("labels"); ok {
				chart.Metadata.Labels = parseLabelList(v)
			}

			if chart.Metadata.Labels == nil {
				chart.Metadata.Labels = map[string]string{}
			}

			if v, ok := record.Get("dataSources"); ok {
				chart.DataSources = parseDataSources(v)
			}

			if v, ok := record.Get("storedProcedures"); ok {
				chart.StoredProcedures = parseStoredProcedures(ctx, tx, v)
			}

			if v, ok := record.Get("events"); ok {
				chart.Events = parseEvents(v)
			}

			if v, ok := record.Get("triggers"); ok {
				chart.EventTriggers = parseTriggers(ctx, tx, v)
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

func (r *RegistryRepo) GetChartId(ctx context.Context, namespace, maintainer, chartId string) (*domain.GetChartMetadataResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $chartId})

			OPTIONAL MATCH (c)-[:HAS_LABEL]->(l:Label)
			WITH c, collect({key: l.key, value: l.value}) AS labels

			OPTIONAL MATCH (c)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, labels, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, labels, storedProcedures, sp, [ds1, ds2] AS spDataSources

			OPTIONAL MATCH (c)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, labels, storedProcedures, collect(spDataSources) AS spDataSourcesList,
				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
			WITH c, labels, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, labels, storedProcedures, triggers,
				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events

			RETURN c, labels, storedProcedures, triggers, events, allDataSources AS dataSources
		`

		rec, err := tx.Run(ctx, query, map[string]any{
			"chartId":    chartId,
			"namespace":  namespace,
			"maintainer": maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("query failed: %w", err)
		}

		if !rec.Next(ctx) {
			return nil, fmt.Errorf("chart not found")
		}

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

		if v, ok := record.Get("labels"); ok {
			chart.Metadata.Labels = parseLabelList(v)
		}
		if chart.Metadata.Labels == nil {
			chart.Metadata.Labels = map[string]string{}
		}

		if v, ok := record.Get("dataSources"); ok {
			chart.DataSources = parseDataSources(v)
		}

		if v, ok := record.Get("storedProcedures"); ok {
			chart.StoredProcedures = parseStoredProcedures(ctx, tx, v)
		}

		if v, ok := record.Get("events"); ok {
			chart.Events = parseEvents(v)
		}

		if v, ok := record.Get("triggers"); ok {
			chart.EventTriggers = parseTriggers(ctx, tx, v)
		}

		return &chart, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*domain.GetChartMetadataResp), nil
}

func (r *RegistryRepo) GetMissingLayers(ctx context.Context, namespace, maintainer, chartId string, layers []string) (*domain.GetMissingLayers, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeRead,
	})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		query := `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $chartId})

			OPTIONAL MATCH (c)-[spRels:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, collect({
				nodeProps: properties(sp),
				relProps: properties(spRels)
			}) AS storedProcedures

			UNWIND storedProcedures AS sp
			OPTIONAL MATCH (ds1:DataSource)<-[:HARD_LINK]-(spNode:StoredProcedure {id: sp.nodeProps.id})
			OPTIONAL MATCH (ds2:DataSource)<-[:SOFT_LINK]-(spNode)
			WITH c, storedProcedures, sp, [ds1, ds2] AS spDataSources

			OPTIONAL MATCH (c)-[tRels:HAS_TRIGGER]->(t:Trigger)
			WITH c, storedProcedures, collect(spDataSources) AS spDataSourcesList,
				collect({nodeProps: properties(t), relProps: properties(tRels)}) AS triggers

			UNWIND triggers AS tr
			OPTIONAL MATCH (ds3:DataSource)<-[:HARD_LINK]-(trNode:Trigger {id: tr.nodeProps.id})
			OPTIONAL MATCH (ds4:DataSource)<-[:SOFT_LINK]-(trNode)
			WITH c, storedProcedures, triggers, collect(spDataSourcesList + [ds3, ds4]) AS dataSourcesNested, tr

			OPTIONAL MATCH (trNode)-[eRels:EVENT_LINK]->(e:Event)
			WITH c, storedProcedures, triggers,
				apoc.coll.flatten([ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL]) AS allDataSources,
				collect({nodeProps: properties(e), relProps: properties(eRels)}) AS events

			WITH c,
				[ds IN allDataSources WHERE NOT ds.hash IN $layers] AS missingDataSources,
				[sp IN storedProcedures WHERE NOT sp.nodeProps.hash IN $layers] AS missingStoredProcedures,
				[t IN triggers WHERE NOT t.nodeProps.hash IN $layers] AS missingTriggers,
				[e IN events WHERE NOT e.nodeProps.hash IN $layers] AS missingEvents
			RETURN c,
				missingDataSources AS dataSources,
				missingStoredProcedures AS storedProcedures,
				missingEvents AS events,
				missingTriggers AS triggers
		`

		rec, err := tx.Run(ctx, query, map[string]any{
			"maintainer": maintainer,
			"namespace":  namespace,
			"chartId":    chartId,
			"layers":     layers,
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
				Id        string
				Name      string
				Namespace string
			}{
				Id:        chartId,
				Name:      maintainer,
				Namespace: namespace,
			},
			DataSources:      make(map[string]*domain.DataSource),
			StoredProcedures: make(map[string]*domain.StoredProcedure),
			EventTriggers:    make(map[string]*domain.EventTrigger),
			Events:           make(map[string]*domain.Event),
		}

		// DataSources
		if v, ok := record.Get("dataSources"); ok {
			dsParsed := parseDataSources(v)
			for key, ds := range dsParsed {
				resp.DataSources[key] = ds
			}
		}

		// StoredProcedures
		if v, ok := record.Get("storedProcedures"); ok {
			spParsed := parseStoredProcedures(ctx, tx, v)
			for key, sp := range spParsed {
				resp.StoredProcedures[key] = sp
			}
		}

		// Events
		if v, ok := record.Get("events"); ok {
			evParsed := parseEvents(v)
			for key, ev := range evParsed {
				resp.Events[key] = ev
			}
		}

		// EventTriggers
		if v, ok := record.Get("triggers"); ok {
			trParsed := parseTriggers(ctx, tx, v)
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

func (r *RegistryRepo) DeleteChart(ctx context.Context, name, namespace, maintainer, apiVersion, schemaVersion, kind string) error {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// Delete Chart node
		queryChart := `
			MATCH (c:Chart {name: $name, apiVersion: $apiVersion, schemaVersion: $schemaVersion, kind: $kind})
			WHERE EXISTS ((:Namespace {name: $namespace})-[:HAS_CHART]->(c))
			DETACH DELETE c
			RETURN c
		`
		result, err := tx.Run(ctx, queryChart, map[string]any{
			"name":          name,
			"namespace":     namespace,
			"apiVersion":    apiVersion,
			"schemaVersion": schemaVersion,
			"kind":          kind,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to delete Chart: %w", err)
		}

		if !result.Next(ctx) {
			if result.Err() != nil {
				return nil, fmt.Errorf("failed to read result: %w", result.Err())
			}
			return nil, fmt.Errorf("chart not found: %s/%s", namespace, name)
		}

		// Delete Labels
		queryLabels := `
			MATCH (l:Label)
			WHERE NOT (l)<-[:HAS_LABEL]-(:Chart)
			DETACH DELETE l
		`
		_, err = tx.Run(ctx, queryLabels, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan Labels: %w", err)
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
			WHERE NOT (:Chart)-[:HAS_PROCEDURE]->(s)
			DETACH DELETE s
		`
		_, err = tx.Run(ctx, querySP, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to delete orphan StoredProcedures: %w", err)
		}

		// Delete Triggers
		queryTriggers := `
			MATCH (t:Trigger)
			WHERE NOT (:Chart)-[:HAS_TRIGGER]->(t)
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

		return nil, nil
	})

	return err
}
