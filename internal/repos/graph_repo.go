package repos

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"registry/config"
	"registry/domain"

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
		labelsJSON, err := json.Marshal(chart.Metadata.Labels)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal labels: %w", err)
		}

		queryChart := `
			MERGE (c:Chart {name: $name})
			SET c.apiVersion = $apiVersion,
				c.schemaVersion = $schemaVersion,
				c.kind = $kind,
				c.description = $description,
				c.visibility = $visibility,
				c.engine = $engine,
				c.labels = $labels
			WITH c
			MATCH (n:Namespace {name: $namespace})
			MERGE (n)-[:HAS_CHART]->(c)
		`
		_, err = tx.Run(ctx, queryChart, map[string]any{
			"name":          chart.Metadata.Name,
			"apiVersion":    chart.ApiVersion,
			"schemaVersion": chart.SchemaVersion,
			"kind":          chart.Kind,
			"description":   chart.Metadata.Description,
			"visibility":    chart.Metadata.Visibility,
			"engine":        chart.Metadata.Engine,
			"labels":        string(labelsJSON),
			"namespace":     chart.Metadata.Namespace,
			"maintainer":    chart.Metadata.Maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Chart node: %w", err)
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
					s.id = $id, 
					s.name = $name,
					s.image = $image,
					s.hash = $hash,
					s.prefix = $prefix,
					s.topic = $topic,
					s.disableVirtualization = $disableVirtualization,
					s.runDetached = $runDetached,
					s.removeOnStop = $removeOnStop,
					s.memory = $memory,
					s.kernelArgs = $kernelArgs,
					s.networks = $networks,
					s.ports = $ports,
					s.volumes = $volumes,
					s.targets = $targets,
					s.envVars = $envVars
				WITH s
				MATCH (c:Chart {name: $chartName})
				MERGE (c)-[:HAS_PROCEDURE]->(s)
			`
			_, err := tx.Run(ctx, querySP, map[string]any{
				"id":                    sp.Metadata.Id,
				"name":                  sp.Metadata.Name,
				"image":                 sp.Metadata.Image,
				"hash":                  sp.Metadata.Hash,
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
				"chartName":             chart.Metadata.Name,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create StoredProcedure node for %s: %w", key, err)
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

		// Events
		for key, ev := range chart.Chart.Events {

			ev.Metadata.Hash = computeHash(ev.Metadata.Image)

			queryEv := `
				MERGE (e:Event {hash: $hash})
				ON CREATE SET 
					e.id = $id,
					e.name = $name,
					e.image = $image,
					e.hash = $hash,
					e.prefix = $prefix,
					e.topic = $topic,
					e.disableVirtualization = $disableVirtualization,
					e.runDetached = $runDetached,
					e.removeOnStop = $removeOnStop,
					e.memory = $memory,
					e.kernelArgs = $kernelArgs,
					e.networks = $networks,
					e.ports = $ports,
					e.volumes = $volumes,
					e.targets = $targets,
					e.envVars = $envVars
			`
			_, err := tx.Run(ctx, queryEv, map[string]any{
				"id":                    ev.Metadata.Id,
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
				return nil, fmt.Errorf("failed to create Event node for %s: %w", key, err)
			}
		}

		// EventTriggers
		for key, et := range chart.Chart.EventTriggers {

			et.Metadata.Hash = computeHash(et.Metadata.Image)

			queryET := `
				MERGE (t:Trigger {hash: $hash})
				ON CREATE SET
					t.id = $id,	 
					t.name = $name,
					t.image = $image,
					t.hash = $hash,
					t.prefix = $prefix,
					t.topic = $topic,
					t.disableVirtualization = $disableVirtualization,
					t.runDetached = $runDetached,
					t.removeOnStop = $removeOnStop,
					t.memory = $memory,
					t.kernelArgs = $kernelArgs,
					t.networks = $networks,
					t.ports = $ports,
					t.volumes = $volumes,
					t.targets = $targets,
					t.envVars = $envVars
				WITH t
				MATCH (c:Chart {name: $chartName})
				MERGE (c)-[:HAS_TRIGGER]->(t)
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
				"chartName":             chart.Metadata.Name,
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

			for _, eventLink := range et.Links.EventLinks {
				queryLink := `
					MATCH (t:Trigger {id: $triggerId})
					MATCH (e:Event {name: $eventName})
					MERGE (t)-[:EVENT_LINK]->(e)
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"triggerId": et.Metadata.Id,
					"eventName": eventLink,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create trigger event link for %s: %w", key, err)
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
			OPTIONAL MATCH (c)-[:HAS_PROCEDURE]->(sp:StoredProcedure)
			WITH c, collect(DISTINCT sp) as storedProcedures
			UNWIND storedProcedures as sp
			OPTIONAL MATCH (sp)-[:HARD_LINK]->(ds1:DataSource)
			OPTIONAL MATCH (sp)-[:SOFT_LINK]->(ds2:DataSource)
			WITH c, storedProcedures, sp, [ds1, ds2] as spDataSources
			OPTIONAL MATCH (c)-[:HAS_TRIGGER]->(t:Trigger)
			OPTIONAL MATCH (t)-[:HARD_LINK]->(ds3:DataSource)
			OPTIONAL MATCH (t)-[:SOFT_LINK]->(ds4:DataSource)
			OPTIONAL MATCH (t)-[:EVENT_LINK]->(e:Event)
			WITH c, storedProcedures, collect(DISTINCT t) as triggers, collect(DISTINCT e) as events, collect(DISTINCT spDataSources + [ds3, ds4]) as dataSourcesNested
			WITH c, storedProcedures, triggers, events, [ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL] as allDataSources
			RETURN c, allDataSources as dataSources, storedProcedures, events, triggers
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
		labelFilters := ""
		params := map[string]any{
			"namespace":  namespace,
			"maintainer": maintainer,
		}

		if len(labels) > 0 {
			labelFilters = "WHERE "
			i := 0
			for k, v := range labels {
				if i > 0 {
					labelFilters += " AND "
				}
				labelFilters += fmt.Sprintf("c.labels CONTAINS '\"%s\":\"%s\"'", k, v)
				i++
			}
		}

		query := fmt.Sprintf(`
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart)
			%s
			OPTIONAL MATCH (c)-[:HAS_PROCEDURE]->(sp:StoredProcedure)
			OPTIONAL MATCH (c)-[:HAS_TRIGGER]->(t:Trigger)
			OPTIONAL MATCH (t)-[:EVENT_LINK]->(e:Event)
			OPTIONAL MATCH (sp)-[:HARD_LINK]->(ds1:DataSource)
			OPTIONAL MATCH (sp)-[:SOFT_LINK]->(ds2:DataSource)
			OPTIONAL MATCH (t)-[:HARD_LINK]->(ds3:DataSource)
			OPTIONAL MATCH (t)-[:SOFT_LINK]->(ds4:DataSource)
			WITH c, 
				collect(DISTINCT sp) as storedProcedures, 
				collect(DISTINCT t) as triggers,
				collect(DISTINCT e) as events,
				collect(DISTINCT [ds1, ds2, ds3, ds4]) as dataSourcesNested
			WITH c, storedProcedures, triggers, events, [ds IN apoc.coll.flatten(dataSourcesNested) WHERE ds IS NOT NULL] as allDataSources
			RETURN c, allDataSources as dataSources, storedProcedures, triggers, events
		`, labelFilters)

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
		_, err := tx.Run(ctx, queryChart, map[string]any{
			"name":          name,
			"namespace":     namespace,
			"apiVersion":    apiVersion,
			"schemaVersion": schemaVersion,
			"kind":          kind,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to delete Chart: %w", err)
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
