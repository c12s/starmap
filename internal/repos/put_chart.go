package repos

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

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
	if chart.Metadata.Namespace == "" {
		chart.Metadata.Namespace = "default"
	}

	computeComponentHashes(&chart)
	versionHash := computeVersionHash(chart)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		// Namespace and User Node
		queryNamespace := `
			MERGE (u:User {name: $maintainer})
			MERGE (u)-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})
		`
		_, err := tx.Run(ctx, queryNamespace, map[string]any{
			"namespace":  chart.Metadata.Namespace,
			"maintainer": chart.Metadata.Maintainer,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Namespace node: %w", err)
		}

		// Chart Node
		queryChart := `
			MERGE (c:Chart {id: $id})
			SET c.name = $name,
				c.kind = $kind,
				c.apiVersion = $apiVersion,
				c.description = $description,
				c.visibility = $visibility,
				c.engine = $engine
			WITH c
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})
			MERGE (n)-[r:HAS_CHART]->(c)
			SET r.versions = coalesce(r.versions, []) + $schemaVersion
			SET r.versions = apoc.coll.toSet(r.versions)
		`
		_, err = tx.Run(ctx, queryChart, map[string]any{
			"id":            chart.Metadata.Id,
			"name":          chart.Metadata.Name,
			"kind":          chart.Kind,
			"description":   chart.Metadata.Description,
			"visibility":    chart.Metadata.Visibility,
			"engine":        chart.Metadata.Engine,
			"namespace":     chart.Metadata.Namespace,
			"maintainer":    chart.Metadata.Maintainer,
			"apiVersion":    chart.ApiVersion,
			"schemaVersion": chart.SchemaVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Chart node: %w", err)
		}

		// ChartLabels
		labelsList := convertMapToList(chart.Metadata.Labels)
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
		tagsChartJSON, _ := json.Marshal(chart.Metadata.Tags)
		queryVersion := `
			MATCH (c:Chart {id: $id})
			MERGE (v:Version {hash: $versionHash})
			ON CREATE SET
				v.schemaVersion = $schemaVersion,
				v.tags = $tags
			MERGE (c)-[r:HAS_VERSION]->(v)
			ON CREATE SET r.createdAt = $now
		`
		_, err = tx.Run(ctx, queryVersion, map[string]any{
			"id":            chart.Metadata.Id,
			"schemaVersion": chart.SchemaVersion,
			"now":           time.Now().Unix(),
			"versionHash":   versionHash,
			"tags":          string(tagsChartJSON),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Version node: %w", err)
		}

		// DataSources
		for key, ds := range chart.Chart.DataSources {

			tagsJSON, _ := json.Marshal(ds.Tags)

			queryDS := `
				MERGE (d:DataSource {hash: $hash})
				ON CREATE SET 
					d.id = $id,
					d.name = $name,
					d.type = $type,
					d.path = $path,
					d.hash = $hash,
					d.resourceName = $resourceName,
					d.description = $description,
					d.tags = $tags
			`
			_, err := tx.Run(ctx, queryDS, map[string]any{
				"id":           ds.Id,
				"name":         ds.Name,
				"type":         ds.Type,
				"path":         ds.Path,
				"hash":         ds.Hash,
				"resourceName": ds.ResourceName,
				"description":  ds.Description,
				"tags":         string(tagsJSON),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create DataSource node for %s: %w", key, err)
			}

			// Data Source Labels
			labelsList := convertMapToList(ds.Labels)
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

			tagsJSON, _ := json.Marshal(sp.Metadata.Tags)

			querySP := `
				MERGE (s:StoredProcedure {hash: $hash})
				ON CREATE SET
					s.id = $id
				WITH s
				MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(v:Version {schemaVersion: $schemaVersion})
				WITH s, v
				MERGE (v)-[r:HAS_PROCEDURE]->(s)
				SET 
					r.name = $name,
					r.prefix = $prefix,
					r.topic = $topic,
					r.description = $description,
					r.disableVirtualization = $disableVirtualization,
					r.runDetached = $runDetached,
					r.removeOnStop = $removeOnStop,
					r.memory = $memory,
					r.kernelArgs = $kernelArgs,
					r.networks = $networks,
					r.ports = $ports,
					r.volumes = $volumes,
					r.targets = $targets,
					r.envVars = $envVars,
					r.tags = $tags,
					r.image = CASE WHEN $image <> '' THEN $image ELSE null END,
    				s.pull = CASE WHEN $pull <> '' THEN $pull ELSE null END,
    				r.workdir = CASE WHEN $workdir <> '' THEN $workdir ELSE null END,
    				s.command = CASE WHEN $command <> '' THEN $command ELSE null END
			`
			_, err := tx.Run(ctx, querySP, map[string]any{
				"id":                    sp.Metadata.Id,
				"hash":                  sp.Metadata.Hash,
				"name":                  sp.Metadata.Name,
				"image":                 sp.Metadata.Image,
				"prefix":                sp.Metadata.Prefix,
				"topic":                 sp.Metadata.Topic,
				"description":           sp.Metadata.Description,
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
				"schemaVersion":         chart.SchemaVersion,
				"chartId":               chart.Metadata.Id,
				"pull":                  sp.Metadata.Build.Pull,
				"command":               sp.Metadata.Build.Command,
				"workdir":               sp.Metadata.Build.Workdir,
				"tags":                  string(tagsJSON),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create StoredProcedure relation for %s: %w", key, err)
			}

			for _, hardLink := range sp.Links.HardLinks {
				queryLink := `
					MATCH (sp:StoredProcedure {id: $spId})
					OPTIONAL MATCH (ds:DataSource {name: $dsName})
					MERGE (sp)-[hl:HARD_LINK]->(ds)
					SET hl.resourceName = ds.resourceName,
						hl.description = ds.description
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
					OPTIONAL MATCH (ds:DataSource {name: $dsName})
					MERGE (sp)-[sl:SOFT_LINK]->(ds)
					SET sl.resourceName = ds.resourceName,
						sl.description = ds.description
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
			labelsList := convertMapToList(sp.Metadata.Labels)
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
			eventMap[ev.Metadata.Name] = ev
		}

		// EventTriggers
		for key, et := range chart.Chart.EventTriggers {

			tagsJSON, _ := json.Marshal(et.Metadata.Tags)

			sort.Strings(et.Links.EventLinks)
			var eventHashes []string

			for _, eventName := range et.Links.EventLinks {
				if ev, ok := chart.Chart.Events[eventName]; ok {
					eventHashes = append(eventHashes, ev.Metadata.Hash)
				}
			}

			triggerEventHash := computeTriggerEventHash(et.Metadata.Hash, eventHashes)

			queryET := `
				MERGE (t:Trigger {triggerEventHash: $triggerEventHash})
				ON CREATE SET
					t.id = $id
				WITH t
				MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(v:Version {schemaVersion: $schemaVersion})
				WITH t, v
				MERGE (v)-[r:HAS_TRIGGER]->(t)
				SET
					r.name = $name,
					r.hash = $hash,
					r.prefix = $prefix,
					r.topic = $topic,
					r.description = $description,
					r.disableVirtualization = $disableVirtualization,
					r.runDetached = $runDetached,
					r.removeOnStop = $removeOnStop,
					r.memory = $memory,
					r.kernelArgs = $kernelArgs,
					r.networks = $networks,
					r.ports = $ports,
					r.volumes = $volumes,
					r.targets = $targets,
					r.envVars = $envVars,
					r.tags = $tags,
					r.image = CASE WHEN $image <> '' THEN $image ELSE null END,
    				t.pull = CASE WHEN $pull <> '' THEN $pull ELSE null END,
    				r.workdir = CASE WHEN $workdir <> '' THEN $workdir ELSE null END,
    				t.command = CASE WHEN $command <> '' THEN $command ELSE null END
			`
			_, err = tx.Run(ctx, queryET, map[string]any{
				"id":                    et.Metadata.Id,
				"name":                  et.Metadata.Name,
				"image":                 et.Metadata.Image,
				"hash":                  et.Metadata.Hash,
				"prefix":                et.Metadata.Prefix,
				"topic":                 et.Metadata.Topic,
				"description":           et.Metadata.Description,
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
				"schemaVersion":         chart.SchemaVersion,
				"chartId":               chart.Metadata.Id,
				"triggerEventHash":      triggerEventHash,
				"pull":                  et.Metadata.Build.Pull,
				"command":               et.Metadata.Build.Command,
				"workdir":               et.Metadata.Build.Workdir,
				"tags":                  string(tagsJSON),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create EventTrigger node for %s: %w", key, err)
			}

			for _, hardLink := range et.Links.HardLinks {
				queryLink := `
					MATCH (t:Trigger {id: $triggerId})
					OPTIONAL MATCH (ds:DataSource {name: $dsName})
					MERGE (t)-[hl:HARD_LINK]->(ds)
					SET hl.resourceName = ds.resourceName,
						hl.description = ds.description
					REMOVE ds.resourceName, ds.description
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
					OPTIONAL MATCH (ds:DataSource {name: $dsName})
					MERGE (t)-[sl:SOFT_LINK]->(ds)
					SET sl.resourceName = ds.resourceName,
						sl.description = ds.description
					REMOVE ds.resourceName, ds.description
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
			labelsList := convertMapToList(et.Metadata.Labels)
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

			// Event
			for _, eventName := range et.Links.EventLinks {
				ev, ok := eventMap[eventName]
				if !ok {
					continue
				}

				evTagsJSON, _ := json.Marshal(ev.Metadata.Tags)

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
						r.prefix = $prefix,
						r.topic = $topic,
						r.description = $description,
						r.disableVirtualization = $disableVirtualization,
						r.runDetached = $runDetached,
						r.removeOnStop = $removeOnStop,
						r.memory = $memory,
						r.kernelArgs = $kernelArgs,
						r.networks = $networks,
						r.ports = $ports,
						r.volumes = $volumes,
						r.targets = $targets,
						r.envVars = $envVars,
						r.tags = $tags,
						r.image = CASE WHEN $image <> '' THEN $image ELSE null END,
    					e.pull = CASE WHEN $pull <> '' THEN $pull ELSE null END,
    					r.workdir = CASE WHEN $workdir <> '' THEN $workdir ELSE null END,
    					e.command = CASE WHEN $command <> '' THEN $command ELSE null END
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
					"description":           ev.Metadata.Description,
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
					"pull":                  ev.Metadata.Build.Pull,
					"command":               ev.Metadata.Build.Command,
					"workdir":               ev.Metadata.Build.Workdir,
					"tags":                  string(evTagsJSON),
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create trigger event link for %s: %w", eventName, err)
				}

				// Event Labels
				labelsList := convertMapToList(ev.Metadata.Labels)
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

		// Entrypoints
		for key, ep := range chart.Chart.Entrypoints {

			tagsJSON, _ := json.Marshal(ep.Metadata.Tags)

			queryEP := `
				MERGE (ep:Entrypoint {hash: $hash})
				ON CREATE SET ep.id = $id
				SET
					ep.name = $name,
					ep.prefix = $prefix,
					ep.topic = $topic,
					ep.description = $description,
					ep.disableVirtualization = $disableVirtualization,
					ep.runDetached = $runDetached,
					ep.removeOnStop = $removeOnStop,
					ep.memory = $memory,
					ep.kernelArgs = $kernelArgs,
					ep.networks = $networks,
					ep.ports = $ports,
					ep.volumes = $volumes,
					ep.targets = $targets,
					ep.envVars = $envVars,
					ep.tags = $tags,
					ep.image = $image
			`
			_, err := tx.Run(ctx, queryEP, map[string]any{
				"id":                    ep.Metadata.Id,
				"hash":                  ep.Metadata.Hash,
				"name":                  ep.Metadata.Name,
				"image":                 ep.Metadata.Image,
				"prefix":                ep.Metadata.Prefix,
				"topic":                 ep.Metadata.Topic,
				"description":           ep.Metadata.Description,
				"disableVirtualization": ep.Control.DisableVirtualization,
				"runDetached":           ep.Control.RunDetached,
				"removeOnStop":          ep.Control.RemoveOnStop,
				"memory":                ep.Control.Memory,
				"kernelArgs":            ep.Control.KernelArgs,
				"networks":              ep.Features.Networks,
				"ports":                 ep.Features.Ports,
				"volumes":               ep.Features.Volumes,
				"targets":               ep.Features.Targets,
				"envVars":               ep.Features.EnvVars,
				"tags":                  string(tagsJSON),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create Entrypoint node for %s: %w", key, err)
			}

			switch {
			case ep.Command != nil:
				queryLink := `
					MATCH (ep:Entrypoint {hash: $epHash})
					MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(v:Version {schemaVersion: $schemaVersion})
					OPTIONAL MATCH (v)-[spRel:HAS_PROCEDURE]->(sp:StoredProcedure)
					WHERE spRel.name = $destination
					OPTIONAL MATCH (v)-[trRel:HAS_TRIGGER]->(tr:Trigger)
					WHERE trRel.name = $destination
					WITH ep, CASE WHEN sp IS NOT NULL THEN sp ELSE tr END AS target
					WHERE target IS NOT NULL
					MERGE (ep)-[l:DESTINATION]->(target)
					SET l.params = $params, l.path = $path, l.type = $type
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"epHash":        ep.Metadata.Hash,
					"chartId":       chart.Metadata.Id,
					"schemaVersion": chart.SchemaVersion,
					"destination":   ep.Command.Destination,
					"params":        ep.Command.Metadata.Params,
					"path":          ep.Command.Metadata.Path,
					"type":          ep.Command.Metadata.Type,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create command link for entrypoint %s: %w", key, err)
				}

			case ep.EntryPoint != nil:
				queryLink := `
					MATCH (ep:Entrypoint {hash: $epHash})
					MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(v:Version {schemaVersion: $schemaVersion})
					OPTIONAL MATCH (v)-[spRel:HAS_PROCEDURE]->(sp:StoredProcedure)
					WHERE spRel.name = $destination
					OPTIONAL MATCH (v)-[trRel:HAS_TRIGGER]->(tr:Trigger)
					WHERE trRel.name = $destination
					WITH ep, CASE WHEN sp IS NOT NULL THEN sp ELSE tr END AS target
					WHERE target IS NOT NULL
					MERGE (ep)-[l:DESTINATION]->(target)
					SET l.path = $path, l.type = $type
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"epHash":        ep.Metadata.Hash,
					"chartId":       chart.Metadata.Id,
					"schemaVersion": chart.SchemaVersion,
					"destination":   ep.EntryPoint.Destination,
					"path":          ep.EntryPoint.Metadata.Path,
					"type":          ep.EntryPoint.Metadata.Type,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create entrypoint link for entrypoint %s: %w", key, err)
				}

			case ep.Run != nil:
				queryLink := `
					MATCH (ep:Entrypoint {hash: $epHash})
					MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(v:Version {schemaVersion: $schemaVersion})
					OPTIONAL MATCH (v)-[spRel:HAS_PROCEDURE]->(sp:StoredProcedure)
					WHERE spRel.name = $destination
					OPTIONAL MATCH (v)-[trRel:HAS_TRIGGER]->(tr:Trigger)
					WHERE trRel.name = $destination
					WITH ep, CASE WHEN sp IS NOT NULL THEN sp ELSE tr END AS target
					WHERE target IS NOT NULL
					MERGE (ep)-[l:DESTINATION]->(target)
					SET l.result = $result
				`
				_, err := tx.Run(ctx, queryLink, map[string]any{
					"epHash":        ep.Metadata.Hash,
					"chartId":       chart.Metadata.Id,
					"schemaVersion": chart.SchemaVersion,
					"destination":   ep.Run.Destination,
					"result":        ep.Run.Metadata.Result,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to create run link for entrypoint %s: %w", key, err)
				}

			default:
				log.Printf("entrypoint %q: no link set (Command/EntryPoint/Run all nil) - no DESTINATION created", key)
			}

			// Entrypoint Labels
			labelsList := convertMapToList(ep.Metadata.Labels)
			if len(labelsList) > 0 {
				queryLabels := `
					MATCH (ep:Entrypoint {hash: $hash})
					UNWIND $labels AS lbl
					MERGE (l:Label {key: lbl.key, value: lbl.value})
					MERGE (ep)-[:HAS_LABEL]->(l)
				`
				_, err = tx.Run(ctx, queryLabels, map[string]any{
					"hash":   ep.Metadata.Hash,
					"labels": labelsList,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to link labels to Entrypoint: %w", err)
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
