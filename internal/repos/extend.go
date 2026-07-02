package repos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) Extend(ctx context.Context, oldVersion string, chart domain.StarChart) (*domain.MetadataResp, error) {

	forExtend, err := r.GetChartId(ctx, oldVersion, chart.Metadata.Namespace, chart.Metadata.Maintainer, chart.Metadata.Id)
	if err != nil {
		return nil, err
	}

	computeComponentHashes(&chart)

	for k, ds := range chart.Chart.DataSources {
		for _, forExDs := range forExtend.DataSources {
			if forExDs.Hash == ds.Hash {
				delete(chart.Chart.DataSources, k)
			}
		}
	}

	for k, sp := range chart.Chart.StoredProcedures {
		for _, forExSp := range forExtend.StoredProcedures {
			if forExSp.Metadata.Hash == sp.Metadata.Hash {
				delete(chart.Chart.StoredProcedures, k)
			}
		}
	}

	for k, tr := range chart.Chart.EventTriggers {
		for _, forExTr := range forExtend.EventTriggers {
			if forExTr.Metadata.Hash == tr.Metadata.Hash {
				delete(chart.Chart.EventTriggers, k)
			}
		}
	}

	if len(chart.Chart.DataSources) == 0 && len(chart.Chart.StoredProcedures) == 0 && len(chart.Chart.EventTriggers) == 0 {
		return nil, errors.New("new version is same as existing version")
	}

	if chart.SchemaVersion == "" {
		chart.SchemaVersion = incrementVersion(forExtend.SchemaVersion)
	}

	writeSession := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer writeSession.Close(ctx)

	_, err = writeSession.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		versionHash := computeVersionHash(chart)
		tagsExtendJSON, _ := json.Marshal(chart.Metadata.Tags)
		// Version node
		queryVersion := `
			MATCH (c:Chart {id: $id})

			OPTIONAL MATCH (c)-[:HAS_VERSION]->(v_direct:Version {schemaVersion: $v1schemaVersion})
			OPTIONAL MATCH (c)-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v_extend:Version {schemaVersion: $v1schemaVersion})

			WITH c, coalesce(v_direct, v_extend) AS v1

			CREATE (v:Version {hash: $versionHash})
			SET v.schemaVersion = $schemaVersion,
				v.tags = $tags

			MERGE (v)-[r:EXTEND]->(v1)
			ON CREATE SET r.createdAt = $now
		`
		if _, err := tx.Run(ctx, queryVersion, map[string]any{
			"schemaVersion":   chart.SchemaVersion,
			"now":             time.Now().Unix(),
			"versionHash":     versionHash,
			"v1schemaVersion": forExtend.SchemaVersion,
			"maintainer":      chart.Metadata.Maintainer,
			"namespace":       chart.Metadata.Namespace,
			"id":              chart.Metadata.Id,
			"tags":            string(tagsExtendJSON),
		}); err != nil {
			return nil, fmt.Errorf("failed to create Version node: %w", err)
		}

		queryUpdateHasChart := `
			MATCH (c:Chart {id: $id})
			MATCH (n:Namespace {name: $namespace})-[hc:HAS_CHART]->(c)
			SET hc.versions = CASE
				WHEN $newSchemaVersion IN coalesce(hc.versions, []) THEN hc.versions
				ELSE coalesce(hc.versions, []) + $newSchemaVersion
			END
		`
		if _, err := tx.Run(ctx, queryUpdateHasChart, map[string]any{
			"id":               chart.Metadata.Id,
			"newSchemaVersion": chart.SchemaVersion,
			"namespace":        chart.Metadata.Namespace,
		}); err != nil {
			return nil, fmt.Errorf("failed to update HAS_CHART versions: %w", err)
		}

		// DataSources
		for key, ds := range chart.Chart.DataSources {

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

			querySP := `
				MERGE (s:StoredProcedure {hash: $hash})
				ON CREATE SET
					s.id = $id
				WITH s
				MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v:Version  {schemaVersion: $schemaVersion})
				WITH s, v
				MERGE (v)-[r:HAS_PROCEDURE]->(s)
				SET 
					r.name = $name,
					r.image = $image,
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
					r.envVars = $envVars
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
				MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v:Version  {schemaVersion: $schemaVersion})
				WITH t, v
				MERGE (v)-[r:HAS_TRIGGER]->(t)
				SET
					r.name = $name,
					r.image = $image,
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
					r.envVars = $envVars
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
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create EventTrigger node for %s: %w", key, err)
			}

			for _, hardLink := range et.Links.HardLinks {
				queryLink := `
					MATCH (t:Trigger {id: $triggerId})
					OPTIONAL MATCH (ds:DataSource {name: $dsName})
					WITH t, ds
					WHERE ds IS NOT NULL
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
					WITH t, ds
					WHERE ds IS NOT NULL
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

			// Events
			for _, eventName := range et.Links.EventLinks {
				ev, ok := eventMap[eventName]
				if !ok {
					continue
				}

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
					ep.image = CASE WHEN $image <> '' THEN $image ELSE null END,
					ep.pull = CASE WHEN $pull <> '' THEN $pull ELSE null END,
					ep.workdir = CASE WHEN $workdir <> '' THEN $workdir ELSE null END,
					ep.command = CASE WHEN $command <> '' THEN $command ELSE null END
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
				"pull":                  ep.Metadata.Build.Pull,
				"command":               ep.Metadata.Build.Command,
				"workdir":               ep.Metadata.Build.Workdir,
				"tags":                  string(tagsJSON),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create Entrypoint node for %s: %w", key, err)
			}

			switch {
			case ep.Command != nil:
				queryLink := `
					MATCH (ep:Entrypoint {hash: $epHash})
					MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v:Version {schemaVersion: $schemaVersion})
					OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
					WITH ep, collect(DISTINCT base) AS versions
					UNWIND versions AS ver
					OPTIONAL MATCH (ver)-[spRel:HAS_PROCEDURE]->(sp:StoredProcedure)
					WHERE spRel.name = $destination
					OPTIONAL MATCH (ver)-[trRel:HAS_TRIGGER]->(tr:Trigger)
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
					MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v:Version {schemaVersion: $schemaVersion})
					OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
					WITH ep, collect(DISTINCT base) AS versions
					UNWIND versions AS ver
					OPTIONAL MATCH (ver)-[spRel:HAS_PROCEDURE]->(sp:StoredProcedure)
					WHERE spRel.name = $destination
					OPTIONAL MATCH (ver)-[trRel:HAS_TRIGGER]->(tr:Trigger)
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
					MATCH (c:Chart {id: $chartId})-[:HAS_VERSION]->(:Version)<-[:EXTEND*1..]-(v:Version {schemaVersion: $schemaVersion})
					OPTIONAL MATCH (v)-[:EXTEND*0..]->(base:Version)
					WITH ep, collect(DISTINCT base) AS versions
					UNWIND versions AS ver
					OPTIONAL MATCH (ver)-[spRel:HAS_PROCEDURE]->(sp:StoredProcedure)
					WHERE spRel.name = $destination
					OPTIONAL MATCH (ver)-[trRel:HAS_TRIGGER]->(tr:Trigger)
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
			}
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return &domain.MetadataResp{
		ApiVersion:    chart.ApiVersion,
		SchemaVersion: chart.SchemaVersion,
		Kind:          chart.Kind,
		Metadata: struct {
			Id         string
			Name       string
			Namespace  string
			Maintainer string
		}{
			chart.Metadata.Id, chart.Metadata.Name, chart.Metadata.Namespace, chart.Metadata.Maintainer,
		},
	}, nil
}
