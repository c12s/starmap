package repos

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/c12s/starmap/internal/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func (r *RegistryRepo) UpdateChart(ctx context.Context, chart domain.StarChart) (*domain.MetadataResp, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	defer session.Close(ctx)

	if chart.Metadata.Namespace == "" {
		chart.Metadata.Namespace = "default"
	}

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
				c.engine = $engine,
				c.apiVersion = $apiVersion
		`
		if _, err := tx.Run(ctx, queryChart, map[string]any{
			"id":          chart.Metadata.Id,
			"name":        chart.Metadata.Name,
			"kind":        chart.Kind,
			"description": chart.Metadata.Description,
			"visibility":  chart.Metadata.Visibility,
			"engine":      chart.Metadata.Engine,
			"apiVersion":  chart.ApiVersion,
		}); err != nil {
			return nil, err
		}

		// Label
		queryDeleteChartLabels := `
			MATCH (c:Chart {id: $id})-[r:HAS_LABEL]->(:Label)
			DELETE r
		`
		tx.Run(ctx, queryDeleteChartLabels, map[string]any{"id": chart.Metadata.Id})

		if lblList := convertMapToList(chart.Metadata.Labels); len(lblList) > 0 {
			queryAddLabels := `
			UNWIND $labels AS lbl
			MATCH (c:Chart {id: $id})
			MERGE (l:Label {key: lbl.key, value: lbl.value})
			MERGE (c)-[:HAS_LABEL]->(l)
			`
			tx.Run(ctx, queryAddLabels, map[string]any{"id": chart.Metadata.Id, "labels": lblList})
		}

		// Version
		schemaVersion := chart.SchemaVersion

		if schemaVersion == "" {
			queryLastVersion := `
        MATCH (c:Chart {id: $id})-[r:HAS_VERSION]->(root:Version)
        OPTIONAL MATCH (root)<-[re:EXTEND*0..]-(v:Version)
        WITH c, v, r, 
            CASE
                WHEN re IS NULL OR size(re) = 0 THEN r.createdAt
                ELSE last(re).createdAt
            END AS versionCreatedAt
        ORDER BY versionCreatedAt DESC
        LIMIT 1
        RETURN v.schemaVersion AS schemaVersion
    `
			res, err := tx.Run(ctx, queryLastVersion, map[string]any{
				"id": chart.Metadata.Id,
			})
			if err != nil {
				return nil, err
			}

			if res.Next(ctx) {
				lastSchema, _ := res.Record().Get("schemaVersion")
				schemaVersion = incrementVersion(lastSchema.(string))
			} else {
				schemaVersion = "v1.0.0"
			}
		} else {
			queryFindLeaf := `
			MATCH (c:Chart {id: $id})-[:HAS_VERSION]->(root:Version)
			OPTIONAL MATCH (root)<-[:EXTEND*0..]-(target:Version {schemaVersion: $schemaVersion})
			OPTIONAL MATCH (target)<-[:EXTEND*1..]-(leaf:Version)
			WHERE NOT (:Version)-[:EXTEND]->(leaf)
			RETURN 
				target IS NOT NULL AS versionExists,
				coalesce(leaf.schemaVersion, target.schemaVersion) AS resolvedVersion
			`
			res, err := tx.Run(ctx, queryFindLeaf, map[string]any{
				"id":            chart.Metadata.Id,
				"schemaVersion": schemaVersion,
			})
			if err != nil {
				return nil, err
			}

			if !res.Next(ctx) {
				return nil, fmt.Errorf("version %s not found for chart %s", schemaVersion, chart.Metadata.Id)
			}

			record := res.Record()

			versionExists, _ := record.Get("versionExists")
			if versionExists == nil || !versionExists.(bool) {
				return nil, fmt.Errorf("version %s not found for chart %s", schemaVersion, chart.Metadata.Id)
			}

			resolved, _ := record.Get("resolvedVersion")
			if resolved != nil {
				schemaVersion = resolved.(string)
			}
		}

		computeComponentHashes(&chart)
		versionHash := computeVersionHash(chart)
		tagsJSON, _ := json.Marshal(chart.Metadata.Tags)
		queryMergeVersion := `
			MATCH (u:User {name: $maintainer})-[:HAS_NAMESPACE]->(n:Namespace {name: $namespace})-[:HAS_CHART]->(c:Chart {id: $id})
			MERGE (c)-[r:HAS_VERSION]->(v:Version {schemaVersion: $schemaVersion})
			SET r.createdAt = $createdAt,
				v.hash = $versionHash,
				v.tags = $tags

			WITH c, v, n

			MATCH (n)-[hc:HAS_CHART]->(c)
			SET hc.versions = CASE
				WHEN $schemaVersion IN coalesce(hc.versions, []) THEN hc.versions
				ELSE coalesce(hc.versions, []) + $schemaVersion
			END

			RETURN DISTINCT v
		`
		result, err := tx.Run(ctx, queryMergeVersion, map[string]any{
			"id":            chart.Metadata.Id,
			"schemaVersion": schemaVersion,
			"createdAt":     time.Now().Unix(),
			"namespace":     chart.Metadata.Namespace,
			"maintainer":    chart.Metadata.Maintainer,
			"versionHash":   versionHash,
			"tags":          string(tagsJSON),
		})
		if err != nil {
			return nil, err
		}

		_, err = result.Consume(ctx)
		if err != nil {
			return nil, err
		}
		queryDeleteVersion := `
			MATCH (v:Version {schemaVersion: $schemaVersion})
			WHERE NOT (:Chart)-[:HAS_VERSION]->(v)
			DETACH DELETE v
		`
		tx.Run(ctx, queryDeleteVersion, map[string]any{
			"schemaVersion": chart.SchemaVersion,
		})

		// DataSource
		for _, ds := range chart.Chart.DataSources {

			queryDS := `
				MERGE (d:DataSource {hash: $hash})
				SET d.id = $id,
					d.name = $name,
					d.type = $type,
					d.path = $path,
					d.hash = $hash
			`
			tx.Run(ctx, queryDS, map[string]any{
				"id":   ds.Id,
				"name": ds.Name,
				"type": ds.Type,
				"path": ds.Path,
				"hash": ds.Hash,
			})

			queryDeleteDSLabels := `
				MATCH (d:DataSource {id: $id})-[r:HAS_LABEL]->(:Label)
				DELETE r
			`
			tx.Run(ctx, queryDeleteDSLabels, map[string]any{"id": ds.Id})

			if lblList := convertMapToList(ds.Labels); len(lblList) > 0 {
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
				MATCH (v:Version {schemaVersion: $schemaVersion})
				MATCH (s:StoredProcedure {id: $id})
				MERGE (v)-[r:HAS_PROCEDURE]->(s)
				SET r.name = $name,
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
				    r.image = CASE WHEN $image <> '' THEN $image ELSE null END,
    				s.pull = CASE WHEN $pull <> '' THEN $pull ELSE null END,
    				r.workdir = CASE WHEN $workdir <> '' THEN $workdir ELSE null END,
    				s.command = CASE WHEN $command <> '' THEN $command ELSE null END
			`
			tx.Run(ctx, queryRel, map[string]any{
				"id":                    sp.Metadata.Id,
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
				"schemaVersion":         schemaVersion,
				"pull":                  sp.Metadata.Build.Pull,
				"command":               sp.Metadata.Build.Command,
				"workdir":               sp.Metadata.Build.Workdir,
			})

			for _, hl := range sp.Links.HardLinks {
				tx.Run(ctx, `
					MATCH (s:StoredProcedure {id: $spId})
					OPTIONAL MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[hl:HARD_LINK]->(d)
					SET hl.resourceName = d.resourceName,
						hl.description = d.description
				`, map[string]any{"spId": sp.Metadata.Id, "dsName": hl})
			}

			for _, sl := range sp.Links.SoftLinks {
				tx.Run(ctx, `
					MATCH (s:StoredProcedure {id: $spId})
					OPTIONAL MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[sl:SOFT_LINK]->(d)
					SET sl.resourceName = d.resourceName,
						sl.description = d.description
				`, map[string]any{"spId": sp.Metadata.Id, "dsName": sl})
			}

			queryDeleteSPLabels := `
				MATCH (s:StoredProcedure {id: $id})-[r:HAS_LABEL]->(:Label)
				DELETE r
			`
			tx.Run(ctx, queryDeleteSPLabels, map[string]any{"id": sp.Metadata.Id})

			if lblList := convertMapToList(sp.Metadata.Labels); len(lblList) > 0 {
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
			eventMap[ev.Metadata.Name] = ev
		}

		// Trigger
		for _, tr := range chart.Chart.EventTriggers {

			querySP := `
				MERGE (t:Trigger {id: $id})
				SET t.hash = $hash
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
				MATCH (v:Version {schemaVersion: $schemaVersion})
				MATCH (s:Trigger {id: $id})
				MERGE (v)-[r:HAS_TRIGGER]->(s)
				SET r.name = $name,
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
					r.image = CASE WHEN $image <> '' THEN $image ELSE null END,
    				s.pull = CASE WHEN $pull <> '' THEN $pull ELSE null END,
    				r.workdir = CASE WHEN $workdir <> '' THEN $workdir ELSE null END,
    				s.command = CASE WHEN $command <> '' THEN $command ELSE null END
			`
			tx.Run(ctx, queryRel, map[string]any{
				"id":                    tr.Metadata.Id,
				"name":                  tr.Metadata.Name,
				"image":                 tr.Metadata.Image,
				"prefix":                tr.Metadata.Prefix,
				"topic":                 tr.Metadata.Topic,
				"description":           tr.Metadata.Description,
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
				"schemaVersion":         schemaVersion,
				"pull":                  tr.Metadata.Build.Pull,
				"command":               tr.Metadata.Build.Command,
				"workdir":               tr.Metadata.Build.Workdir,
			})

			for _, hl := range tr.Links.HardLinks {
				tx.Run(ctx, `
					MATCH (s:Trigger {id: $spId})
					OPTIONAL MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[hl:HARD_LINK]->(d)
					SET hl.resourceName = d.resourceName,
						hl.description = d.description
					REMOVE d.resourceName, d.description
				`, map[string]any{"spId": tr.Metadata.Id, "dsName": hl})
			}

			for _, sl := range tr.Links.SoftLinks {
				tx.Run(ctx, `
					MATCH (s:Trigger {id: $spId})
					OPTIONAL MATCH (d:DataSource {name: $dsName})
					MERGE (s)-[sl:SOFT_LINK]->(d)
					SET sl.resourceName = d.resourceName,
						sl.description = d.description
					REMOVE d.resourceName, d.description
				`, map[string]any{"spId": tr.Metadata.Id, "dsName": sl})
			}

			queryDeleteSPLabels := `
				MATCH (s:Trigger {id: $id})-[r:HAS_LABEL]->(:Label)
				DELETE r
			`
			tx.Run(ctx, queryDeleteSPLabels, map[string]any{"id": tr.Metadata.Id})

			if lblList := convertMapToList(tr.Metadata.Labels); len(lblList) > 0 {
				queryLabels := `
					MATCH (d:Trigger {id: $id})
					UNWIND $labels AS lbl
					MERGE (l:Label {key: lbl.key, value: lbl.value})
					MERGE (d)-[:HAS_LABEL]->(l)
				`
				tx.Run(ctx, queryLabels, map[string]any{"id": tr.Metadata.Id, "labels": lblList})
			}

			// Event
			for _, eventName := range tr.Links.EventLinks {
				ev, ok := eventMap[eventName]
				if !ok {
					continue
				}

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

				_, err = tx.Run(ctx, `
				MATCH (t:Trigger {id: $triggerId})
				MATCH (e:Event {id: $eventId})
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
					r.image = CASE WHEN $image <> '' THEN $image ELSE null END,
    				e.pull = CASE WHEN $pull <> '' THEN $pull ELSE null END,
    				r.workdir = CASE WHEN $workdir <> '' THEN $workdir ELSE null END,
    				e.command = CASE WHEN $command <> '' THEN $command ELSE null END
				`, map[string]any{
					"triggerId":             tr.Metadata.Id,
					"eventId":               ev.Metadata.Id,
					"name":                  ev.Metadata.Name,
					"image":                 ev.Metadata.Image,
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
				evLbl := convertMapToList(ev.Metadata.Labels)
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

		// Entrypoint
		for _, ep := range chart.Chart.Entrypoints {

			tagsJSON, _ := json.Marshal(ep.Metadata.Tags)

			queryEP := `
				MERGE (ep:Entrypoint {id: $id})
				SET ep.hash = $hash,
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
			tx.Run(ctx, queryEP, map[string]any{
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

			queryDeleteEP := `
				MATCH (ep:Entrypoint {id: $id})-[r]->()
				DELETE r
			`
			tx.Run(ctx, queryDeleteEP, map[string]any{"id": ep.Metadata.Id})

			switch {
			case ep.Command != nil:
				queryLink := `
					MATCH (ep:Entrypoint {id: $id})
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
				tx.Run(ctx, queryLink, map[string]any{
					"id":            ep.Metadata.Id,
					"chartId":       chart.Metadata.Id,
					"schemaVersion": schemaVersion,
					"destination":   ep.Command.Destination,
					"params":        ep.Command.Metadata.Params,
					"path":          ep.Command.Metadata.Path,
					"type":          ep.Command.Metadata.Type,
				})
			case ep.EntryPoint != nil:
				queryLink := `
					MATCH (ep:Entrypoint {id: $id})
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
				tx.Run(ctx, queryLink, map[string]any{
					"id":            ep.Metadata.Id,
					"chartId":       chart.Metadata.Id,
					"schemaVersion": schemaVersion,
					"destination":   ep.EntryPoint.Destination,
					"path":          ep.EntryPoint.Metadata.Path,
					"type":          ep.EntryPoint.Metadata.Type,
				})
			case ep.Run != nil:
				queryLink := `
					MATCH (ep:Entrypoint {id: $id})
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
				tx.Run(ctx, queryLink, map[string]any{
					"id":            ep.Metadata.Id,
					"chartId":       chart.Metadata.Id,
					"schemaVersion": schemaVersion,
					"destination":   ep.Run.Destination,
					"result":        ep.Run.Metadata.Result,
				})
			}

			queryDeleteAlone := `
				MATCH (ep:Entrypoint {id: $id})
				WHERE NOT (ep)-[:DESTINATION]->()
				DETACH DELETE ep
			`
			tx.Run(ctx, queryDeleteAlone, map[string]any{"id": ep.Metadata.Id})
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
