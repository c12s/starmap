package repos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/c12s/starmap/internal/domain"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func computeHash(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func parseDataSources(v any, dsLabels map[string]map[string]string) map[string]*domain.DataSource {
	result := make(map[string]*domain.DataSource)
	if v == nil {
		return result
	}

	items, ok := v.([]interface{})
	if !ok {
		return result
	}

	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		nodeRaw, ok := m["node"]
		if !ok || nodeRaw == nil {
			continue
		}
		node, ok := nodeRaw.(neo4j.Node)
		if !ok {
			continue
		}

		relProps, _ := m["relProps"].(map[string]any)

		ds := &domain.DataSource{
			Id:           getStringProp(node, "id"),
			Name:         getStringProp(node, "name"),
			Type:         getStringProp(node, "type"),
			Path:         getStringProp(node, "path"),
			Hash:         getStringProp(node, "hash"),
			ResourceName: getStringFromMap(relProps, "resourceName"),
			Description:  getStringFromMap(relProps, "description"),
		}

		ds.Labels = dsLabels[ds.Id]
		if ds.Name != "" {
			result[ds.Name] = ds
		}

		if tagsStr := getStringProp(node, "tags"); tagsStr != "" {
			json.Unmarshal([]byte(tagsStr), &ds.Tags)
		}
	}
	return result
}

func parseEntity(nodeProps, relProps map[string]any) (metadata domain.Metadata, control domain.Control, features domain.Features) {
	metadata = domain.Metadata{
		Id:          getStringFromMap(nodeProps, "id"),
		Name:        getStringFromMap(relProps, "name"),
		Hash:        getStringFromMap(nodeProps, "hash"),
		Prefix:      getStringFromMap(relProps, "prefix"),
		Topic:       getStringFromMap(relProps, "topic"),
		Description: getStringFromMap(relProps, "description"),
	}

	image := getStringFromMap(relProps, "image")
	if image != "" {
		metadata.Image = image
	} else {
		metadata.Build = domain.Build{
			Pull:    getStringFromMap(nodeProps, "pull"),
			Workdir: getStringFromMap(relProps, "workdir"),
			Command: getStringFromMap(nodeProps, "command"),
		}
	}

	control = domain.Control{
		DisableVirtualization: getBoolFromMap(relProps, "disableVirtualization"),
		RunDetached:           getBoolFromMap(relProps, "runDetached"),
		RemoveOnStop:          getBoolFromMap(relProps, "removeOnStop"),
		Memory:                getStringFromMap(relProps, "memory"),
		KernelArgs:            getStringFromMap(relProps, "kernelArgs"),
	}

	features = domain.Features{
		Networks: getStringSliceFromMap(relProps, "networks"),
		Ports:    getStringSliceFromMap(relProps, "ports"),
		Volumes:  getStringSliceFromMap(relProps, "volumes"),
		Targets:  getStringSliceFromMap(relProps, "targets"),
		EnvVars:  getStringSliceFromMap(relProps, "envVars"),
	}

	if tagsStr := getStringFromMap(relProps, "tags"); tagsStr != "" {
		json.Unmarshal([]byte(tagsStr), &metadata.Tags)
	}

	return
}

func parseStoredProcedures(ctx context.Context, tx neo4j.ManagedTransaction, v any, spLabels map[string]map[string]string) map[string]*domain.StoredProcedure {
	result := make(map[string]*domain.StoredProcedure)
	if v == nil {
		return result
	}

	combinedList, ok := v.([]interface{})
	if !ok {
		return result
	}

	for _, item := range combinedList {
		entityMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		nodeProps, ok := entityMap["nodeProps"].(map[string]any)
		if !ok {
			continue
		}
		relProps, ok := entityMap["relProps"].(map[string]any)
		if !ok {
			continue
		}

		metadata, control, features := parseEntity(nodeProps, relProps)

		sp := &domain.StoredProcedure{
			Metadata: metadata,
			Control:  control,
			Features: features,
			Links:    getLinksForNode(ctx, tx, "StoredProcedure", metadata.Id),
		}

		if spLabels != nil {
			sp.Metadata.Labels = spLabels[sp.Metadata.Id]
		}

		result[metadata.Name] = sp
	}

	return result
}

func parseTriggers(ctx context.Context, tx neo4j.ManagedTransaction, v any, trLabels map[string]map[string]string) map[string]*domain.EventTrigger {
	result := make(map[string]*domain.EventTrigger)
	if v == nil {
		return result
	}

	combinedList, ok := v.([]interface{})
	if !ok {
		return result
	}

	for _, item := range combinedList {
		entityMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		nodeProps, ok := entityMap["nodeProps"].(map[string]any)
		if !ok {
			continue
		}
		relProps, ok := entityMap["relProps"].(map[string]any)
		if !ok {
			continue
		}

		metadata, control, features := parseEntity(nodeProps, relProps)

		tr := &domain.EventTrigger{
			Metadata: metadata,
			Control:  control,
			Features: features,
			Links:    getLinksForNode(ctx, tx, "Trigger", metadata.Id),
		}

		if trLabels != nil {
			tr.Metadata.Labels = trLabels[tr.Metadata.Id]
		}
		tr.Metadata.Hash = getStringFromMap(relProps, "hash")

		result[metadata.Name] = tr
	}

	return result
}

func parseEvents(v any, evLabels map[string]map[string]string) map[string]*domain.Event {
	result := make(map[string]*domain.Event)
	if v == nil {
		return result
	}

	combinedList, ok := v.([]interface{})
	if !ok {
		return result
	}

	for _, item := range combinedList {
		entityMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		nodeProps, ok := entityMap["nodeProps"].(map[string]any)
		if !ok {
			continue
		}

		relProps, ok := entityMap["relProps"].(map[string]any)
		if !ok {
			continue
		}

		metadata, control, features := parseEntity(nodeProps, relProps)

		ev := &domain.Event{
			Metadata: metadata,
			Control:  control,
			Features: features,
		}

		if evLabels != nil {
			ev.Metadata.Labels = evLabels[ev.Metadata.Id]
		}

		result[metadata.Name] = ev
	}

	return result
}

func parseEntrypoints(v any, epLabels map[string]map[string]string) map[string]*domain.Entrypoint {
	result := make(map[string]*domain.Entrypoint)
	if v == nil {
		return result
	}

	combinedList, ok := v.([]interface{})
	if !ok {
		return result
	}

	for _, item := range combinedList {
		entityMap, ok := item.(map[string]any)
		if !ok {
			continue
		}

		nodeProps, ok := entityMap["nodeProps"].(map[string]any)
		if !ok {
			continue
		}
		relProps, _ := entityMap["relProps"].(map[string]any)
		destination, _ := entityMap["destination"].(string)

		metadata := domain.Metadata{
			Id:          getStringFromMap(nodeProps, "id"),
			Name:        getStringFromMap(nodeProps, "name"),
			Hash:        getStringFromMap(nodeProps, "hash"),
			Prefix:      getStringFromMap(nodeProps, "prefix"),
			Topic:       getStringFromMap(nodeProps, "topic"),
			Description: getStringFromMap(nodeProps, "description"),
		}

		image := getStringFromMap(nodeProps, "image")
		if image != "" {
			metadata.Image = image
		}

		if tagsStr := getStringFromMap(nodeProps, "tags"); tagsStr != "" {
			json.Unmarshal([]byte(tagsStr), &metadata.Tags)
		}

		control := domain.Control{
			DisableVirtualization: getBoolFromMap(nodeProps, "disableVirtualization"),
			RunDetached:           getBoolFromMap(nodeProps, "runDetached"),
			RemoveOnStop:          getBoolFromMap(nodeProps, "removeOnStop"),
			Memory:                getStringFromMap(nodeProps, "memory"),
			KernelArgs:            getStringFromMap(nodeProps, "kernelArgs"),
		}

		features := domain.Features{
			Networks: getStringSliceFromMap(nodeProps, "networks"),
			Ports:    getStringSliceFromMap(nodeProps, "ports"),
			Volumes:  getStringSliceFromMap(nodeProps, "volumes"),
			Targets:  getStringSliceFromMap(nodeProps, "targets"),
			EnvVars:  getStringSliceFromMap(nodeProps, "envVars"),
		}

		if epLabels != nil {
			metadata.Labels = epLabels[metadata.Id]
		}

		ep := &domain.Entrypoint{
			Metadata: metadata,
			Control:  control,
			Features: features,
		}

		_, hasResult := relProps["result"]
		_, hasParams := relProps["params"]

		switch {
		case hasResult:
			ep.Run = &domain.RunLink{
				Destination: destination,
				Metadata: domain.RunLinkMetadata{
					Result: getStringFromMap(relProps, "result"),
				},
			}
		case hasParams:
			ep.Command = &domain.CommandLink{
				Destination: destination,
				Metadata: domain.CommandLinkMetadata{
					Params: getStringFromMap(relProps, "params"),
					Path:   getStringFromMap(relProps, "path"),
					Type:   getStringFromMap(relProps, "type"),
				},
			}
		default:
			ep.EntryPoint = &domain.EntrypointLink{
				Destination: destination,
				Metadata: domain.EntrypointLinkMetadata{
					Path: getStringFromMap(relProps, "path"),
					Type: getStringFromMap(relProps, "type"),
				},
			}
		}

		result[metadata.Name] = ep
	}

	return result
}

func getStringFromMap(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getBoolFromMap(m map[string]any, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func getStringSliceFromMap(m map[string]any, key string) []string {
	if v, ok := m[key]; ok {
		if arr, ok := v.([]interface{}); ok {
			res := make([]string, 0, len(arr))
			for _, e := range arr {
				if s, ok := e.(string); ok {
					res = append(res, s)
				}
			}
			return res
		}
	}
	return nil
}

func getLinksForNode(ctx context.Context, tx neo4j.ManagedTransaction, nodeLabel, nodeID string) domain.Links {
	links := domain.Links{
		HardLinks:  []string{},
		SoftLinks:  []string{},
		EventLinks: []string{},
	}

	query := fmt.Sprintf(`
		MATCH (n:%s {id: $id})
		OPTIONAL MATCH (n)-[:HARD_LINK]->(ds1:DataSource)
		OPTIONAL MATCH (n)-[:SOFT_LINK]->(ds2:DataSource)
		OPTIONAL MATCH (n)-[e:EVENT_LINK]->(:Event)
		RETURN 
			collect(DISTINCT ds1.name) as hard,
			collect(DISTINCT ds2.name) as soft,
			collect(DISTINCT e.name)  as events
	`, nodeLabel)

	res, err := tx.Run(ctx, query, map[string]any{"id": nodeID})
	if err != nil {
		return links
	}

	if res.Next(ctx) {
		rec := res.Record()
		if v, ok := rec.Get("hard"); ok {
			links.HardLinks = toStringSlice(v)
		}
		if v, ok := rec.Get("soft"); ok {
			links.SoftLinks = toStringSlice(v)
		}
		if v, ok := rec.Get("events"); ok {
			links.EventLinks = toStringSlice(v)
		}
	}

	return links
}

func toStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	if arr, ok := v.([]interface{}); ok {
		res := make([]string, 0, len(arr))
		for _, e := range arr {
			if s, ok := e.(string); ok {
				res = append(res, s)
			}
		}
		return res
	}
	return nil
}

func parseLabels(labelsJSON string) map[string]string {
	if labelsJSON == "" {
		return nil
	}
	var m map[string]string
	_ = json.Unmarshal([]byte(labelsJSON), &m)
	return m
}

func getStringProp(node neo4j.Node, key string) string {
	if v, ok := node.Props[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return ""
}

func convertMapToList(labels map[string]string) []map[string]string {
	result := make([]map[string]string, 0, len(labels))
	for k, v := range labels {
		result = append(result, map[string]string{
			"key":   k,
			"value": v,
		})
	}
	return result
}

func parseLabelList(v any) map[string]string {
	labels := make(map[string]string)
	if v == nil {
		return labels
	}

	if arr, ok := v.([]interface{}); ok {
		for _, item := range arr {
			if m, ok := item.(map[string]interface{}); ok {
				key, _ := m["key"].(string)
				value, _ := m["value"].(string)
				if key != "" {
					labels[key] = value
				}
			}
		}
	}
	return labels
}

func parseLabelsIntoMap(v any) map[string]map[string]string {
	result := make(map[string]map[string]string)
	list, ok := v.([]interface{})
	if !ok {
		return result
	}
	for _, item := range list {
		if item == nil {
			continue // preskoči null-ove iz CASE WHEN
		}
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id, ok := m["id"].(string)
		if !ok || id == "" {
			continue
		}
		key, _ := m["key"].(string)
		value, _ := m["value"].(string)
		if result[id] == nil {
			result[id] = make(map[string]string)
		}
		result[id][key] = value
	}
	return result
}
func incrementVersion(ver string) string {
	if !strings.HasPrefix(ver, "v") {
		return "v1.0.0"
	}

	ver = strings.TrimPrefix(ver, "v")
	parts := strings.Split(ver, ".")
	if len(parts) != 3 {
		return "v1.0.0"
	}

	major, _ := strconv.Atoi(parts[0])
	minor, _ := strconv.Atoi(parts[1])
	patch, _ := strconv.Atoi(parts[2])

	patch += 1

	return fmt.Sprintf("v%d.%d.%d", major, minor, patch)
}

func computeComponentHashes(chart *domain.StarChart) {
	for _, ds := range chart.Chart.DataSources {
		ds.Hash = computeHash(ds.Type + ds.Path)
	}
	for _, sp := range chart.Chart.StoredProcedures {
		sp.Metadata.Hash = computeLayerHash(sp.Metadata)
	}
	for _, et := range chart.Chart.EventTriggers {
		et.Metadata.Hash = computeLayerHash(et.Metadata)
	}
	for _, ev := range chart.Chart.Events {
		ev.Metadata.Hash = computeLayerHash(ev.Metadata)
	}
	for _, ep := range chart.Chart.Entrypoints {
		ep.Metadata.Hash = computeHash(ep.Metadata.Image)
	}
}

func computeLayerHash(m domain.Metadata) string {
	if m.Image != "" {
		return computeHash(m.Image)
	}
	return computeHash(m.Build.Pull + m.Build.Command)
}

func computeVersionHash(chart domain.StarChart) string {
	var hashes []string
	for _, ds := range chart.Chart.DataSources {
		hashes = append(hashes, ds.Hash)
	}
	for _, sp := range chart.Chart.StoredProcedures {
		hashes = append(hashes, sp.Metadata.Hash)
	}
	for _, et := range chart.Chart.EventTriggers {
		hashes = append(hashes, et.Metadata.Hash)
	}
	for _, ev := range chart.Chart.Events {
		hashes = append(hashes, ev.Metadata.Hash)
	}
	for _, ep := range chart.Chart.Entrypoints {
		hashes = append(hashes, ep.Metadata.Hash)
	}
	sort.Strings(hashes)
	return computeHash(strings.Join(hashes, ""))
}

func computeTriggerEventHash(triggerHash string, eventHashes []string) string {
	sorted := make([]string, len(eventHashes))
	copy(sorted, eventHashes)
	sort.Strings(sorted)
	return computeHash(triggerHash + strings.Join(sorted, ""))
}

func entrypointDestination(ep *domain.Entrypoint) string {
	switch {
	case ep.Command != nil:
		return ep.Command.Destination
	case ep.EntryPoint != nil:
		return ep.EntryPoint.Destination
	case ep.Run != nil:
		return ep.Run.Destination
	}
	return ""
}
