package repos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/c12s/starmap/internal/domain"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func computeHash(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func processNodes(v any) []neo4j.Node {
	nodes := []neo4j.Node{}
	if v == nil {
		return nodes
	}
	if ifaceSlice, ok := v.([]interface{}); ok {
		for _, n := range ifaceSlice {
			if node, ok := n.(neo4j.Node); ok {
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

func parseDataSources(v any) map[string]*domain.DataSource {
	result := make(map[string]*domain.DataSource)
	for _, node := range processNodes(v) {
		ds := &domain.DataSource{
			Id:           getStringProp(node, "id"),
			Name:         getStringProp(node, "name"),
			Type:         getStringProp(node, "type"),
			Path:         getStringProp(node, "path"),
			Hash:         getStringProp(node, "hash"),
			ResourceName: getStringProp(node, "resourceName"),
			Description:  getStringProp(node, "description"),
		}
		if ds.Name != "" {
			result[ds.Name] = ds
		}
	}
	return result
}

func parseEntity(nodeProps, relProps map[string]any) (metadata domain.Metadata, control domain.Control, features domain.Features) {
	metadata = domain.Metadata{
		Id:     getStringFromMap(nodeProps, "id"),
		Name:   getStringFromMap(relProps, "name"),
		Image:  getStringFromMap(relProps, "image"),
		Hash:   getStringFromMap(nodeProps, "hash"),
		Prefix: getStringFromMap(relProps, "prefix"),
		Topic:  getStringFromMap(relProps, "topic"),
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

	return
}

func parseStoredProcedures(ctx context.Context, tx neo4j.ManagedTransaction, v any) map[string]*domain.StoredProcedure {
	result := make(map[string]*domain.StoredProcedure)

	if combinedList, ok := v.([]interface{}); ok {
		for _, item := range combinedList {
			if entityMap, ok := item.(map[string]any); ok {
				nodeProps := entityMap["nodeProps"].(map[string]any)
				relProps := entityMap["relProps"].(map[string]any)

				metadata, control, features := parseEntity(nodeProps, relProps)

				sp := &domain.StoredProcedure{
					Metadata: metadata,
					Control:  control,
					Features: features,
					Links:    getLinksForNode(ctx, tx, "StoredProcedure", metadata.Id),
				}

				result[metadata.Name] = sp
			}
		}
	}

	return result
}

func parseTriggers(ctx context.Context, tx neo4j.ManagedTransaction, v any) map[string]*domain.EventTrigger {
	result := make(map[string]*domain.EventTrigger)

	if combinedList, ok := v.([]interface{}); ok {
		for _, item := range combinedList {
			if entityMap, ok := item.(map[string]any); ok {
				nodeProps := entityMap["nodeProps"].(map[string]any)
				relProps := entityMap["relProps"].(map[string]any)

				metadata, control, features := parseEntity(nodeProps, relProps)

				tr := &domain.EventTrigger{
					Metadata: metadata,
					Control:  control,
					Features: features,
					Links:    getLinksForNode(ctx, tx, "Trigger", metadata.Id),
				}

				result[metadata.Name] = tr
			}
		}
	}

	return result
}

func parseEvents(v any) map[string]*domain.Event {
	result := make(map[string]*domain.Event)

	if combinedList, ok := v.([]interface{}); ok {
		for _, item := range combinedList {
			if entityMap, ok := item.(map[string]any); ok {
				nodeProps := entityMap["nodeProps"].(map[string]any)
				relProps := entityMap["relProps"].(map[string]any)

				metadata, control, features := parseEntity(nodeProps, relProps)

				ev := &domain.Event{
					Metadata: metadata,
					Control:  control,
					Features: features,
				}

				result[metadata.Name] = ev
			}
		}
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
		OPTIONAL MATCH (n)-[:EVENT_LINK]->(e:Event)
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

func convertLabelsToList(labels map[string]string) []map[string]string {
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
