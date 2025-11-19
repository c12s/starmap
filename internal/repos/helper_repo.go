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

func parseStoredProcedures(ctx context.Context, tx neo4j.ManagedTransaction, v any) map[string]*domain.StoredProcedure {
	result := make(map[string]*domain.StoredProcedure)
	for _, node := range processNodes(v) {
		sp := &domain.StoredProcedure{
			Metadata: domain.Metadata{
				Id:     getStringProp(node, "id"),
				Name:   getStringProp(node, "name"),
				Image:  getStringProp(node, "image"),
				Hash:   getStringProp(node, "hash"),
				Prefix: getStringProp(node, "prefix"),
				Topic:  getStringProp(node, "topic"),
			},
			Control: domain.Control{
				DisableVirtualization: getBoolProp(node, "disableVirtualization"),
				RunDetached:           getBoolProp(node, "runDetached"),
				RemoveOnStop:          getBoolProp(node, "removeOnStop"),
				Memory:                getStringProp(node, "memory"),
				KernelArgs:            getStringProp(node, "kernelArgs"),
			},
			Features: domain.Features{
				Networks: getStringSliceProp(node, "networks"),
				Ports:    getStringSliceProp(node, "ports"),
				Volumes:  getStringSliceProp(node, "volumes"),
				Targets:  getStringSliceProp(node, "targets"),
				EnvVars:  getStringSliceProp(node, "envVars"),
			},
		}
		sp.Links = getLinksForNode(ctx, tx, "StoredProcedure", sp.Metadata.Id)
		if sp.Metadata.Name != "" {
			result[sp.Metadata.Name] = sp
		}
	}
	return result
}

func parseEvents(v any) map[string]*domain.Event {
	result := make(map[string]*domain.Event)
	for _, node := range processNodes(v) {
		ev := &domain.Event{
			Metadata: domain.Metadata{
				Id:     getStringProp(node, "id"),
				Name:   getStringProp(node, "name"),
				Image:  getStringProp(node, "image"),
				Hash:   getStringProp(node, "hash"),
				Prefix: getStringProp(node, "prefix"),
				Topic:  getStringProp(node, "topic"),
			},
			Control: domain.Control{
				DisableVirtualization: getBoolProp(node, "disableVirtualization"),
				RunDetached:           getBoolProp(node, "runDetached"),
				RemoveOnStop:          getBoolProp(node, "removeOnStop"),
				Memory:                getStringProp(node, "memory"),
				KernelArgs:            getStringProp(node, "kernelArgs"),
			},
			Features: domain.Features{
				Networks: getStringSliceProp(node, "networks"),
				Ports:    getStringSliceProp(node, "ports"),
				Volumes:  getStringSliceProp(node, "volumes"),
				Targets:  getStringSliceProp(node, "targets"),
				EnvVars:  getStringSliceProp(node, "envVars"),
			},
		}
		if ev.Metadata.Name != "" {
			result[ev.Metadata.Name] = ev
		}
	}
	return result
}

func parseTriggers(ctx context.Context, tx neo4j.ManagedTransaction, v any) map[string]*domain.EventTrigger {
	result := make(map[string]*domain.EventTrigger)
	for _, node := range processNodes(v) {
		tr := &domain.EventTrigger{
			Metadata: domain.Metadata{
				Id:     getStringProp(node, "id"),
				Name:   getStringProp(node, "name"),
				Image:  getStringProp(node, "image"),
				Hash:   getStringProp(node, "hash"),
				Prefix: getStringProp(node, "prefix"),
				Topic:  getStringProp(node, "topic"),
			},
			Control: domain.Control{
				DisableVirtualization: getBoolProp(node, "disableVirtualization"),
				RunDetached:           getBoolProp(node, "runDetached"),
				RemoveOnStop:          getBoolProp(node, "removeOnStop"),
				Memory:                getStringProp(node, "memory"),
				KernelArgs:            getStringProp(node, "kernelArgs"),
			},
			Features: domain.Features{
				Networks: getStringSliceProp(node, "networks"),
				Ports:    getStringSliceProp(node, "ports"),
				Volumes:  getStringSliceProp(node, "volumes"),
				Targets:  getStringSliceProp(node, "targets"),
				EnvVars:  getStringSliceProp(node, "envVars"),
			},
		}
		tr.Links = getLinksForNode(ctx, tx, "Trigger", tr.Metadata.Id)
		if tr.Metadata.Name != "" {
			result[tr.Metadata.Name] = tr
		}
	}
	return result
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

func getBoolProp(node neo4j.Node, key string) bool {
	if v, ok := node.Props[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func getStringSliceProp(node neo4j.Node, key string) []string {
	if v, ok := node.Props[key]; ok {
		switch val := v.(type) {
		case []interface{}:
			out := make([]string, 0, len(val))
			for _, item := range val {
				if s, ok := item.(string); ok {
					out = append(out, s)
				}
			}
			return out
		case []string:
			return val
		}
	}
	return []string{}
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
