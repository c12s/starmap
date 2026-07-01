package repos

import (
	"context"
	"fmt"

	"github.com/c12s/starmap/internal/domain"
)

func (r *RegistryRepo) SwitchCheckpoint(ctx context.Context, namespace, maintainer, chartId, oldVersion, newVersion string, layers []string) (*domain.SwitchCheckpointResp, error) {

	old, err := r.GetChartId(ctx, oldVersion, namespace, maintainer, chartId)
	if err != nil {
		return nil, fmt.Errorf("old version chart not found")
	}

	new, err := r.GetChartId(ctx, newVersion, namespace, maintainer, chartId)
	if err != nil {
		return nil, fmt.Errorf("new version chart not found")
	}

	// layers hash set
	layerSet := make(map[string]struct{})
	for _, h := range layers {
		layerSet[h] = struct{}{}
	}

	resp := &domain.SwitchCheckpointResp{}

	resp.Start.DataSources = map[string]*domain.DataSource{}
	resp.Start.StoredProcedures = map[string]*domain.StoredProcedure{}
	resp.Start.EventTriggers = map[string]*domain.EventTrigger{}
	resp.Start.Events = map[string]*domain.Event{}
	resp.Start.Entrypoints = map[string]*domain.Entrypoint{}

	resp.Stop.DataSources = map[string]*domain.DataSource{}
	resp.Stop.StoredProcedures = map[string]*domain.StoredProcedure{}
	resp.Stop.EventTriggers = map[string]*domain.EventTrigger{}
	resp.Stop.Events = map[string]*domain.Event{}
	resp.Stop.Entrypoints = map[string]*domain.Entrypoint{}

	resp.Download.DataSources = map[string]*domain.DataSource{}
	resp.Download.StoredProcedures = map[string]*domain.StoredProcedure{}
	resp.Download.EventTriggers = map[string]*domain.EventTrigger{}
	resp.Download.Events = map[string]*domain.Event{}
	resp.Download.Entrypoints = map[string]*domain.Entrypoint{}

	// Data Sources
	oldDSHashes := map[string]*domain.DataSource{}
	for _, ds := range old.DataSources {
		oldDSHashes[ds.Hash] = ds
	}
	newDSHashes := map[string]*domain.DataSource{}
	for _, ds := range new.DataSources {
		newDSHashes[ds.Hash] = ds
	}

	for h, ds := range newDSHashes {
		if _, exists := oldDSHashes[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Start.DataSources[h] = ds
			} else {
				resp.Download.DataSources[h] = ds
			}
		}
	}

	for h, ds := range oldDSHashes {
		if _, exists := newDSHashes[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Stop.DataSources[h] = ds
			}
		}
	}

	// Entrypoints
	oldEpByDest := map[string]*domain.Entrypoint{}
	for _, ep := range old.Entrypoints {
		if dest := entrypointDestination(ep); dest != "" {
			oldEpByDest[dest] = ep
		}
	}
	newEpByDest := map[string]*domain.Entrypoint{}
	for _, ep := range new.Entrypoints {
		if dest := entrypointDestination(ep); dest != "" {
			newEpByDest[dest] = ep
		}
	}

	attachEp := func(dst map[string]*domain.Entrypoint, name string, src map[string]*domain.Entrypoint) {
		if ep, ok := src[name]; ok {
			dst[ep.Metadata.Hash] = ep
		}
	}

	// Stored Procedures
	oldSP := map[string]*domain.StoredProcedure{}
	for _, sp := range old.StoredProcedures {
		oldSP[sp.Metadata.Hash] = sp
	}

	newSP := map[string]*domain.StoredProcedure{}
	for _, sp := range new.StoredProcedures {
		newSP[sp.Metadata.Hash] = sp
	}

	for h, sp := range newSP {
		if _, exists := oldSP[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Start.StoredProcedures[h] = sp
				attachEp(resp.Start.Entrypoints, sp.Metadata.Name, newEpByDest)
			} else {
				resp.Download.StoredProcedures[h] = sp
				attachEp(resp.Download.Entrypoints, sp.Metadata.Name, newEpByDest)
			}
		}
	}

	for h, sp := range oldSP {
		if _, exists := newSP[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Stop.StoredProcedures[h] = sp
				attachEp(resp.Stop.Entrypoints, sp.Metadata.Name, oldEpByDest)
			}
		}
	}

	// Event Trigger
	oldET := map[string]*domain.EventTrigger{}
	for _, et := range old.EventTriggers {
		oldET[computeHash(et.Metadata.Image)] = et
	}
	newET := map[string]*domain.EventTrigger{}
	for _, et := range new.EventTriggers {
		newET[computeHash(et.Metadata.Image)] = et
	}

	for h, et := range newET {
		if _, exists := oldET[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Start.EventTriggers[h] = et
				attachEp(resp.Start.Entrypoints, et.Metadata.Name, newEpByDest)
			} else {
				resp.Download.EventTriggers[h] = et
				attachEp(resp.Download.Entrypoints, et.Metadata.Name, newEpByDest)
			}
		}
	}

	for h, et := range oldET {
		if _, exists := newET[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Stop.EventTriggers[h] = et
				attachEp(resp.Stop.Entrypoints, et.Metadata.Name, oldEpByDest)
			}
		}
	}

	// Event
	oldEv := map[string]*domain.Event{}
	for _, ev := range old.Events {
		oldEv[ev.Metadata.Hash] = ev
	}

	newEv := map[string]*domain.Event{}
	for _, ev := range new.Events {
		newEv[ev.Metadata.Hash] = ev
	}

	for h, ev := range newEv {
		if _, exists := oldEv[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Start.Events[h] = ev
			} else {
				resp.Download.Events[h] = ev
			}
		}
	}

	for h, ev := range oldEv {
		if _, exists := newEv[h]; !exists {
			if _, inLayers := layerSet[h]; inLayers {
				resp.Stop.Events[h] = ev
			}
		}
	}

	return resp, nil
}
