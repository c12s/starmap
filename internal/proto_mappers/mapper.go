package protomappers

import (
	"errors"

	proto "github.com/c12s/starmap/api"
	"github.com/c12s/starmap/internal/domain"
)

func ProtoToStarChart(chart *proto.StarChart) (*domain.StarChart, error) {
	if chart == nil {
		return nil, errors.New("chart is nil")
	}

	if chart.Kind == "" {
		return nil, errors.New("invalid or missing kind")
	}

	meta := chart.Metadata
	if meta == nil {
		return nil, errors.New("missing metadata block")
	}
	if meta.Name == "" || meta.Maintainer == "" || meta.Description == "" ||
		meta.Visibility == "" || meta.Engine == "" {
		return nil, errors.New("metadata fields are incomplete")
	}

	domainChart := &domain.StarChart{
		ApiVersion:    chart.ApiVersion,
		SchemaVersion: chart.SchemaVersion,
		Kind:          chart.Kind,
		Chart: domain.Chart{
			DataSources:      make(map[string]*domain.DataSource),
			StoredProcedures: make(map[string]*domain.StoredProcedure),
			EventTriggers:    make(map[string]*domain.EventTrigger),
			Events:           make(map[string]*domain.Event),
		},
	}

	domainChart.Metadata.Id = meta.Id
	domainChart.Metadata.Name = meta.Name
	domainChart.Metadata.Namespace = meta.Namespace
	domainChart.Metadata.Maintainer = meta.Maintainer
	domainChart.Metadata.Description = meta.Description
	domainChart.Metadata.Visibility = meta.Visibility
	domainChart.Metadata.Engine = meta.Engine
	domainChart.Metadata.Labels = meta.Labels
	domainChart.Metadata.Tags = meta.Tags

	for key, ds := range chart.Chart.DataSources {
		domainChart.Chart.DataSources[key] = &domain.DataSource{
			Id:           ds.Id,
			Name:         ds.Name,
			Type:         ds.Type,
			Path:         ds.Path,
			ResourceName: ds.ResourceName,
			Description:  ds.Description,
			Labels:       ds.Labels,
			Tags:         ds.Tags,
		}
	}

	for key, sp := range chart.Chart.StoredProcedures {
		metadata := domain.Metadata{
			Id:          sp.Metadata.Id,
			Name:        sp.Metadata.Name,
			Prefix:      sp.Metadata.Prefix,
			Topic:       sp.Metadata.Topic,
			Description: sp.Metadata.Description,
			Labels:      sp.Metadata.Labels,
			Tags:        sp.Metadata.Tags,
		}

		if sp.Metadata.Image != "" {
			metadata.Image = sp.Metadata.Image
		} else {
			metadata.Build = domain.Build{
				Pull:    sp.Metadata.Build.Pull,
				Workdir: sp.Metadata.Build.Workdir,
				Command: sp.Metadata.Build.Command,
			}
		}

		domainChart.Chart.StoredProcedures[key] = &domain.StoredProcedure{
			Metadata: metadata,
			Control: domain.Control{
				DisableVirtualization: sp.Control.DisableVirtualization,
				RunDetached:           sp.Control.RunDetached,
				RemoveOnStop:          sp.Control.RemoveOnStop,
				Memory:                sp.Control.Memory,
				KernelArgs:            sp.Control.KernelArgs,
			},
			Features: domain.Features{
				Networks: sp.Features.Networks,
				Ports:    sp.Features.Ports,
				Volumes:  sp.Features.Volumes,
				Targets:  sp.Features.Targets,
				EnvVars:  sp.Features.EnvVars,
			},
			Links: domain.Links{
				SoftLinks:  sp.Links.SoftLinks,
				HardLinks:  sp.Links.HardLinks,
				EventLinks: sp.Links.EventLinks,
			},
		}
	}

	for key, et := range chart.Chart.EventTriggers {
		metadata := domain.Metadata{
			Id:          et.Metadata.Id,
			Name:        et.Metadata.Name,
			Prefix:      et.Metadata.Prefix,
			Topic:       et.Metadata.Topic,
			Description: et.Metadata.Description,
			Labels:      et.Metadata.Labels,
			Tags:        et.Metadata.Tags,
		}

		if et.Metadata.Image != "" {
			metadata.Image = et.Metadata.Image
		} else {
			metadata.Build = domain.Build{
				Pull:    et.Metadata.Build.Pull,
				Workdir: et.Metadata.Build.Workdir,
				Command: et.Metadata.Build.Command,
			}
		}

		domainChart.Chart.EventTriggers[key] = &domain.EventTrigger{
			Metadata: metadata,
			Control: domain.Control{
				DisableVirtualization: et.Control.DisableVirtualization,
				RunDetached:           et.Control.RunDetached,
				RemoveOnStop:          et.Control.RemoveOnStop,
				Memory:                et.Control.Memory,
				KernelArgs:            et.Control.KernelArgs,
			},
			Features: domain.Features{
				Networks: et.Features.Networks,
				Ports:    et.Features.Ports,
				Volumes:  et.Features.Volumes,
				Targets:  et.Features.Targets,
				EnvVars:  et.Features.EnvVars,
			},
			Links: domain.Links{
				SoftLinks:  et.Links.SoftLinks,
				HardLinks:  et.Links.HardLinks,
				EventLinks: et.Links.EventLinks,
			},
		}
	}

	for key, ev := range chart.Chart.Events {
		metadata := domain.Metadata{
			Id:          ev.Metadata.Id,
			Name:        ev.Metadata.Name,
			Prefix:      ev.Metadata.Prefix,
			Topic:       ev.Metadata.Topic,
			Description: ev.Metadata.Description,
			Labels:      ev.Metadata.Labels,
			Tags:        ev.Metadata.Tags,
		}

		if ev.Metadata.Image != "" {
			metadata.Image = ev.Metadata.Image
		} else {
			metadata.Build = domain.Build{
				Pull:    ev.Metadata.Build.Pull,
				Workdir: ev.Metadata.Build.Workdir,
				Command: ev.Metadata.Build.Command,
			}
		}

		domainChart.Chart.Events[key] = &domain.Event{
			Metadata: metadata,
			Control: domain.Control{
				DisableVirtualization: ev.Control.DisableVirtualization,
				RunDetached:           ev.Control.RunDetached,
				RemoveOnStop:          ev.Control.RemoveOnStop,
				Memory:                ev.Control.Memory,
				KernelArgs:            ev.Control.KernelArgs,
			},
			Features: domain.Features{
				Networks: ev.Features.Networks,
				Ports:    ev.Features.Ports,
				Volumes:  ev.Features.Volumes,
				Targets:  ev.Features.Targets,
				EnvVars:  ev.Features.EnvVars,
			},
		}
	}

	return domainChart, nil
}

func ChartMetadataToProto(chart domain.GetChartMetadataResp) *proto.GetChartResp {
	return &proto.GetChartResp{
		ApiVersion:    chart.ApiVersion,
		SchemaVersion: chart.SchemaVersion,
		Metadata: &proto.MetadataChart{
			Id:          chart.Metadata.Id,
			Name:        chart.Metadata.Name,
			Namespace:   chart.Metadata.Namespace,
			Maintainer:  chart.Metadata.Maintainer,
			Description: chart.Metadata.Description,
			Visibility:  chart.Metadata.Visibility,
			Engine:      chart.Metadata.Engine,
			Labels:      chart.Metadata.Labels,
			Tags:        chart.Metadata.Tags,
		},
		Chart: &proto.Chart{
			DataSources:      mapDataSourcesToProto(chart.DataSources),
			StoredProcedures: mapStoredProceduresToProto(chart.StoredProcedures),
			EventTriggers:    mapEventTriggersToProto(chart.EventTriggers),
			Events:           mapEventsToProto(chart.Events),
		},
	}
}

func GetMissingLayersToProto(layers domain.GetMissingLayers) *proto.GetMissingLayersResp {
	return &proto.GetMissingLayersResp{
		ChartId:          layers.Metadata.Id,
		Namespace:        layers.Metadata.Namespace,
		Maintainer:       layers.Metadata.Name,
		ApiVersion:       layers.Metadata.ApiVersion,
		SchemaVersion:    layers.Metadata.SchemaVersion,
		DataSources:      mapDataSourcesToProto(layers.DataSources),
		StoredProcedures: mapStoredProceduresToProto(layers.StoredProcedures),
		EventTriggers:    mapEventTriggersToProto(layers.EventTriggers),
		Events:           mapEventsToProto(layers.Events),
	}
}

func SwitchCheckpointMapperToProto(sc domain.SwitchCheckpointResp) *proto.SwitchCheckpointResp {
	return &proto.SwitchCheckpointResp{
		Start: &proto.LayersResp{
			DataSources:      mapDataSourcesToProto(sc.Start.DataSources),
			StoredProcedures: mapStoredProceduresToProto(sc.Start.StoredProcedures),
			EventTriggers:    mapEventTriggersToProto(sc.Start.EventTriggers),
			Events:           mapEventsToProto(sc.Start.Events),
		},
		Stop: &proto.LayersResp{
			DataSources:      mapDataSourcesToProto(sc.Stop.DataSources),
			StoredProcedures: mapStoredProceduresToProto(sc.Stop.StoredProcedures),
			EventTriggers:    mapEventTriggersToProto(sc.Stop.EventTriggers),
			Events:           mapEventsToProto(sc.Stop.Events),
		},
		Download: &proto.LayersResp{
			DataSources:      mapDataSourcesToProto(sc.Download.DataSources),
			StoredProcedures: mapStoredProceduresToProto(sc.Download.StoredProcedures),
			EventTriggers:    mapEventTriggersToProto(sc.Download.EventTriggers),
			Events:           mapEventsToProto(sc.Download.Events),
		},
	}
}

func SearchToProto(sc domain.SearchResp) *proto.LayersResp {
	return &proto.LayersResp{
		DataSources:      mapDataSourcesToProto(sc.DataSources),
		StoredProcedures: mapStoredProceduresToProto(sc.StoredProcedures),
		EventTriggers:    mapEventTriggersToProto(sc.EventTriggers),
		Events:           mapEventsToProto(sc.Events),
	}
}
