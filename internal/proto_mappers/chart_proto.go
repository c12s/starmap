package protomappers

import (
	"errors"

	proto "github.com/c12s/starmap/api/proto/starchart"
	"github.com/c12s/starmap/internal/domain"
)

func ProtoToStarChart(chart *proto.StarChart) (*domain.StarChart, error) {
	if chart == nil {
		return nil, errors.New("chart is nil")
	}

	if chart.ApiVersion == "" {
		return nil, errors.New("invalid or missing apiVersion")
	}
	if chart.SchemaVersion == "" {
		return nil, errors.New("invalid or missing schemaVersion")
	}
	if chart.Kind == "" {
		return nil, errors.New("invalid or missing kind")
	}

	meta := chart.Metadata
	if meta == nil {
		return nil, errors.New("missing metadata block")
	}
	if meta.Name == "" || meta.Maintainer == "" || meta.Description == "" ||
		meta.Visibility == "" || meta.Engine == "" || meta.Namespace == "" {
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

	domainChart.Metadata.Name = meta.Name
	domainChart.Metadata.Namespace = meta.Namespace
	domainChart.Metadata.Maintainer = meta.Maintainer
	domainChart.Metadata.Description = meta.Description
	domainChart.Metadata.Visibility = meta.Visibility
	domainChart.Metadata.Engine = meta.Engine
	domainChart.Metadata.Labels = meta.Labels

	for key, ds := range chart.Chart.DataSources {
		domainChart.Chart.DataSources[key] = &domain.DataSource{
			Id:           ds.Id,
			Name:         ds.Name,
			Type:         ds.Type,
			Path:         ds.Path,
			ResourceName: ds.ResourceName,
			Description:  ds.Description,
		}
	}

	for key, sp := range chart.Chart.StoredProcedures {
		domainChart.Chart.StoredProcedures[key] = &domain.StoredProcedure{
			Metadata: domain.Metadata{
				Id:     sp.Metadata.Id,
				Name:   sp.Metadata.Name,
				Image:  sp.Metadata.Image,
				Prefix: sp.Metadata.Prefix,
				Topic:  sp.Metadata.Topic,
			},
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
		domainChart.Chart.EventTriggers[key] = &domain.EventTrigger{
			Metadata: domain.Metadata{
				Id:     et.Metadata.Id,
				Name:   et.Metadata.Name,
				Image:  et.Metadata.Image,
				Prefix: et.Metadata.Prefix,
				Topic:  et.Metadata.Topic,
			},
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
		domainChart.Chart.Events[key] = &domain.Event{
			Metadata: domain.Metadata{
				Id:     ev.Metadata.Id,
				Name:   ev.Metadata.Name,
				Image:  ev.Metadata.Image,
				Prefix: ev.Metadata.Prefix,
				Topic:  ev.Metadata.Topic,
			},
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

func ChartMetadataToProto(chart domain.GetChartMetadataResp) *proto.GetChartFromMetadataResp {
	protoChart := &proto.GetChartFromMetadataResp{
		Metadata: &proto.MetadataChart{
			Name:        chart.Metadata.Name,
			Namespace:   chart.Metadata.Namespace,
			Maintainer:  chart.Metadata.Maintainer,
			Description: chart.Metadata.Description,
			Visibility:  chart.Metadata.Visibility,
			Engine:      chart.Metadata.Engine,
			Labels:      chart.Metadata.Labels,
		},
		Chart: &proto.Chart{
			DataSources:      make(map[string]*proto.DataSource),
			StoredProcedures: make(map[string]*proto.StoredProcedure),
			EventTriggers:    make(map[string]*proto.EventTrigger),
			Events:           make(map[string]*proto.Event),
		},
	}

	// DataSources
	for key, ds := range chart.DataSources {
		protoChart.Chart.DataSources[key] = &proto.DataSource{
			Id:           ds.Id,
			Name:         ds.Name,
			Type:         ds.Type,
			Path:         ds.Path,
			ResourceName: ds.ResourceName,
			Description:  ds.Description,
		}
	}

	// StoredProcedures
	for key, sp := range chart.StoredProcedures {
		protoChart.Chart.StoredProcedures[key] = &proto.StoredProcedure{
			Metadata: &proto.Metadata{
				Id:     sp.Metadata.Id,
				Name:   sp.Metadata.Name,
				Image:  sp.Metadata.Image,
				Prefix: sp.Metadata.Prefix,
				Topic:  sp.Metadata.Topic,
			},
			Control: &proto.Control{
				DisableVirtualization: sp.Control.DisableVirtualization,
				RunDetached:           sp.Control.RunDetached,
				RemoveOnStop:          sp.Control.RemoveOnStop,
				Memory:                sp.Control.Memory,
				KernelArgs:            sp.Control.KernelArgs,
			},
			Features: &proto.Features{
				Networks: sp.Features.Networks,
				Ports:    sp.Features.Ports,
				Volumes:  sp.Features.Volumes,
				Targets:  sp.Features.Targets,
				EnvVars:  sp.Features.EnvVars,
			},
			Links: &proto.Links{
				SoftLinks:  sp.Links.SoftLinks,
				HardLinks:  sp.Links.HardLinks,
				EventLinks: sp.Links.EventLinks,
			},
		}
	}

	// EventTriggers
	for key, et := range chart.EventTriggers {
		protoChart.Chart.EventTriggers[key] = &proto.EventTrigger{
			Metadata: &proto.Metadata{
				Id:     et.Metadata.Id,
				Name:   et.Metadata.Name,
				Image:  et.Metadata.Image,
				Prefix: et.Metadata.Prefix,
				Topic:  et.Metadata.Topic,
			},
			Control: &proto.Control{
				DisableVirtualization: et.Control.DisableVirtualization,
				RunDetached:           et.Control.RunDetached,
				RemoveOnStop:          et.Control.RemoveOnStop,
				Memory:                et.Control.Memory,
				KernelArgs:            et.Control.KernelArgs,
			},
			Features: &proto.Features{
				Networks: et.Features.Networks,
				Ports:    et.Features.Ports,
				Volumes:  et.Features.Volumes,
				Targets:  et.Features.Targets,
				EnvVars:  et.Features.EnvVars,
			},
			Links: &proto.Links{
				SoftLinks:  et.Links.SoftLinks,
				HardLinks:  et.Links.HardLinks,
				EventLinks: et.Links.EventLinks,
			},
		}
	}

	// Events
	for key, ev := range chart.Events {
		protoChart.Chart.Events[key] = &proto.Event{
			Metadata: &proto.Metadata{
				Id:     ev.Metadata.Id,
				Name:   ev.Metadata.Name,
				Image:  ev.Metadata.Image,
				Prefix: ev.Metadata.Prefix,
				Topic:  ev.Metadata.Topic,
			},
			Control: &proto.Control{
				DisableVirtualization: ev.Control.DisableVirtualization,
				RunDetached:           ev.Control.RunDetached,
				RemoveOnStop:          ev.Control.RemoveOnStop,
				Memory:                ev.Control.Memory,
				KernelArgs:            ev.Control.KernelArgs,
			},
			Features: &proto.Features{
				Networks: ev.Features.Networks,
				Ports:    ev.Features.Ports,
				Volumes:  ev.Features.Volumes,
				Targets:  ev.Features.Targets,
				EnvVars:  ev.Features.EnvVars,
			},
		}
	}

	return protoChart
}
