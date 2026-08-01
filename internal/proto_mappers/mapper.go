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
			Entrypoints:      make(map[string]*domain.Entrypoint),
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
		if sp.Metadata == nil {
			return nil, errors.New("stored procedure missing metadata")
		}
		if sp.Control == nil {
			sp.Control = &proto.Control{}
		}
		if sp.Features == nil {
			sp.Features = &proto.Features{}
		}
		if sp.Links == nil {
			sp.Links = &proto.Links{}
		}
		metadata := domain.Metadata{
			Id:          sp.Metadata.Id,
			Name:        sp.Metadata.Name,
			Prefix:      sp.Metadata.Prefix,
			Topic:       sp.Metadata.Topic,
			Description: sp.Metadata.Description,
			Labels:      sp.Metadata.Labels,
			Tags:        sp.Metadata.Tags,
			Sha:         sp.Metadata.Sha,
			Semver:      sp.Metadata.Semver,
			Arch:        sp.Metadata.Arch,
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
		if et.Metadata == nil {
			return nil, errors.New("event trigger missing metadata")
		}
		if et.Control == nil {
			et.Control = &proto.Control{}
		}
		if et.Features == nil {
			et.Features = &proto.Features{}
		}
		if et.Links == nil {
			et.Links = &proto.Links{}
		}
		metadata := domain.Metadata{
			Id:          et.Metadata.Id,
			Name:        et.Metadata.Name,
			Prefix:      et.Metadata.Prefix,
			Topic:       et.Metadata.Topic,
			Description: et.Metadata.Description,
			Labels:      et.Metadata.Labels,
			Tags:        et.Metadata.Tags,
			Sha:         et.Metadata.Sha,
			Semver:      et.Metadata.Semver,
			Arch:        et.Metadata.Arch,
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
		if ev.Metadata == nil {
			return nil, errors.New("event missing metadata")
		}
		if ev.Control == nil {
			ev.Control = &proto.Control{}
		}
		if ev.Features == nil {
			ev.Features = &proto.Features{}
		}
		metadata := domain.Metadata{
			Id:          ev.Metadata.Id,
			Name:        ev.Metadata.Name,
			Prefix:      ev.Metadata.Prefix,
			Topic:       ev.Metadata.Topic,
			Description: ev.Metadata.Description,
			Labels:      ev.Metadata.Labels,
			Tags:        ev.Metadata.Tags,
			Sha:         ev.Metadata.Sha,
			Semver:      ev.Metadata.Semver,
			Arch:        ev.Metadata.Arch,
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

	for key, ep := range chart.Chart.Entrypoints {
		if ep == nil {
			continue
		}
		if ep.Metadata == nil {
			return nil, errors.New("entrypoint missing metadata")
		}
		if ep.Control == nil {
			ep.Control = &proto.Control{}
		}
		if ep.Features == nil {
			ep.Features = &proto.Features{}
		}
		metadata := domain.Metadata{
			Id:          ep.Metadata.Id,
			Name:        ep.Metadata.Name,
			Prefix:      ep.Metadata.Prefix,
			Topic:       ep.Metadata.Topic,
			Description: ep.Metadata.Description,
			Labels:      ep.Metadata.Labels,
			Tags:        ep.Metadata.Tags,
			Sha:         ep.Metadata.Sha,
			Semver:      ep.Metadata.Semver,
			Arch:        ep.Metadata.Arch,
		}
		if ep.Metadata.Image != "" {
			metadata.Image = ep.Metadata.Image
		} else {
			metadata.Build = domain.Build{
				Pull:    ep.Metadata.Build.Pull,
				Workdir: ep.Metadata.Build.Workdir,
				Command: ep.Metadata.Build.Command,
			}
		}

		domainEp := &domain.Entrypoint{
			Metadata: metadata,
			Control: domain.Control{
				DisableVirtualization: ep.Control.DisableVirtualization,
				RunDetached:           ep.Control.RunDetached,
				RemoveOnStop:          ep.Control.RemoveOnStop,
				Memory:                ep.Control.Memory,
				KernelArgs:            ep.Control.KernelArgs,
			},
			Features: domain.Features{
				Networks: ep.Features.Networks,
				Ports:    ep.Features.Ports,
				Volumes:  ep.Features.Volumes,
				Targets:  ep.Features.Targets,
				EnvVars:  ep.Features.EnvVars,
			},
		}

		if links := ep.GetLinks(); links != nil {
			switch l := links.Link.(type) {
			case *proto.EntrypointLinks_Command:
				if l.Command != nil {
					domainEp.Command = &domain.CommandLink{Destination: l.Command.Destination}
					if l.Command.Metadata != nil {
						domainEp.Command.Metadata = domain.CommandLinkMetadata{
							Params: l.Command.Metadata.Params,
							Path:   l.Command.Metadata.Path,
							Type:   l.Command.Metadata.Type,
						}
					}
				}
			case *proto.EntrypointLinks_Entrypoint:
				if l.Entrypoint != nil {
					domainEp.EntryPoint = &domain.EntrypointLink{Destination: l.Entrypoint.Destination}
					if l.Entrypoint.Metadata != nil {
						domainEp.EntryPoint.Metadata = domain.EntrypointLinkMetadata{
							Path: l.Entrypoint.Metadata.Path,
							Type: l.Entrypoint.Metadata.Type,
						}
					}
				}
			case *proto.EntrypointLinks_Run:
				if l.Run != nil {
					domainEp.Run = &domain.RunLink{Destination: l.Run.Destination}
					if l.Run.Metadata != nil {
						domainEp.Run.Metadata = domain.RunLinkMetadata{
							Result: l.Run.Metadata.Result,
						}
					}
				}
			}
		}

		domainChart.Chart.Entrypoints[key] = domainEp
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
			Entrypoints:      mapEntrypointsToProto(chart.Entrypoints),
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
		Entrypoints:      mapEntrypointsToProto(layers.Entrypoint),
	}
}

func SwitchCheckpointMapperToProto(sc domain.SwitchCheckpointResp) *proto.SwitchCheckpointResp {
	return &proto.SwitchCheckpointResp{
		Start: &proto.LayersResp{
			DataSources:      mapDataSourcesToProto(sc.Start.DataSources),
			StoredProcedures: mapStoredProceduresToProto(sc.Start.StoredProcedures),
			EventTriggers:    mapEventTriggersToProto(sc.Start.EventTriggers),
			Events:           mapEventsToProto(sc.Start.Events),
			Entrypoints:      mapEntrypointsToProto(sc.Start.Entrypoints),
		},
		Stop: &proto.LayersResp{
			DataSources:      mapDataSourcesToProto(sc.Stop.DataSources),
			StoredProcedures: mapStoredProceduresToProto(sc.Stop.StoredProcedures),
			EventTriggers:    mapEventTriggersToProto(sc.Stop.EventTriggers),
			Events:           mapEventsToProto(sc.Stop.Events),
			Entrypoints:      mapEntrypointsToProto(sc.Stop.Entrypoints),
		},
		Download: &proto.LayersResp{
			DataSources:      mapDataSourcesToProto(sc.Download.DataSources),
			StoredProcedures: mapStoredProceduresToProto(sc.Download.StoredProcedures),
			EventTriggers:    mapEventTriggersToProto(sc.Download.EventTriggers),
			Events:           mapEventsToProto(sc.Download.Events),
			Entrypoints:      mapEntrypointsToProto(sc.Download.Entrypoints),
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
