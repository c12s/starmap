package protomappers

import (
	proto "github.com/c12s/starmap/api"
	"github.com/c12s/starmap/internal/domain"
)

func dataSourceToProto(ds *domain.DataSource) *proto.DataSource {
	if ds == nil {
		return nil
	}
	return &proto.DataSource{
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

func metadataToProto(metadata *domain.Metadata) *proto.Metadata {
	if metadata == nil {
		return &proto.Metadata{}
	}

	m := &proto.Metadata{
		Id:          metadata.Id,
		Name:        metadata.Name,
		Prefix:      metadata.Prefix,
		Topic:       metadata.Topic,
		Description: metadata.Description,
		Labels:      metadata.Labels,
		Tags:        metadata.Tags,
	}

	if metadata.Image != "" {
		m.Image = metadata.Image
	} else {
		m.Build = &proto.Build{
			Pull:    metadata.Build.Pull,
			Workdir: metadata.Build.Workdir,
			Command: metadata.Build.Command,
		}
	}

	return m
}

func controlToProto(control *domain.Control) *proto.Control {
	if control == nil {
		return &proto.Control{}
	}
	return &proto.Control{
		DisableVirtualization: control.DisableVirtualization,
		RunDetached:           control.RunDetached,
		RemoveOnStop:          control.RemoveOnStop,
		Memory:                control.Memory,
		KernelArgs:            control.KernelArgs,
	}
}

func featuresToProto(features *domain.Features) *proto.Features {
	if features == nil {
		return &proto.Features{}
	}
	return &proto.Features{
		Networks: features.Networks,
		Ports:    features.Ports,
		Volumes:  features.Volumes,
		Targets:  features.Targets,
		EnvVars:  features.EnvVars,
	}
}

func linksToProto(links *domain.Links) *proto.Links {
	if links == nil {
		return &proto.Links{}
	}
	return &proto.Links{
		SoftLinks:  links.SoftLinks,
		HardLinks:  links.HardLinks,
		EventLinks: links.EventLinks,
	}
}

func storedProcedureToProto(sp *domain.StoredProcedure) *proto.StoredProcedure {
	if sp == nil {
		return nil
	}
	return &proto.StoredProcedure{
		Metadata: metadataToProto(&sp.Metadata),
		Control:  controlToProto(&sp.Control),
		Features: featuresToProto(&sp.Features),
		Links:    linksToProto(&sp.Links),
	}
}

func eventTriggerToProto(et *domain.EventTrigger) *proto.EventTrigger {
	if et == nil {
		return nil
	}
	return &proto.EventTrigger{
		Metadata: metadataToProto(&et.Metadata),
		Control:  controlToProto(&et.Control),
		Features: featuresToProto(&et.Features),
		Links:    linksToProto(&et.Links),
	}
}

func eventToProto(ev *domain.Event) *proto.Event {
	if ev == nil {
		return nil
	}
	return &proto.Event{
		Metadata: metadataToProto(&ev.Metadata),
		Control:  controlToProto(&ev.Control),
		Features: featuresToProto(&ev.Features),
	}
}

func entrypointToProto(ep *domain.Entrypoint) *proto.Entrypoint {
	if ep == nil {
		return nil
	}

	protoEp := &proto.Entrypoint{
		Metadata: metadataToProto(&ep.Metadata),
		Control:  controlToProto(&ep.Control),
		Features: featuresToProto(&ep.Features),
	}

	links := &proto.EntrypointLinks{}
	switch {
	case ep.Command != nil:
		links.Link = &proto.EntrypointLinks_Command{
			Command: &proto.CommandLink{
				Metadata: &proto.CommandLink_CommandLinkMetadata{
					Params: ep.Command.Metadata.Params,
					Path:   ep.Command.Metadata.Path,
					Type:   ep.Command.Metadata.Type,
				},
				Destination: ep.Command.Destination,
			},
		}
	case ep.EntryPoint != nil:
		links.Link = &proto.EntrypointLinks_Entrypoint{
			Entrypoint: &proto.EntrypointLink{
				Metadata: &proto.EntrypointLink_EntrypointLinkMetadata{
					Path: ep.EntryPoint.Metadata.Path,
					Type: ep.EntryPoint.Metadata.Type,
				},
				Destination: ep.EntryPoint.Destination,
			},
		}
	case ep.Run != nil:
		links.Link = &proto.EntrypointLinks_Run{
			Run: &proto.RunLink{
				Metadata: &proto.RunLink_RunLinkMetadata{
					Result: ep.Run.Metadata.Result,
				},
				Destination: ep.Run.Destination,
			},
		}
	}
	protoEp.Links = links

	return protoEp
}

func mapDataSourcesToProto(dataSources map[string]*domain.DataSource) map[string]*proto.DataSource {
	result := make(map[string]*proto.DataSource)
	for key, ds := range dataSources {
		if protoDS := dataSourceToProto(ds); protoDS != nil {
			result[key] = protoDS
		}
	}
	return result
}

func mapStoredProceduresToProto(procedures map[string]*domain.StoredProcedure) map[string]*proto.StoredProcedure {
	result := make(map[string]*proto.StoredProcedure)
	for key, sp := range procedures {
		if protoSP := storedProcedureToProto(sp); protoSP != nil {
			result[key] = protoSP
		}
	}
	return result
}

func mapEventTriggersToProto(triggers map[string]*domain.EventTrigger) map[string]*proto.EventTrigger {
	result := make(map[string]*proto.EventTrigger)
	for key, et := range triggers {
		if protoET := eventTriggerToProto(et); protoET != nil {
			result[key] = protoET
		}
	}
	return result
}

func mapEventsToProto(events map[string]*domain.Event) map[string]*proto.Event {
	result := make(map[string]*proto.Event)
	for key, ev := range events {
		if protoEv := eventToProto(ev); protoEv != nil {
			result[key] = protoEv
		}
	}
	return result
}

func mapEntrypointsToProto(entrypoints map[string]*domain.Entrypoint) map[string]*proto.Entrypoint {
	result := make(map[string]*proto.Entrypoint)
	for key, ep := range entrypoints {
		if protoEp := entrypointToProto(ep); protoEp != nil {
			result[key] = protoEp
		}
	}
	return result
}
