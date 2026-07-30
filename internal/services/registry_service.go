package services

import (
	"context"

	proto "github.com/c12s/starmap/api"
	"github.com/c12s/starmap/internal/domain"
	protomappers "github.com/c12s/starmap/internal/proto_mappers"
	"github.com/c12s/starmap/internal/repos"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RegistryService struct {
	repo repos.RegistryRepo
	proto.UnimplementedRegistryServiceServer
}

func NewRegistryService(repo *repos.RegistryRepo) *RegistryService {
	return &RegistryService{
		repo: *repo,
	}
}

func (s *RegistryService) PutChart(ctx context.Context, req *proto.StarChart) (*proto.PutChartResp, error) {
	chart, err := protomappers.ProtoToStarChart(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chart: %v", err)
	}

	starChart, err := s.repo.PutChart(ctx, *chart)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store chart: %v", err)
	}

	return &proto.PutChartResp{
		Id:            starChart.Metadata.Id,
		ApiVersion:    starChart.ApiVersion,
		SchemaVersion: starChart.SchemaVersion,
		Kind:          starChart.Kind,
		Name:          starChart.Metadata.Name,
		Namespace:     starChart.Metadata.Namespace,
		Maintainer:    starChart.Metadata.Maintainer,
	}, nil

}

func layersToDomain(in map[string]*proto.LayerSelector) map[string]domain.LayerSelector {
	if in == nil {
		return nil
	}
	out := make(map[string]domain.LayerSelector, len(in))
	for img, sel := range in {
		if sel == nil {
			continue
		}
		out[img] = domain.LayerSelector{Semver: sel.Semver, Arch: sel.Arch}
	}
	return out
}

func (s *RegistryService) GetChartMetadata(ctx context.Context, req *proto.GetChartFromMetadataReq) (*proto.GetChartResp, error) {
	chart, err := s.repo.GetChartMetadata(ctx, req.SchemaVersion, req.Namespace, req.Maintainer, req.Name, layersToDomain(req.Layers))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get chart metadata: %v", err)
	}

	return protomappers.ChartMetadataToProto(*chart), nil
}

func (s *RegistryService) GetChartsLabels(ctx context.Context, req *proto.GetChartsLabelsReq) (*proto.GetChartsLabelsResp, error) {
	charts, err := s.repo.GetChartsLabels(ctx, req.SchemaVersion, req.Namespace, req.Maintainer, req.Labels)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get charts by labels: %v", err)
	}

	resp := &proto.GetChartsLabelsResp{}

	for _, chart := range charts.Charts {
		chartProto := protomappers.ChartMetadataToProto(chart)
		resp.Charts = append(resp.Charts, chartProto)
	}

	return resp, nil
}

func (s *RegistryService) GetChartId(ctx context.Context, req *proto.GetChartIdReq) (*proto.GetChartResp, error) {
	chart, err := s.repo.GetChartId(ctx, req.SchemaVersion, req.Namespace, req.Maintainer, req.ChartId, layersToDomain(req.Layers))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get chart by id: %v", err)
	}

	return protomappers.ChartMetadataToProto(*chart), nil

}

func (s *RegistryService) GetMissingLayers(ctx context.Context, req *proto.GetMissingLayersReq) (*proto.GetMissingLayersResp, error) {
	result, err := s.repo.GetMissingLayers(ctx, req.SchemaVersion, req.Namespace, req.Maintainer, req.ChartId, req.Layers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get missing layers: %v", err)
	}

	protoResp := protomappers.GetMissingLayersToProto(*result)
	protoResp.ChartId = req.ChartId
	protoResp.Maintainer = req.Maintainer
	protoResp.Namespace = req.Namespace

	return protoResp, nil
}

func (s *RegistryService) GetCharts(ctx context.Context, req *proto.EmptyMessage) (*proto.GetChartsLabelsResp, error) {
	result, err := s.repo.GetAllCharts(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get all charts: %v", err)
	}

	resp := &proto.GetChartsLabelsResp{}

	for _, chart := range result.Charts {
		chartProto := protomappers.ChartMetadataToProto(chart)
		resp.Charts = append(resp.Charts, chartProto)
	}

	return resp, nil
}

func (s *RegistryService) DeleteChart(ctx context.Context, req *proto.DeleteChartReq) (*proto.EmptyMessage, error) {
	err := s.repo.DeleteChart(ctx, req.Id, req.Name, req.Namespace, req.Maintainer, req.SchemaVersion, req.Kind)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete chart: %v", err)
	}

	return &proto.EmptyMessage{}, nil
}

func (s *RegistryService) UpdateChart(ctx context.Context, req *proto.StarChart) (*proto.PutChartResp, error) {
	chart, err := protomappers.ProtoToStarChart(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chart: %v", err)
	}

	starChart, err := s.repo.UpdateChart(ctx, *chart)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to store chart: %v", err)
	}

	return &proto.PutChartResp{
		Id:            starChart.Metadata.Id,
		ApiVersion:    starChart.ApiVersion,
		SchemaVersion: starChart.SchemaVersion,
		Kind:          starChart.Kind,
		Name:          starChart.Metadata.Name,
		Namespace:     starChart.Metadata.Namespace,
		Maintainer:    starChart.Metadata.Maintainer,
	}, nil

}

func (s *RegistryService) SwitchCheckpoint(ctx context.Context, req *proto.SwitchCheckpointReq) (*proto.SwitchCheckpointResp, error) {
	if req.NewVersion == "" || req.OldVersion == "" {
		return nil, status.Errorf(codes.Aborted, "missing version")
	}
	resp, err := s.repo.SwitchCheckpoint(ctx, req.Namespace, req.Maintainer, req.ChartId, req.OldVersion, req.NewVersion, req.Layers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to switch checkpoint: %v", err)
	}

	return protomappers.SwitchCheckpointMapperToProto(*resp), nil
}

func (s *RegistryService) Timeline(ctx context.Context, req *proto.TimelineReq) (*proto.TimelineResp, error) {
	result, err := s.repo.Timeline(ctx, req.Namespace, req.Maintainer, req.ChartId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to timeline: %v", err)
	}

	resp := &proto.TimelineResp{}

	for _, chart := range result.Charts {
		chartProto := protomappers.ChartMetadataToProto(chart)
		resp.Charts = append(resp.Charts, chartProto)
	}

	return resp, nil
}

func (s *RegistryService) Extend(ctx context.Context, req *proto.ExtendReq) (*proto.PutChartResp, error) {
	chart, err := protomappers.ProtoToStarChart(req.Chart)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid chart: %v", err)
	}

	result, err := s.repo.Extend(ctx, req.OldVersion, *chart)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to extend: %v", err)
	}

	return &proto.PutChartResp{
		Id:            result.Metadata.Id,
		ApiVersion:    result.ApiVersion,
		SchemaVersion: result.SchemaVersion,
		Kind:          result.Kind,
		Name:          result.Metadata.Name,
		Namespace:     result.Metadata.Namespace,
		Maintainer:    result.Metadata.Maintainer,
	}, nil
}

func (s *RegistryService) PushLayer(ctx context.Context, req *proto.PushLayerReq) (*proto.PushLayerResp, error) {
	builds := make([]domain.LayerBuild, 0, len(req.Builds))
	for _, b := range req.Builds {
		builds = append(builds, domain.LayerBuild{Arch: b.Arch, Sha: b.Sha})
	}

	result, err := s.repo.PushLayer(ctx, domain.PushLayerInput{
		SourceType: req.SourceType,
		NodeType:   req.NodeType,
		Image:      req.Image,
		Pull:       req.Pull,
		Command:    req.Command,
		Semver:     req.Semver,
		Sha:        req.Sha,
		Builds:     builds,
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failed to push layer: %v", err)
	}

	return &proto.PushLayerResp{
		Semver:         result.Semver,
		PreviousSemver: result.PreviousSemver,
	}, nil
}

func (s *RegistryService) ListLayerVersions(ctx context.Context, req *proto.ListLayerVersionsReq) (*proto.ListLayerVersionsResp, error) {
	sourceType := "oci"
	if req.Image == "" && req.Pull != "" {
		sourceType = "git"
	}

	result, err := s.repo.ListLayerVersions(ctx, sourceType, req.Image, req.Pull, req.Command)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list layer versions: %v", err)
	}

	resp := &proto.ListLayerVersionsResp{SourceType: result.SourceType}
	for _, v := range result.Versions {
		info := &proto.LayerVersionInfo{
			Semver:    v.Semver,
			Sha:       v.Sha,
			CreatedAt: v.CreatedAt,
			IsLatest:  v.IsLatest,
		}
		for _, b := range v.Builds {
			info.Builds = append(info.Builds, &proto.LayerBuild{Arch: b.Arch, Sha: b.Sha})
		}
		resp.Versions = append(resp.Versions, info)
	}
	return resp, nil
}

func (s *RegistryService) DeleteLayer(ctx context.Context, req *proto.DeleteLayerReq) (*proto.EmptyMessage, error) {
	err := s.repo.DeleteLayer(ctx, req.Image, req.Pull, req.Command)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to delete layer: %v", err)
	}
	return &proto.EmptyMessage{}, nil
}

func (s *RegistryService) Search(ctx context.Context, req *proto.SearchReq) (*proto.GetChartsLabelsResp, error) {
	charts, err := s.repo.Search(ctx, req.Name, req.Description, req.Tags, req.DeepSearch, req.ComponentTags)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to search db: %v", err)
	}

	resp := &proto.GetChartsLabelsResp{}

	for _, chart := range charts.Charts {
		chartProto := protomappers.ChartMetadataToProto(chart)
		resp.Charts = append(resp.Charts, chartProto)
	}

	return resp, nil
}
