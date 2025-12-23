package services

import (
	"context"

	proto "github.com/c12s/starmap/api"
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

func (s *RegistryService) GetChartMetadata(ctx context.Context, req *proto.GetChartFromMetadataReq) (*proto.GetChartResp, error) {
	chart, err := s.repo.GetChartMetadata(ctx, req.SchemaVersion, req.Name, req.Namespace, req.Maintainer)
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
	chart, err := s.repo.GetChartId(ctx, req.SchemaVersion, req.Namespace, req.Maintainer, req.ChartId)
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
