package domain

type Metadata struct {
	Id     string
	Name   string
	Image  string
	Hash   string
	Prefix string
	Topic  string
}

type Control struct {
	DisableVirtualization bool
	RunDetached           bool
	RemoveOnStop          bool
	Memory                string
	KernelArgs            string
}

type Features struct {
	Networks []string
	Ports    []string
	Volumes  []string
	Targets  []string
	EnvVars  []string
}

type Links struct {
	SoftLinks  []string
	HardLinks  []string
	EventLinks []string
}

type DataSource struct {
	Id           string
	Name         string
	Type         string
	Path         string
	Hash         string
	ResourceName string
	Description  string
}

type StoredProcedure struct {
	Metadata Metadata
	Control  Control
	Features Features
	Links    Links
}

type EventTrigger struct {
	Metadata Metadata
	Control  Control
	Features Features
	Links    Links
}

type Event struct {
	Metadata Metadata
	Control  Control
	Features Features
}

type Chart struct {
	DataSources      map[string]*DataSource
	StoredProcedures map[string]*StoredProcedure
	EventTriggers    map[string]*EventTrigger
	Events           map[string]*Event
}

type StarChart struct {
	ApiVersion    string
	SchemaVersion string
	Kind          string
	Metadata      struct {
		Name        string
		Namespace   string
		Maintainer  string
		Description string
		Visibility  string
		Engine      string
		Labels      map[string]string
	}
	Chart Chart
}

type GetChartMetadataResp struct {
	Metadata struct {
		Name        string
		Namespace   string
		Maintainer  string
		Description string
		Visibility  string
		Engine      string
		Labels      map[string]string
	}
	DataSources      map[string]*DataSource
	StoredProcedures map[string]*StoredProcedure
	EventTriggers    map[string]*EventTrigger
	Events           map[string]*Event
}

type GetChartsLabelsResp struct {
	Charts []GetChartMetadataResp
}
