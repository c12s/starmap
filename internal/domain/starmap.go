package domain

type Metadata struct {
	Id          string
	Name        string
	Image       string
	Hash        string
	Prefix      string
	Topic       string
	Labels      map[string]string
	TriggerHash string
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
	Labels       map[string]string
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
		Id          string
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

type GetMissingLayers struct {
	Metadata struct {
		Id            string
		Name          string
		Namespace     string
		ApiVersion    string
		SchemaVersion string
	}
	DataSources      map[string]*DataSource
	StoredProcedures map[string]*StoredProcedure
	EventTriggers    map[string]*EventTrigger
	Events           map[string]*Event
}

type GetChartMetadataResp struct {
	ApiVersion    string
	SchemaVersion string
	Metadata      struct {
		Id          string
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

type MetadataResp struct {
	ApiVersion    string
	SchemaVersion string
	Kind          string
	Metadata      struct {
		Id         string
		Name       string
		Namespace  string
		Maintainer string
	}
}

type TriggerHashStruct struct {
	Trigger EventTrigger
	Events  []Event
}

type TriggerEventExtendHash struct {
	Trigger EventTrigger
	Events  []string
}

type SwitchCheckpointResp struct {
	Start struct {
		DataSources      map[string]*DataSource
		StoredProcedures map[string]*StoredProcedure
		EventTriggers    map[string]*EventTrigger
		Events           map[string]*Event
	}
	Stop struct {
		DataSources      map[string]*DataSource
		StoredProcedures map[string]*StoredProcedure
		EventTriggers    map[string]*EventTrigger
		Events           map[string]*Event
	}
	Download struct {
		DataSources      map[string]*DataSource
		StoredProcedures map[string]*StoredProcedure
		EventTriggers    map[string]*EventTrigger
		Events           map[string]*Event
	}
}
