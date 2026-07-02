package domain

type Metadata struct {
	Id          string
	Name        string
	Image       string
	Build       Build
	Hash        string
	Prefix      string
	Topic       string
	Description string
	Labels      map[string]string
	Tags        map[string]string
	TriggerHash string
}

type Build struct {
	Pull    string
	Workdir string
	Command string
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
	Tags         map[string]string
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
		Tags        map[string]string
	}
	Chart Chart
}

type Chart struct {
	DataSources      map[string]*DataSource
	StoredProcedures map[string]*StoredProcedure
	EventTriggers    map[string]*EventTrigger
	Events           map[string]*Event
	Entrypoints      map[string]*Entrypoint
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

type CommandLinkMetadata struct {
	Params string
	Path   string
	Type   string
}

type EntrypointLinkMetadata struct {
	Path string
	Type string
}

type RunLinkMetadata struct {
	Result string
}

type CommandLink struct {
	Metadata    CommandLinkMetadata
	Destination string
}

type EntrypointLink struct {
	Metadata    EntrypointLinkMetadata
	Destination string
}

type RunLink struct {
	Metadata    RunLinkMetadata
	Destination string
}

type Entrypoint struct {
	Metadata   Metadata
	Control    Control
	Features   Features
	Command    *CommandLink
	EntryPoint *EntrypointLink
	Run        *RunLink
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
	Entrypoint       map[string]*Entrypoint
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
		Tags        map[string]string
	}
	DataSources      map[string]*DataSource
	StoredProcedures map[string]*StoredProcedure
	EventTriggers    map[string]*EventTrigger
	Events           map[string]*Event
	Entrypoints      map[string]*Entrypoint
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

type SwitchCheckpointResp struct {
	Start struct {
		DataSources      map[string]*DataSource
		StoredProcedures map[string]*StoredProcedure
		EventTriggers    map[string]*EventTrigger
		Events           map[string]*Event
		Entrypoints      map[string]*Entrypoint
	}
	Stop struct {
		DataSources      map[string]*DataSource
		StoredProcedures map[string]*StoredProcedure
		EventTriggers    map[string]*EventTrigger
		Events           map[string]*Event
		Entrypoints      map[string]*Entrypoint
	}
	Download struct {
		DataSources      map[string]*DataSource
		StoredProcedures map[string]*StoredProcedure
		EventTriggers    map[string]*EventTrigger
		Events           map[string]*Event
		Entrypoints      map[string]*Entrypoint
	}
}

type SearchResp struct {
	DataSources      map[string]*DataSource
	StoredProcedures map[string]*StoredProcedure
	EventTriggers    map[string]*EventTrigger
	Events           map[string]*Event
	Entrypoints      map[string]*Entrypoint
}
