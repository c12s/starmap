package domain

type Metadata struct {
	Id          string
	Name        string
	Image       string // Layer node (stored without tag)
	Build       Build  // Layer node
	Hash        string // Layer node identity (MERGE key)
	Prefix      string
	Topic       string
	Description string
	Labels      map[string]string
	Tags        map[string]string
	TriggerHash string
	Pin         string // HAS_PROCEDURE/HAS_TRIGGER edge: which version this chart wants
	Sha         string // resolved from the pinned version + arch (Build node)
	Semver      string // resolved layer version
	Arch        string // resolved build arch
}

type Build struct {
	Pull    string
	Workdir string
	Command string
}

type LayerBuild struct {
	Arch string
	Sha  string
}

type LayerSelector struct {
	Semver string
	Arch   string
}

type LayerVersion struct {
	Semver    string
	Sha       string // manifest-list digest (arch-agnostic default pull)
	CreatedAt int64
	IsLatest  bool
	Builds    []LayerBuild
}

type PushLayerInput struct {
	SourceType string
	NodeType   string
	Image      string
	Pull       string
	Command    string
	Semver     string
	Sha        string // manifest-list digest
	Builds     []LayerBuild
}

type PushLayerResult struct {
	Sha            string
	Semver         string
	PreviousSemver string
}

type ListLayerVersionsResult struct {
	SourceType string
	Versions   []LayerVersion
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
