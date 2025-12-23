# Starmap
Registry for Overlay data mesh definitions.

### Application configuration

| Parameter | Description | Default value |
|--|--|--|
REGISTRY_SERVICE_PORT | The port number at which the registry gRPC server is listening. | 8001 |
REGISTRY_SERVICE_ADDRESS | The hostname of the registry service. | registry-server |
NEO4J_URI | The connection URI for the Neo4j database instance. | bolt://neo4j:7687 |
NEO4J_PORT | The port number for Neo4j bolt protocol connection. | 7687 |
NEO4J_UI_PORT | The port number for Neo4j browser UI. | 7474 |
NEO4J_USER | The username for Neo4j database authentication. | neo4j |
NEO4J_PASS | The password for Neo4j database authentication. | password123 |

.env example:

    #Registry Service
    REGISTRY_SERVICE_PORT=8001
    REGISTRY_SERVICE_ADDRESS=registry-server

    #Neo4J
    NEO4J_PORT=7687
    NEO4J_UI_PORT=7474

    NEO4J_URI=bolt://neo4j:7687
    NEO4J_USER=neo4j
    NEO4J_PASS=password123

## Running with Docker Compose

StarMap services can be started using Docker Compose.
This will automatically run:

    - Registry service
    - Neo4j database

### Protobuf Model
```proto
message DataSource {
  string id = 1;
  string name = 2;
  string type = 3;
  string path = 4;
  string resourceName = 5;
  string description = 6;
  map<string, string> labels = 7;
}

message Metadata {
  string id = 1;
  string name = 2;
  string image = 3;
  string prefix = 4;
  string topic = 5;
  map<string, string> labels = 6;
}

message Control {
  bool disableVirtualization = 1;
  bool runDetached = 2;
  bool removeOnStop = 3;
  string memory = 4;
  string kernelArgs = 5;
}

message Features {
  repeated string networks = 1;
  repeated string ports = 2;
  repeated string volumes = 3;
  repeated string targets = 4;
  repeated string envVars = 5;
}

message Links {
  repeated string softLinks = 1;
  repeated string hardLinks = 2;
  repeated string eventLinks = 3;
}

message StoredProcedure {
  Metadata metadata = 1;
  Control control = 2;
  Features features = 3;
  Links links = 4;
}

message Event {
  Metadata metadata = 1;
  Control control = 2;
  Features features = 3;
}

message EventTrigger {
  Metadata metadata = 1;
  Control control = 2;
  Features features = 3;
  Links links = 4;
}

message Chart {
  map<string, DataSource> dataSources = 1;
  map<string, StoredProcedure> storedProcedures = 2;
  map<string, EventTrigger> eventTriggers = 3;
  map<string, Event> events = 4;
}

message MetadataChart {
    string id = 1;
    string name = 2;
    string namespace = 3;
    string maintainer = 4;
    string description = 5;
    string visibility = 6;
    string engine = 7;
    map<string, string> labels = 8;
  }

message StarChart {
  string apiVersion = 1;
  string schemaVersion = 2;
  string kind = 3;

  MetadataChart metadata = 4;
  Chart chart = 5;
}
```

### gRPC Endpoints

#### /PutChart
The endpoint for storing a new Chart.

#### Request body
```proto
message StarChart {
  string apiVersion = 1;
  string schemaVersion = 2;
  string kind = 3;
  MetadataChart metadata = 4;
  Chart chart = 5;
}
```

Full example json request:
starmap/yamlStarChart.json

#### Response - 0 OK
```proto
message PutChartResp {
  string id = 1;
  string apiVersion = 2;
  string schemaVersion = 3;
  string kind = 4;
  string name = 5;
  string namespace = 6;
  string maintainer = 7;
}
```

#### /GetChartMetadata
The endpoint for retrieving complete chart by metadata: 
name, namespace, maintainer and versions.

#### Request body
```proto
message GetChartFromMetadataReq {
  string name = 1;
  string namespace = 2;
  string maintainer = 3;
  string schemaVersion = 4;
}
```

#### Response - 0 OK
```proto
message GetChartResp {
  string apiVersion = 1;
  string schemaVersion = 2;
  MetadataChart metadata = 3;
  Chart chart = 4;
}
```

#### /GetChartsLabels
The endpoint for querying charts by namespace, maintainer, labels and versions.

#### Request body
```proto
message GetChartsLabelsReq {
  string schemaVersion = 1;
  string namespace = 2;
  string maintainer = 3;
  map<string, string> labels = 4;
}
```

#### Response - 0 OK
```proto
message GetChartsLabelsResp {
  repeated GetChartResp charts = 1;
}
```

#### /GetChartId
The endpoint for querying charts by chartId, namespace, maintainer and versions.

#### Request body
```proto
message GetChartIdReq {
  string schemaVersion = 1;
  string chartId = 2;
  string namespace = 3;
  string maintainer = 4;
}
```

#### Response - 0 OK
```proto
message GetChartResp {
  string apiVersion = 1;
  string schemaVersion = 2;
  MetadataChart metadata = 3;
  Chart chart = 4;
}
```

#### /GetMissingLayers
Endpoint returns chart layers present in the registry but missing from the provided layer hashes.

#### Request body
```proto
message GetMissingLayersReq {
  string schemaVersion = 1;
  string chartId = 2;
  string namespace = 3;
  string maintainer = 4;
  repeated string layers = 5;
}
```

#### Response - 0 OK
```proto
message GetMissingLayersResp {
  string chartId = 1;
  string namespace = 2;
  string maintainer = 3;
  string apiVersion = 4;
  string schemaVersion = 5;
  map<string, DataSource> dataSources = 6;
  map<string, StoredProcedure> storedProcedures = 7;
  map<string, EventTrigger> eventTriggers = 8;
  map<string, Event> events = 9;
}
```

#### /DeleteChart
The endpoint for deleting charts and cleaning up orphaned nodes from the graph database.

#### Request body
```proto
message DeleteChartReq {
  string id = 1;
  string name = 2;
  string namespace = 3;
  string maintainer = 4;
  string schemaVersion = 5;
  string kind = 6;
}
```

#### Response - 0 OK

#### /UpdateChart
The endpoint for updating a chart’s metadata, version, and associated resources.

#### Request body
```proto
message StarChart {
  string apiVersion = 1;
  string schemaVersion = 2;
  string kind = 3;

  MetadataChart metadata = 4;
  Chart chart = 5;
}
```

#### Response - 0 OK
```proto
message PutChartResp {
  string id = 1;
  string apiVersion = 2;
  string schemaVersion = 3;
  string kind = 4;
  string name = 5;
  string namespace = 6;
  string maintainer = 7;
}
```

#### /SwitchCheckpoint
Compares two versions of the same chart and determines which components must be started, stopped, or downloaded when switching from one version to another.

#### Request body
```proto
message SwitchCheckpointReq {
  string chartId = 1;
  string namespace = 2;
  string maintainer = 3;
  string oldVersion = 4;
  string newVersion = 5;
  repeated string layers = 6;
} 
```

#### Response - 0 OK
```proto
message LayersResp {
  map<string, DataSource> dataSources = 1;
  map<string, StoredProcedure> storedProcedures = 2;
  map<string, EventTrigger> eventTriggers = 3;
  map<string, Event> events = 4;
}
message SwitchCheckpointResp {
  LayersResp start = 1;
  LayersResp stop = 2;
  LayersResp download = 3;
}
```

#### /Timeline
Timeline returns a chronological view of all versions of a chart.

#### Request body
```proto
message TimelineReq {
  string chartId = 1;
  string namespace = 2;
  string maintainer = 3;
}
```

#### Response - 0 OK
```proto
message TimelineResp {
  repeated GetChartResp charts = 1;
}
```

#### /Extend
Extends an existing chart version by creating a new version node and adding only nodes that don’t already exist in the base version.

#### Request body
```proto
message ExtendReq {
  string oldVersion = 1;
  StarChart chart = 2;
}
```

#### Response - 0 OK
```proto
message PutChartResp {
  string id = 1;
  string apiVersion = 2;
  string schemaVersion = 3;
  string kind = 4;
  string name = 5;
  string namespace = 6;
  string maintainer = 7;
}
```