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
}

message Metadata {
  string id = 1;
  string name = 2;
  string image = 3;
  string prefix = 4;
  string topic = 5;
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
    string name = 1;
    string namespace = 2;
    string maintainer = 3;
    string description = 4;
    string visibility = 5;
    string engine = 6;
    map<string, string> labels = 7;
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
  string apiVersion = 1;
  string schemaVersion = 2;
  string kind = 3;
  string name = 4;
  string namespace = 5;
  string maintainer = 6;
}
```

#### /GetChartMetadata
The endpoint for retrieving complete chart by metadata: name, namespace, and maintainer.

#### Request body
```proto
message GetChartFromMetadataReq {
  string name = 1;
  string namespace = 2;
  string maintainer = 3;
}
```

#### Response - 0 OK
```proto
message GetChartFromMetadataResp {
  MetadataChart metadata = 1;
  Chart chart = 2;
}
```

#### /GetChartsLabels
The endpoint for querying charts by namespace, maintainer, and labels.

#### Request body
```proto
message GetChartsLabelsReq {
  string namespace = 1;
  string maintainer = 2;
  map<string, string> labels = 3;
}
```

#### Response - 0 OK
```proto
message GetChartsLabelsResp {
  repeated GetChartFromMetadataResp charts = 1;
}
```

#### /DeleteChart
The endpoint for deleting charts and cleaning up orphaned nodes from the graph database.

#### Request body
```proto
message DeleteChartReq {
  string name = 1;
  string namespace = 2;
  string maintainer = 3;
  string apiVersion = 4;
  string schemaVersion = 5;
  string kind = 6;
}
```

#### Response - 0 OK