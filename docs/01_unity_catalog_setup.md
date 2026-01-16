# Unity Catalog Setup

## Objective
Set up logical data goverance using Unity Catalog for a Databricks-based lakehouse.


## Catalog Selection
Due to workspace-level restrictions (no metastore storage root configured), creation of a new catalog with managed or external storage was not permitted.


An existing Unity Catalog-backed catalog `migration_project_db_ws` was used instead.

## Schema Design
The following schemas were created to implement the Medallion Architecture:


- bronze - Raw, append-only ingestion layer
- silver - Cleaned and validated data
- gold - Business-level aggregations

These schemas were created at the same level as `default` and `information_schema`, which is expected behavior in Unity Catalog.


## Issues Faced
- Metastore storage root URL not configured
- External locations could not be created due to permission restrictions


## Resolution
Used an existing Unity Catalog catalog and logically separated layers using schemas.


## Production Consideration
In a production environment, a dedicated catalog backed by cloud object storage (ADLS/S3) would be created with external locations and fine-grained access control.



## Justification on the Task List A1. Configure Unity Catalog with metastore, catalog, schema, external locations.

### Unity Catalog Configuration (A1)

Unity Catalog was enabled at the workspace level with an active metastore. 
Due to workspace permission constraints, creation of a new catalog with a managed or external storage location was not permitted.

An existing Unity Catalog–backed catalog (`migration_project_db_ws`) was used instead.
Medallion architecture was implemented using schema-level separation (bronze, silver, gold).

External locations were evaluated conceptually. In a production environment, external locations backed by ADLS would be created and associated with the catalog.

```text
[Oracle / Files]
        ↓
[External Location – ADLS]
        ↓
[Bronze Tables – Unity Catalog]
```

“External locations are centrally managed by platform administrators and were simulated in this assessment due to workspace constraints.”
