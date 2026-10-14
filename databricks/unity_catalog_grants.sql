-- Unity Catalog governance for the pharmacy lakehouse (run once by a metastore admin).
CREATE CATALOG IF NOT EXISTS pharmacy;
CREATE SCHEMA IF NOT EXISTS pharmacy.bronze;
CREATE SCHEMA IF NOT EXISTS pharmacy.silver;
CREATE SCHEMA IF NOT EXISTS pharmacy.gold;
CREATE SCHEMA IF NOT EXISTS pharmacy.quarantine;

-- Engineers build the pipeline; analysts only ever see de-identified Gold data.
GRANT USE CATALOG ON CATALOG pharmacy TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA pharmacy.bronze TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA pharmacy.silver TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA pharmacy.gold TO `data-engineers`;

GRANT USE CATALOG ON CATALOG pharmacy TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA pharmacy.gold TO `analysts`;

-- Quarantine may still hold raw identifiers: restricted to a small data-steward group.
GRANT USE SCHEMA, SELECT ON SCHEMA pharmacy.quarantine TO `phi-data-stewards`;

-- Delta maintenance for the large fact table
OPTIMIZE pharmacy.gold.fact_rx_fill ZORDER BY (store_id, ndc11);
