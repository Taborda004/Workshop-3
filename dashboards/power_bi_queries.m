// ============================================================================
// Workshop-3: Power BI Power Query (M) Scripts
// Connects directly to PostgreSQL Database: localhost:6432 / happiness_db
// ============================================================================

// ----------------------------------------------------------------------------
// 1. Table: dim_country
// ----------------------------------------------------------------------------
let
    Source = PostgreSQL.Database("localhost:6432", "happiness_db"),
    public_dim_country = Source{[Schema="public",Item="dim_country"]}[Data]
in
    public_dim_country


// ----------------------------------------------------------------------------
// 2. Table: dim_date
// ----------------------------------------------------------------------------
let
    Source = PostgreSQL.Database("localhost:6432", "happiness_db"),
    public_dim_date = Source{[Schema="public",Item="dim_date"]}[Data]
in
    public_dim_date


// ----------------------------------------------------------------------------
// 3. Table: fact_predictions
// ----------------------------------------------------------------------------
let
    Source = PostgreSQL.Database("localhost:6432", "happiness_db"),
    public_fact_predictions = Source{[Schema="public",Item="fact_predictions"]}[Data]
in
    public_fact_predictions


// ----------------------------------------------------------------------------
// 4. Table: raw_happiness_events (Auditoría de Ingesta y Resiliencia)
// ----------------------------------------------------------------------------
let
    Source = PostgreSQL.Database("localhost:6432", "happiness_db"),
    public_raw_happiness_events = Source{[Schema="public",Item="raw_happiness_events"]}[Data]
in
    public_raw_happiness_events
