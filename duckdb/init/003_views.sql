-- Template: bootstrap_duckdb.py substitutes {root} with MARKET_LAKE_ROOT.
-- Do NOT run directly — use: python scripts/build/bootstrap_duckdb.py

CREATE OR REPLACE VIEW canonical.dim_symbol AS
SELECT * FROM read_parquet('{root}/canonical/dimensions/dim_symbol/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW canonical.dim_option_contract AS
SELECT * FROM read_parquet('{root}/canonical/dimensions/dim_option_contract/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW canonical.dim_calendar AS
SELECT * FROM read_parquet('{root}/canonical/dimensions/dim_calendar/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW canonical.vw_prices_daily AS
SELECT * FROM read_parquet('{root}/canonical/facts/fact_underlying_bar_daily/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW canonical.vw_option_eod AS
SELECT * FROM read_parquet('{root}/canonical/facts/fact_option_eod/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW features.vw_option_features_daily AS
SELECT * FROM read_parquet('{root}/canonical/features/fact_option_feature_daily/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW canonical.vw_macro_series AS
SELECT * FROM read_parquet('{root}/canonical/facts/fact_macro_series/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW metadata.vw_dataset_manifest AS
SELECT * FROM read_parquet('{root}/canonical/metadata/fact_dataset_manifest/**/*.parquet', union_by_name=true);

CREATE OR REPLACE VIEW canonical.vw_ff_factors_daily AS
SELECT * FROM read_parquet('{root}/canonical/facts/fact_ff_factors_daily/**/*.parquet', union_by_name=true);
