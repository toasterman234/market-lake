# raw/thetadata/

Place raw ThetaData vendor files here before running ingest scripts.

```
raw/thetadata/
  contracts/        ← option contract listings (CSV or Parquet)
  options_eod/      ← daily EOD option quotes, volume, OI, Greeks
  vrp/              ← raw VRP source files (if applicable)
```

**Important:** Raw files are immutable — never edit them.
Ingest scripts read from here and write normalized output to `canonical/`.

These directories are excluded from git (see `.gitignore`).
