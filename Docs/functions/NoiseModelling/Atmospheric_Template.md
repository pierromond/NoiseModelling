# Atmospheric Template

> **Script:** `noisemodelling-scripts/.../NoiseModelling/Atmospheric_Template.groovy`

## Overview

Generates a default atmospheric settings table from the `PERIOD` field of a noise emission table.

The generated table is meant to be **exported, edited, and re-imported** before running `Noise_level_from_source` or `Noise_level_from_traffic`. It allows you to set different temperature, humidity, pressure, wind rose, and other propagation conditions for each time period of the simulation.

---

## Inputs

### Mandatory

| Parameter | Type | Description |
|---|---|---|
| `tableSourcesEmission` | `String` | Name of the sources emission table (e.g. `SOURCES_EMISSION`) |

The source emission table must contain:
- **`IDSOURCE`** *(INTEGER)* — identifier linked to the primary key of the roads/sources table
- **`PERIOD`** *(VARCHAR)* — time period label (e.g. `D`, `E`, `N` for day/evening/night)

### Optional

| Parameter | Type | Default | Description |
|---|---|---|---|
| `tablePeriodAtmosphericSettings` | `String` | `SOURCES_ATMOSPHERIC` | Name of the output atmospheric settings table |

---

## Output table schema — `SOURCES_ATMOSPHERIC`

| Column | Type | Default | Description |
|---|---|---|---|
| `PERIOD` | `VARCHAR` | — | Time period label *(PRIMARY KEY)* |
| `WINDROSE` | `ARRAY(16)` | `[0.5, …, 0.5]` | Probability of favourable propagation conditions for each of the 16 wind directions |
| `TEMPERATURE` | `FLOAT` | `15` °C | Air temperature |
| `PRESSURE` | `FLOAT` | `101325` Pa | Atmospheric pressure |
| `HUMIDITY` | `FLOAT` | `70` % | Relative air humidity |
| `GDISC` | `BOOLEAN` | `true` | Accept G discontinuity in CNOSSOS ground effect computation |
| `PRIME2520` | `BOOLEAN` | `false` | Use prime values to compute CNOSSOS equation 2.5.20 |

Default values come from `AttenuationParameters` (ISO/CNOSSOS reference conditions).

---

## Workflow

```
SOURCES_EMISSION          Atmospheric_Template         SOURCES_ATMOSPHERIC
  (with PERIOD col)  ──►  reads unique PERIOD  ──►   one row per period
                          writes default params        (edit before use)
                                                            │
                                                            ▼
                                               Noise_level_from_source
                                               Noise_level_from_traffic
```

1. The script reads the distinct `PERIOD` values from `tableSourcesEmission`.
2. For each period it calls `AttenuationParameters.writeToDatabase()` with default values.
3. The resulting table is exported (CSV or shapefile), manually edited to reflect actual meteorological conditions, then re-imported.
4. The edited table is passed to `Noise_level_from_source` or `Noise_level_from_traffic` to apply period-specific atmospheric corrections.

---

## Default atmospheric constants (`AttenuationParameters`)

| Constant | Value | Description |
|---|---|---|
| `K_0` | 273.15 K | Absolute zero |
| `Pref` | 101 325 Pa | Standard atmosphere |
| `Kref` | 293.15 K | Reference ambient temperature |
| `FmolO` | 0.209 | Mole fraction of oxygen |
| `FmolN` | 0.781 | Mole fraction of nitrogen |
| `KvibO` | 2 239.1 K | Vibrational temperature of O₂ |
| `KvibN` | 3 352.0 K | Vibrational temperature of N₂ |

---

## Example usage (Groovy / WPS)

```groovy
// Inputs map
def inputs = [
    tableSourcesEmission          : "SOURCES_EMISSION",
    tablePeriodAtmosphericSettings: "MY_ATMO_SETTINGS"
]

// Run the block
Atmospheric_Template.exec(connection, inputs)

// → Table MY_ATMO_SETTINGS is created with one row per PERIOD found in SOURCES_EMISSION
// → Edit the table (e.g. set TEMPERATURE=10, HUMIDITY=80 for period "N")
// → Re-import and pass to Noise_level_from_source
```

---

## Related scripts

- [`Noise_level_from_source`](Noise_level_from_source.rst) — uses the atmospheric settings table for propagation
- [`Noise_level_from_traffic`](Noise_level_from_traffic.rst) — idem with traffic emission
- [`Road_Emission_from_Traffic`](Road_Emission_from_Traffic.rst) — upstream step that generates `SOURCES_EMISSION`

---

## Wind rose format

The `WINDROSE` column is a 16-element array where each value is the **probability of favourable propagation conditions** (0 to 1) for a given wind sector. The sectors are uniformly distributed over 360°, starting at North, clockwise:

| Index | Direction | Default |
|---|---|---|
| 0 | N | 0.5 |
| 1 | NNE | 0.5 |
| … | … | … |
| 15 | NNW | 0.5 |

A value of `0.5` means favourable conditions occur 50 % of the time for that direction (CNOSSOS default).  
A value of `0.0` means strictly homogeneous (unfavourable) conditions.  
A value of `1.0` means always favourable conditions for that direction.
