## DHIS2 CMM Morbidity Pipeline

### Overview

This pipeline automates the extraction, computation, and push of CMM morbidity indicators between DHIS2 instances. It is designed to synchronize organizational units, extract relevant data, compute custom indicators, and push results to a target DHIS2.

### Main Features

#### 1. Organization Unit Synchronization

- **sync_organisation_units**: Aligns org units between source and target DHIS2, extracts pyramid data, and saves it for later validation.
- **sync_organisation_unit_groups**: Updates org unit groups in the target DHIS2, ensuring datasets are correctly linked to the right org units.

#### 2. Data Extraction

- **extract_data**: Extracts data elements from the source DHIS2 based on configuration. Data is saved in parquet format for efficient processing.
- Extraction is performed per period and org unit, using settings from `extract_config.json` and `cmm_config.json`.

#### 3. Indicator Computation

- **compute_cmm_morbidity_indicators**: Computes CMM indicators for each extract and period, using formulas defined in `cmm_config.json`.
- Indicators are calculated using a flexible formula engine supporting operations like sum, multiply, constant, and conditional logic (e.g., `if orgUnitInGroupDescendant`).
- Example indicators:
	- **MII, TDR**: Sum of specific data elements and category option combos.
	- **SP**: Sum of several elements multiplied by a constant.
	- **AI60, AS100, ASAQ1-4**: Complex formulas involving sums, multiplications, and conditional logic based on org unit group membership.

#### 4. Data Push

- **push_data**: Pushes computed indicator data to the target DHIS2 instance, using mappings defined in the configuration.
- Supports batching, dry-run mode, and custom import strategies.

### Indicator Computation Logic

Formulas for each indicator are defined in [cmm_config.json](workspace/pipelines/dhis2_cmm_morbidity/configuration/cmm_config.json) under `EXTRACTS.FORMULAS`. The computation engine supports:

- **Sum**: Aggregates values from multiple data elements.
- **Multiply**: Multiplies sums or values by constants.
- **Constant**: Fixed values used in calculations.
- **Conditional (if)**: Applies different formulas based on org unit group membership.

Example (simplified):

```
"FORMULAS": {
	"MII": {
		"type": "sum",
		"items": [
			{ "dataElement": "aZwnLALknnj", "categoryOptionCombo": "xCV9NGB897u" },
			...
		]
	},
	"SP": {
		"type": "multiply",
		"left": {
			"type": "sum",
			"items": [...]
		},
		"right": { "type": "constant", "value": 3 }
	},
	"ASAQ1": {
		"type": "if",
		"condition": { "type": "orgUnitInGroupDescendant", "orgUnitGroup": "cOK4Feyi0nP" },
		"then": { ... },
		"else": { ... }
	}
}
```

### Data Flow

1. **Sync org units** → 2. **Extract data** → 3. **Compute indicators** → 4. **Push results**

### Configuration

- **cmm_config.json**: Defines indicator formulas, org unit groups, and extract settings.
- **extract_config.json**: Specifies which data elements to extract and extraction periods.
- **push_config.json**: Controls push behavior and mappings.

---

## Usage

1. Configure extraction and indicator formulas in the JSON files.
2. Run the pipeline to synchronize org units, extract data, compute indicators, and push results.
3. Review logs for each step in the respective logs directory.

