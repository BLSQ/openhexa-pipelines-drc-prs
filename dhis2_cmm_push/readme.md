## DHIS2 CMM Push Pipeline

### Overview

This pipeline automates the extraction, computation, and push of CMM (Consommation mensuelle moyenne) data elements between DHIS2 instances. It is designed to extract monthly data, compute rolling averages, and push results to the target DHIS2 dataset.

### Main Features

#### 1. Data Extraction

- **extract_data**: Extracts data elements from the source DHIS2 instance, using settings from `extract_config.json`.
- Extraction is performed for specified periods and organizational units, with a rolling window (default 6 months) for CMM computation.
- Data is saved in parquet format for efficient processing.

#### 2. CMM Computation

- **compute_cmm_and_queue**: Computes the CMM (monthly average) for each data element and org unit over the defined window.
- Aggregates values using mean, filters by category option combo, and formats results for DHIS2 import.
- Results are enqueued for pushing.

#### 3. Organization Unit Synchronization

- **update_dataset_org_units**: Updates the org units of the target dataset in DHIS2, ensuring alignment with the source dataset.
- Checks for differences and updates only if needed.

#### 4. Data Push

- **push_data**: Pushes computed CMM data to the target DHIS2 instance, using mappings defined in `push_config.json`.
- Supports batching, dry-run mode, and custom import strategies.
- Applies mapping functions to format data elements for import.

### Configuration

- **extract_config.json**: Defines source connection, extraction window, data elements, org unit level, and dataset UID.
- **push_config.json**: Specifies target connection, import strategy, dry-run mode, max post size, and mappings for data elements.

#### Example Extract Configuration

```json
{
	"SETTINGS": {
		"SOURCE_DHIS2_CONNECTION": "drc-prs",
		"CMM_MONTHS_WINDOW": 6,
		"NUMBER_MONTHS_WINDOW": 1,
		"STARTDATE": "202509",
		"ENDDATE": "202509",
		"MODE": "DOWNLOAD_REPLACE"
	},
	"DATA_ELEMENTS": {
		"EXTRACTS": [
			{
				"EXTRACT_UID": "cmm_fosa",
				"UIDS": [...],
				"ORG_UNITS_LEVEL": 5,
				"DATASET_UID": "wMCnDAQfGZN",
				"FREQUENCY": "MONTHLY"
			}
		]
	}
}
```

#### Example Push Configuration

```json
{
	"SETTINGS": {
		"TARGET_DHIS2_CONNECTION": "drc-prs",
		"IMPORT_STRATEGY": "CREATE_AND_UPDATE",
		"DRY_RUN": true,
		"MAX_POST": 2000,
		"PUSH_WAIT_MINUTES": 1
	},
	"DATA_ELEMENTS": {
		"EXTRACTS": [
			{
				"EXTRACT_UID": "cmm_fosa",
				"MAPPINGS": {
					"imqSmhfQTPN": {
						"UID": "frK7p1wlD1S",
						"CATEGORY_OPTION_COMBO": { "cjeG5HSWRIU": "HllvX50cXC0" },
						"ATTRIBUTE_OPTION_COMBO": {}
					},
					...
				}
			}
		]
	}
}
```

### Data Flow

1. **Extract data** → 2. **Compute CMM** → 3. **Sync org units** → 4. **Push results**

### Usage

1. Configure extraction and push settings in the JSON files.
2. Run the pipeline to extract, compute, and push CMM data.
3. Review logs for each step in the respective logs directory.
