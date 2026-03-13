## DHIS2 Dataset Sync Pipeline

### Overview

This pipeline automates the synchronization of datasets, organizational units, and data elements between DHIS2 instances. It aligns org units, synchronizes dataset org units, extracts and pushes data elements, and optionally syncs dataset completion statuses.

### Main Features

#### 1. Organization Unit Synchronization

- **sync_organisation_units**: Aligns org units between source and target DHIS2, extracts pyramid data, and saves it for validation.
- Uses selection, inclusion of children, and level limits from `sync_config.json`.

#### 2. Dataset Organization Unit Sync

- **sync_dataset_organisation_units**: Aligns org units for datasets between source and target DHIS2, using mappings from `sync_config.json`.
- Handles special cases for full pyramid and zones de santé datasets.

#### 3. Data Extraction

- **extract_data**: Extracts data elements from the source DHIS2 based on configuration in `extract_config.json`.
- Extraction is performed for specified periods and org units, with settings for window, start/end dates, and mode.
- Data is saved in parquet format for efficient processing.

#### 4. Data Push

- **push_data**: Pushes extracted data elements to the target DHIS2 instance, using mappings from `push_config.json`.
- Supports batching, dry-run mode, and custom import strategies.
- Applies mapping functions to format data elements for import and filters by dataset org units.

#### 5. Dataset Completion Sync (Optional)

- **sync_dataset_statuses**: Syncs dataset completion statuses between source and target DHIS2, using backup data from previous pushes.

### Configuration

- **extract_config.json**: Defines source connection, extraction window, data elements, org unit level, and frequency.
- **push_config.json**: Specifies target connection, import strategy, dry-run mode, max post size, and mappings for data elements.
- **sync_config.json**: Controls org unit selection, dataset mappings, and pyramid/zones de santé handling.

#### Example Extract Configuration

```json
{
	"SETTINGS": {
		"SOURCE_DHIS2_CONNECTION": "drc-snis",
		"NUMBER_MONTHS_WINDOW": 4,
		"STARTDATE": "",
		"ENDDATE": "",
		"MODE": "DOWNLOAD_REPLACE"
	},
	"DATA_ELEMENTS": {
		"EXTRACTS": [
			{
				"EXTRACT_UID": "level_zs",
				"UIDS": [...],
				"ORG_UNITS_LEVEL": 3,
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
		"DRY_RUN": false,
		"MAX_POST": 5000,
		"PUSH_WAIT_MINUTES": 5
	},
	"DATA_ELEMENTS": {
		"EXTRACTS": [
			{
				"EXTRACT_UID": "level_zs",
				"SOURCE_DATASET_UID": "k8mWHTAqHYZ",
				"TARGET_DATASET_UID": "jp5eWxb66XT",
				"SYNC_PERIOD_WINDOW": 4,
				"MAPPINGS": { ... }
			}
		]
	}
}
```

#### Example Sync Configuration

```json
{
	"ORG_UNITS": {
		"SELECTION": {
			"UIDS": [...],
			"INCLUDE_CHILDREN": true,
			"LIMIT_LEVEL": 5
		},
		"ORG_UNITS_GROUPS": { "UIDS": [] }
	},
	"DATASETS": {
		"DxDuYrrZSa7": ["wMCnDAQfGZN"],
		"FULL_PYRAMID": ["dbf1uGX1XU3"],
		"ZONES_SANTE": ["Om2WgL4TNEy", "jp5eWxb66XT"]
	}
}
```

### Data Flow

1. **Sync org units** → 2. **Sync dataset org units** → 3. **Extract data** → 4. **Push data** → 5. *(Optional)* **Sync dataset completion**

### Usage

1. Configure extraction, push, and sync settings in the JSON files.
2. Run the pipeline to synchronize org units, datasets, extract and push data, and optionally sync completion statuses.
3. Review logs for each step in the respective logs directory.
