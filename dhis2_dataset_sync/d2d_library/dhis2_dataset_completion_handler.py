import json
import logging

import pandas as pd
import requests
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2


class DatasetCompletionSync:
    """Main class to handle pushing data to DHIS2.

    ATTENTION: This syncer assumes the source and target DHIS2 instances
     have the same organisation units configured.
    """

    def __init__(
        self,
        source_dhis2: DHIS2,
        target_dhis2: DHIS2,
        import_strategy: str = "CREATE_AND_UPDATE",
        dry_run: bool = True,
        logger: logging.Logger | None = None,
    ):
        self.source_dhis2 = source_dhis2
        self.target_dhis2 = target_dhis2
        if import_strategy not in {"CREATE", "UPDATE", "CREATE_AND_UPDATE"}:
            raise ValueError("Invalid import strategy (use 'CREATE', 'UPDATE' or 'CREATE_AND_UPDATE')")
        self.import_strategy = import_strategy
        self.dry_run = dry_run
        self.import_summary = {
            "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
            "errors": {"fetch_errors": 0, "push_errors": 0},
        }
        self.completion_table = pd.DataFrame()
        self.logger = logger if logger else logging.getLogger(__name__)

    def _fetch_completion_status_from_source(
        self,
        dataset_id: str,
        period: str,
        org_unit: str,
        timeout: int = 5,
    ) -> list[dict]:
        """Fetch completion status from source DHIS2.

        Args:
            dataset_id: The dataset ID to fetch completion status for.
            period: The period for which to fetch the completion status.
            org_unit: The organisation unit to fetch completion status for.
            timeout: Timeout for the request in seconds.

        Returns:
            list[dict]: A list of completion status dictionaries from the DHIS2 API.
                Returns an empty list if the request fails or no data is found.
        """
        endpoint = f"{self.source_dhis2.api.url}/completeDataSetRegistrations"
        params = {"period": period, "orgUnit": org_unit, "children": "true", "dataSet": dataset_id}

        try:
            response = self.source_dhis2.api.session.get(endpoint, params=params, timeout=timeout)
            response.raise_for_status()  # raise exception for HTTP errors
            try:
                completion = response.json().get("completeDataSetRegistrations", [])
            except ValueError as e:
                self.import_summary["errors"]["fetch_errors"] += 1
                self.logger.error(f"Invalid JSON from {endpoint} for ds:{dataset_id} pe:{period} ou:{org_unit}: {e}")
                return []
            return completion if completion else []
        except requests.RequestException as e:
            self.import_summary["errors"]["fetch_errors"] += 1
            self.logger.error(
                f"GET request to {self.source_dhis2.api.url} failed to retrieve completion status for "
                f"ds: {dataset_id} pe: {period} ou: {org_unit} failed : {e}"
            )
        return []

    def _push_completion_status_to_target(
        self,
        dataset_id: str,
        period: str,
        org_unit: str,
        date: str,
        completed: bool,
        timeout: int = 5,
    ) -> None:
        """Perform a PUT request (or POST with importStrategy) to a DHIS2 API endpoint.

        Args:
        dataset_id: The dataset ID to push completion status for.
        period: The period for which to push the completion status.
        org_unit: The organisation unit to push completion status for.
        date: The date of completion.
        completed: Whether the dataset is marked as completed.
        timeout: Timeout for the request in seconds.

        Raises:
            requests.HTTPError if the request fails after retries.
        """
        endpoint = f"{self.target_dhis2.api.url}/completeDataSetRegistrations"
        payload = {
            "completeDataSetRegistrations": [
                {
                    "organisationUnit": org_unit,
                    "period": period,
                    "completed": completed,
                    "date": date,
                    "dataSet": dataset_id,
                }
            ]
        }
        params = {
            "dryRun": str(self.dry_run).lower(),
            "importStrategy": self.import_strategy,
            "preheatCache": True,
            "skipAudit": True,
            "reportMode": "FULL",
        }

        response = None
        try:
            response = self.target_dhis2.api.session.post(endpoint, json=payload, params=params, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as e:
            self.import_summary["errors"]["push_errors"] += 1
            self.logger.error(f"Push (PUT) request failed for ds:{dataset_id} ou:{org_unit} pe:{period} error: {e}")
        except Exception as e:
            self.import_summary["errors"]["push_errors"] += 1
            self.logger.error(f"Push (PUT) request failed for ds:{dataset_id} ou:{org_unit} pe:{period} error: {e}")
        finally:
            self._process_response(ds=dataset_id, pe=period, ou=org_unit, response=response)

    def _try_build_source_completion_table(
        self, pyramid: pd.DataFrame, level: int, dataset_id: str, period: str
    ) -> None:
        """Build a completion status table for all organisation units in the pyramid."""
        if pyramid is None:
            return

        if level is None:
            level = 2  # default to level 2

        ous = pyramid[pyramid["level"] == level]["id"].unique().tolist()
        if not ous:
            return

        current_run.log_info(f"Building source completion table for {len(ous)} OUs level {level} period {period}.")
        completion_statuses = []
        for ou in ous:
            completion = self._fetch_completion_status_from_source(
                dataset_id=dataset_id, period=period, org_unit=ou, timeout=30
            )
            if completion:
                completion_statuses.extend(completion)

        self.completion_table = pd.DataFrame(completion_statuses)

    def _get_source_completion_status_for_ou(self, dataset_id: str, period: str, org_unit: str) -> dict | None:
        """Handle fetching completion status for a specific org unit.

        Returns:
            list: The completion status as dictionaries for the specified org unit (children) if found, otherwise [].
        """
        if not self.completion_table.empty:
            completion_status = self.completion_table[self.completion_table["organisationUnit"] == org_unit]
            if not completion_status.empty:
                return completion_status.iloc[0].to_dict()

        first_value = self._fetch_completion_status_from_source(dataset_id=dataset_id, period=period, org_unit=org_unit)
        return first_value[0] if first_value else None

    def sync(
        self,
        source_dataset_id: str,
        target_dataset_id: str,
        dhis2_pyramid: pd.DataFrame | None,
        level: int | None,
        period: list[str],
        org_units: list[str],
        logging_interval: int = 2000,
    ) -> None:
        """Sync completion status between datasets.

        source_dataset_id: The dataset ID in the source DHIS2 instance.
        target_dataset_id: The dataset ID in the target DHIS2 instance.
        dhis2_pyramid: DataFrame containing the DHIS2 organisation unit pyramid (optional).
        level: The organisational unit level to consider in the pyramid (optional).
        period: The period for which to sync the completion status.
        org_units: List of organisation unit IDs to process.
        logging_interval: Interval for logging progress (defaults to 2000).

        """
        self._reset_import_summary()
        self._try_build_source_completion_table(
            pyramid=dhis2_pyramid, level=level, dataset_id=source_dataset_id, period=period
        )

        try:
            for idx, ou in enumerate(org_units, start=1):
                completion_status = self._get_source_completion_status_for_ou(
                    dataset_id=source_dataset_id,
                    period=period,
                    org_unit=ou,
                )

                if not completion_status:
                    continue

                self._push_completion_status_to_target(
                    dataset_id=target_dataset_id,
                    period=period,
                    org_unit=ou,
                    date=completion_status.get("date"),
                    completed=completion_status.get("completed"),
                )

                if idx % logging_interval == 0 or idx == len(org_units):
                    current_run.log_info(f"{idx} / {len(org_units)} OUs processed")

        except Exception as e:
            self.logger.error(f"Error setting dataset completion for dataset {target_dataset_id}, period {period}: {e}")
        finally:
            self._log_summary(org_units=org_units, period=period)

    def _log_summary(self, org_units: list, period: str) -> None:
        msg = (
            f"Dataset completion for period {period} summary: {self.import_summary['import_counts']} "
            f"total org units: {len(org_units)}"
        )
        current_run.log_info(msg)
        self.logger.info(msg)

        if self.import_summary["errors"]["fetch_errors"] > 0:
            msg = (
                f"{self.import_summary['errors']['fetch_errors']} out of "
                f"{len(org_units)} completion statuses failed to fetch."
            )
            current_run.log_warning(msg)
            self.logger.warning(msg)

        if self.import_summary["errors"]["push_errors"] > 0:
            msg = (
                f"{self.import_summary['errors']['push_errors']} "
                f"out of {len(org_units)} completion statuses failed to push."
            )
            current_run.log_warning(msg)
            self.logger.warning(msg)

    def _process_response(self, ds: str, pe: str, ou: str, response: dict) -> None:
        """Log the response from the DHIS2 API after pushing completion status."""
        json_or_none = self._safe_json(response)
        if not json_or_none:
            self.import_summary["errors"]["push_errors"] += 1
            self.logger.error(
                f"No JSON response received for completion request ds: {ds} pe: {pe} ou: {ou} from DHIS2 API."
            )
            return

        conflicts: list[str] = json_or_none.get("conflicts", {})
        status = json_or_none.get("status")
        if status in ["ERROR", "WARNING"] or conflicts:
            for conflict in conflicts:
                self.import_summary["errors"]["push_errors"] += 1
                self.logger.error(
                    f"Conflict pushing completion for ds: {ds} pe: {pe} ou: {ou} status: {status} - {conflict}"
                )
            self._update_import_summary(response=json_or_none)
            return

        if status == "SUCCESS":
            self.logger.info(f"Successfully pushed to target completion ds: {ds} pe:{pe} ou: {ou}")
            self._update_import_summary(response=json_or_none)

    def _safe_json(self, r: requests.Response) -> dict | None:
        if r is None:
            return None
        try:
            return r.json()
        except (ValueError, json.JSONDecodeError):
            return None

    def _update_import_summary(self, response: dict) -> None:
        if response:
            import_counts = response.get("importCount", {})
            for key in ["imported", "updated", "ignored", "deleted"]:
                self.import_summary["import_counts"][key] += import_counts.get(key, 0)

    def _reset_import_summary(self) -> None:
        self.import_summary = {
            "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
            "errors": {"fetch_errors": 0, "push_errors": 0},
        }
