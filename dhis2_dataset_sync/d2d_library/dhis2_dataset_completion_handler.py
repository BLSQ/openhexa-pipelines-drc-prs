import json
import logging

import requests
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2

logger = logging.getLogger(__name__)


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
    ):
        self.source_dhis2 = source_dhis2
        self.target_dhis2 = target_dhis2
        if import_strategy not in {"CREATE", "UPDATE", "CREATE_AND_UPDATE"}:
            raise ValueError("Invalid import strategy (use 'CREATE', 'UPDATE' or 'CREATE_AND_UPDATE')")
        self.import_strategy = import_strategy
        self.dry_run = dry_run
        self.import_summary = {
            "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
        }

    def _fetch_completion_status_from_source(
        self,
        dataset_id: str,
        period: str,
        org_unit: str,
        retries: int = 2,
        timeout: int = 5,
    ) -> dict | None:
        """Fetch completion status from source DHIS2.

        Args:
            dataset_id: The dataset ID to fetch completion status for.
            period: The period for which to fetch the completion status.
            org_unit: The organisation unit to fetch completion status for.
            retries: Number of times to retry the request in case of failure.
            timeout: Timeout for the request in seconds.

        Returns:
            dict: The JSON response from the DHIS2 API if successful, otherwise None.
        """
        endpoint = f"{self.source_dhis2.api.url}/completeDataSetRegistrations"
        params = {"period": period, "orgUnit": org_unit, "children": "true", "dataSet": dataset_id}

        attempt = 0
        while attempt < retries:
            try:
                response = self.source_dhis2.api.session.get(endpoint, params=params, timeout=5)
                response.raise_for_status()  # raise exception for HTTP errors
                return response.json().get("completeDataSetRegistrations")
            except requests.RequestException as e:
                attempt += 1
                logging.error(
                    f"GET request to {self.source_dhis2.api.url} "
                    f"ds: {dataset_id} pe: {period} ou: {org_unit} "
                    f"failed (attempt {attempt}/{retries}): {e!s}"
                )
        return None

    def _push_completion_status_to_target(
        self,
        dataset_id: str,
        period: str,
        org_unit: str,
        date: str,
        completed: bool,
        retries: int = 2,
        timeout: int = 5,
    ) -> None:
        """Perform a PUT request (or POST with importStrategy) to a DHIS2 API endpoint.

        Args:
        dataset_id: The dataset ID to push completion status for.
        period: The period for which to push the completion status.
        org_unit: The organisation unit to push completion status for.
        date: The date of completion.
        completed: Whether the dataset is marked as completed.
        retries: Number of times to retry the request in case of failure.
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

        attempt = 0
        while attempt < retries:
            try:
                response = self.target_dhis2.api.session.post(endpoint, json=payload, params=params, timeout=timeout)
                response.raise_for_status()
                self._process_response(ds=dataset_id, pe=period, ou=org_unit, response=response)
            except requests.RequestException as e:
                logging.error(
                    f"PUT request failed for ds:{dataset_id} ou:{org_unit} pe:{period} "
                    f"(attempt {attempt}/{retries}): {e!s}"
                )
                attempt += 1

    def sync(
        self,
        source_dataset_id: str,
        target_dataset_id: str,
        period: list[str],
        org_units: list[str],
    ) -> None:
        """Sync completion status between datasets."""
        self.import_summary = {
            "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
        }

        for ou in org_units:
            completion_status = self._fetch_completion_status_from_source(
                dataset_id=source_dataset_id, period=period, org_unit=ou
            )

            if completion_status is None:
                msg = (
                    f"Failed to fetch completion status from source {self.source_dhis2.api.url} :"
                    f"dataset {source_dataset_id} ,period {period}, org unit {ou}. Skipping."
                )
                logging.warning(msg)
                current_run.log_warning(msg)
                continue

            self._push_completion_status_to_target(
                dataset_id=target_dataset_id,
                period=period,
                org_unit=[ou],
                date=completion_status.get("date"),
                completed=completion_status.get("completed"),
            )

        current_run.log_info(
            f"Dataset completion for period {period} summary: {self.import_summary} total org units: {len(org_units)}"
        )

    def _process_response(self, ds: str, pe: str, ou: str, response: dict) -> None:
        """Log the response from the DHIS2 API after pushing completion status."""
        json_or_none = self._safe_json(response)
        if not json_or_none:
            logging.error(
                f"No JSON response received for completion request ds: {ds} pe: {pe} ou: {ou} from DHIS2 API."
            )
            return

        conflict: str = (json_or_none.json().get("conflicts") or [{}])[0].get("value")
        status = json_or_none.get("status")
        if conflict:
            logging.error(f"Conflict pushing completion for ds: {ds} pe: {pe} ou: {ou} status: {status} - {conflict}")
            self._update_import_summary(import_summary=json_or_none)
            return

        if status == "SUCCESS":
            logging.info(f"Successfully pushed completion status: ds: {ds} pe:{pe} ou: {ou}")
            self._update_import_summary(import_summary=json_or_none)

    def _safe_json(self, r: requests.Response) -> dict | None:
        try:
            return r.json()
        except (ValueError, json.JSONDecodeError):
            return None

    def _update_import_summary(self, response: dict) -> None:
        if not response:
            return
        import_counts = response.get("importCount", {})
        for key in ["imported", "updated", "ignored", "deleted"]:
            self.import_summary["import_counts"][key] += import_counts.get(key, 0)
