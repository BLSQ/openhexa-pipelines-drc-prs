"""Utility functions for DHIS2 dataset operations."""

import logging
from typing import Any

import polars as pl
import requests
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets
from requests.exceptions import HTTPError, RequestException

logger = logging.getLogger(__name__)


def dhis2_request(session: requests.Session, method: str, url: str, **kwargs: Any) -> dict:
    """Wrapper around requests to handle DHIS2 GET/PUT with error handling.

    Parameters
    ----------
    session : requests.Session
        Session object used to perform requests.
    method : str
        HTTP method: 'get' or 'put'.
    url : str
        Full URL for the request.
    **kwargs
        Additional arguments for session.request (json, params, etc.)

    Returns
    -------
    dict
        Either the response JSON or an error payload with 'error' and 'status_code'.
    """
    try:
        r = session.request(method, url, **kwargs)
        r.raise_for_status()
        return r.json()
    except HTTPError as e:
        try:
            return {
                "error": f"HTTP error during {method.upper()} {e} status_code: {r.status_code} response: {r.json()}"
            }
        except Exception:
            return {"error": f"HTTP error during {method.upper()} {e} status_code: {r.status_code}"}
    except RequestException as e:
        return {"error": f"Request error during {method.upper()} {url}: {e}"}
    except Exception as e:
        return {"error": f"Unexpected error during {method.upper()} {url}: {e}"}


def push_dataset_org_units(
    dhis2_client: DHIS2,
    source_dataset_id: str,
    target_dataset_id: str,
    dry_run: bool = True,
) -> dict:
    """Updates the organisation units of a DHIS2 dataset.

    Parameters
    ----------
    dhis2_client : DHIS2
        DHIS2 client for the target instance.
    source_dataset_id : str
        The ID of the dataset from where to retrieve the org unit ids.
    target_dataset_id : str
        The ID of the dataset to be updated.
    dry_run : bool, optional
        If True, performs a dry run without making changes (default is True).

    Returns
    -------
    dict
        The response from the DHIS2 API, or an error payload.
    """
    datasets = get_datasets(dhis2_client)
    source_dataset = datasets.filter(pl.col("id").is_in([source_dataset_id]))
    target_dataset = datasets.filter(pl.col("id").is_in([target_dataset_id]))
    source_ous = source_dataset["organisation_units"].explode().to_list()
    target_ous = target_dataset["organisation_units"].explode().to_list()

    # here first check if the list of ids is different
    to_add = set(source_ous) - set(target_ous)  # missing in target
    to_remove = set(target_ous) - set(source_ous)  # extra in target
    diff_org_units = to_add | to_remove
    if len(diff_org_units) == 0:
        current_run.log_info("Source and target dataset organisation units are in sync, no update needed.")
        return {"status": "skipped", "message": "No update needed, org units are identical."}

    current_run.log_info(
        f"Found {len(to_add)} org units to add and {len(to_remove)} org units to remove "
        f"from target dataset '{target_dataset['name'].item()}' ({target_dataset_id})."
    )

    # Step 1: GET current dataset
    url = f"{dhis2_client.api.url}/dataSets/{target_dataset_id}"
    dataset_payload = dhis2_request(dhis2_client.api.session, "get", url)
    if "error" in dataset_payload:
        return dataset_payload

    # Step 2: Update organisationUnits (just push the source OUs)
    dataset_payload["organisationUnits"] = [{"id": ou_id} for ou_id in source_ous]

    # Step 3: PUT updated dataset
    update_response = dhis2_request(
        dhis2_client.api.session, "put", url, json=dataset_payload, params={"dryRun": str(dry_run).lower()}
    )

    if "error" in update_response:
        current_run.log_info(f"Error updating dataset {target_dataset_id}: {update_response['error']}")
        logging.error(f"Error updating dataset {target_dataset_id}: {update_response['error']}")
    else:
        msg = f"Dataset {target_dataset['name'].item()} ({target_dataset_id}) org units updated: {len(source_ous)}"
        current_run.log_info(msg)
        logging.info(msg)

    return update_response
