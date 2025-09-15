import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta
from openhexa.sdk import current_run, workspace
from openhexa.toolbox.dhis2 import DHIS2


def connect_to_dhis2(connection_str: str, cache_dir: Path) -> DHIS2:
    """Establishes a connection to DHIS2 using the provided connection string and cache directory.

    Parameters
    ----------
    connection_str : str
        The connection string for DHIS2.
    cache_dir : Path
        The directory to use for caching DHIS2 data.

    Returns
    -------
    DHIS2
        An instance of the DHIS2 client.

    Raises
    ------
    Exception
        If there is an error while connecting to DHIS2.
    """
    try:
        connection = workspace.dhis2_connection(connection_str)
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)
        dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
        current_run.log_info(f"Connected to DHIS2 connection: {connection.url}")
        return dhis2_client
    except Exception as e:
        raise Exception(f"Error while connecting to DHIS2 {connection_str}: {e}") from e


def load_configuration(config_path: Path) -> dict:
    """Reads a JSON file configuration and returns its contents as a dictionary.

    Args:
        config_path (str): Root path of the pipeline to find the file.

    Returns:
        dict: Dictionary containing the JSON data.
    """
    try:
        with Path.open(config_path, "r") as file:
            data = json.load(file)

        current_run.log_info(f"Configuration loaded from {config_path}.")
        return data
    except FileNotFoundError as e:
        raise Exception(f"The file '{config_path}' was not found {e}") from e
    except json.JSONDecodeError as e:
        raise Exception(f"Error decoding JSON: {e}") from e
    except Exception as e:
        raise Exception(f"Unexpected error while loading configuration '{config_path}' {e}") from e


def retrieve_ou_list(dhis2_client: DHIS2, ou_level: int) -> list:
    """Retrieve a list of organisational unit IDs from DHIS2 filtered by the specified organisational unit level.

    Parameters
    ----------
    dhis2_client : DHIS2
        An instance of the DHIS2 client.
    ou_level : int
        The organisational unit level to filter by.

    Returns
    -------
    list
        A list of organisational unit IDs matching the specified level.

    Raises
    ------
    Exception
        If there is an error while retrieving the organisational unit IDs.
    """
    try:
        # Retrieve organisational units and filter by ou_level
        ous = pd.DataFrame(dhis2_client.meta.organisation_units())
        ou_list = ous.loc[ous.level == ou_level].id.to_list()

        # Log the result based on the OU level
        if ou_level == 5:
            current_run.log_info(f"Retrieved SNIS DHIS2 FOSA id list {len(ou_list)}")
        elif ou_level == 4:
            current_run.log_info(f"Retrieved SNIS DHIS2 Aires de Sante id list {len(ou_list)}")
        else:
            current_run.log_info(f"Retrieved SNIS DHIS2 OU level {ou_level} id list {len(ou_list)}")

        return ou_list

    except Exception as e:
        raise Exception(f"Error while retrieving OU id list for level {ou_level}: {e}") from e


def select_descendants(df: pd.DataFrame, parent_ids: list[str]) -> pd.DataFrame:
    """Select all rows from a hierarchical DataFrame that are descendants of the given parent IDs.

    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame containing at least two columns: "id" and "parent_id". Each row represents
        a node in a hierarchy, where "parent_id" references the parent node's "id".
    parent_ids : list[str]
        A list of parent IDs for which to retrieve all descendant rows.

    Returns
    -------
    pd.DataFrame
        A filtered DataFrame containing the rows with IDs in the input `parent_ids` and all
        of their descendants.

    Notes
    -----
    - The returned DataFrame is a **copy**, not a view of the original `df`.
      Modifying it will not affect the input DataFrame.
    - Works for hierarchies of any depth.
    """
    # Use a set to accumulate all descendant IDs
    all_ids = set(parent_ids)
    new_children = set(parent_ids)

    # Iteratively find children
    while new_children:
        # Find rows where parent_id is in new_children
        children = df[df["parent_id"].isin(new_children)]
        # Get their IDs
        child_ids = set(children["id"])
        # Only keep the new ones
        new_children = child_ids - all_ids
        # Add to all_ids
        all_ids.update(new_children)

    # Filter DataFrame to include only the parent and all descendants
    return df[df["id"].isin(all_ids)]


def merge_dataframes(dataframes: list[pd.DataFrame]) -> pd.DataFrame | None:
    """Merge a list of dataframes, excluding None values.

    Assume they shared the same columns.

    Args:
        dataframes (list[pd.DataFrame]): A list of dataframes to merge.

    Returns:
        pd.DataFrame: Concatenated dataframe, or None if all inputs are None.
    """
    # Filter out None values from the list
    not_none_df = [df for df in dataframes if df is not None]

    # Check if all columns match
    if len(not_none_df) > 1:
        first_columns = set(not_none_df[0].columns)
        for df in not_none_df[1:]:
            if set(df.columns) != first_columns:
                raise ValueError("DataFrames have mismatched columns and cannot be concatenated.")

    # Concatenate if there are valid dataframes, else return None
    return pd.concat(not_none_df) if not_none_df else None


def first_day_of_future_month(date: str, months_to_add: int) -> str:
    """Compute the first day of the month after adding a given number of months.

    Args:
        date (str): A date in the "YYYYMM" format.
        months_to_add (int): Number of months to add.

    Returns:
        str: The resulting date in "YYYY-MM-DD" format.
    """
    # Parse the input date string
    input_date = datetime.strptime(date, "%Y%m")
    target_date = input_date + relativedelta(months=months_to_add)

    return target_date.strftime("%Y-%m-01")


def save_to_parquet(data: pd.DataFrame, filename: Path) -> None:
    """Safely saves a DataFrame to a Parquet file using a temporary file and atomic replace.

    Args:
        data (pd.DataFrame): The DataFrame to save.
        filename (Path): The path where the Parquet file will be saved.
    """
    try:
        if not isinstance(data, pd.DataFrame):
            raise TypeError("The 'data' parameter must be a pandas DataFrame.")

        # Write to a temporary file in the same directory
        with tempfile.NamedTemporaryFile(suffix=".parquet", dir=filename.parent, delete=False) as tmp_file:
            temp_filename = Path(tmp_file.name)
            data.to_parquet(temp_filename, engine="pyarrow", index=False)

        # Atomically replace the old file with the new one
        temp_filename.replace(filename)

    except Exception as e:
        # Clean up the temp file if it exists
        if "temp_filename" in locals() and temp_filename.exists():
            temp_filename.unlink()
        raise RuntimeError(f"Failed to save parquet file to {filename}") from e


def read_parquet_extract(parquet_file: Path) -> pd.DataFrame:
    """Reads a Parquet file and returns its contents as a pandas DataFrame.

    Parameters
    ----------
    parquet_file : Path
        The path to the Parquet file to be read.

    Returns
    -------
    pd.DataFrame
        The contents of the Parquet file as a DataFrame.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    pd.errors.EmptyDataError
        If the Parquet file is empty.
    Exception
        For any other unexpected errors during reading.
    """
    try:
        ou_source = pd.read_parquet(parquet_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Error while loading the extract: File was not found {parquet_file}.") from None
    except pd.errors.EmptyDataError:
        raise pd.errors.EmptyDataError(f"Error while loading the extract: File is empty {parquet_file}.") from None
    except Exception as e:
        raise RuntimeError(f"Error while loading the extract: {parquet_file}. Error: {e}") from None

    return ou_source


def configure_logging(logs_path: Path, task_name: str):
    """Configure logging for the pipeline.

    Parameters
    ----------
    logs_path : Path
        Directory path where log files will be stored.
    task_name : str
        Name of the task to include in the log filename.

    This function creates the log directory if it does not exist and sets up logging to a file.
    """
    # Configure logging
    logs_path.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d-%H_%M")
    logging.basicConfig(
        filename=logs_path / f"{task_name}_{now}.log",
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
    )


def read_json_file(file_path: Path) -> dict:
    """Reads a JSON file and handles potential errors.

    Args:
        file_path (Path): The path to the JSON file.

    Returns:
        dict: Parsed JSON data if successful.
    """
    try:
        with Path.open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Error: The file '{file_path}' was not found.") from None
    except json.JSONDecodeError as e:
        raise Exception(f"Failed to decode JSON : '{file_path}'. Details: {e}") from e
    except Exception as e:
        raise Exception(f"Unexpected error while reading the file '{file_path}': {e}") from e
