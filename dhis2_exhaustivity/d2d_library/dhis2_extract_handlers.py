import tempfile
from pathlib import Path

import polars as pl
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2


class AnanalyticsDataElementExtractor:
    """Handles downloading and formatting of data elements from DHIS2."""

    def __init__(self, extractor: "DHIS2Extractor"):
        self.extractor = extractor

    def download_period(
        self, data_elements: list[str], org_units: list[str], period: str, output_dir: Path, filename: str | None = None
    ) -> Path | None:
        """Hacky solution due to the old version from where we're retrieving data elements.

        NOTE: VERSION under 2.39 should not be supported (!).

        Parameters
        ----------
        data_elements : list[str]
            List of DHIS2 data element UIDs to extract.
        org_units : list[str]
            List of DHIS2 organization unit UIDs to extract data for.
        period : str
            DHIS2 period (valid format) to extract data for.
        output_dir : Path
            Directory where extracted data files will be saved.
        filename : str | None
            Optional filename for the extracted data file. If None, a default name will be used.

        Returns
        -------
        Path | None
            The path to the extracted data file, or None if no data was extracted.

        Raises
        ------
        Exception
            If an error occurs during the extract process.
        """
        try:
            current_run.log_info(f"Retrieving data elements extract from analytics for period : {period}")
            return self.extractor._handle_extract_for_period(
                handler=self,
                data_products=data_elements,
                org_units=org_units,
                period=period,
                output_dir=output_dir,
                filename=filename,
            )
        except Exception as e:
            raise Exception(f"Extract data elements download error : {e}") from e

    def _retrieve_data(self, data_elements: list[str], org_units: list[str], period: str) -> pl.DataFrame:
        if not self.extractor._valid_dhis2_period_format(period):
            raise ValueError(f"Invalid DHIS2 period format: {period}")
        try:
            current_run.log_info(f"Requesting analytics data: {len(data_elements)} data elements, {len(org_units)} org units, period {period}")
            response = self.extractor.dhis2_client.analytics.get(
                data_elements=data_elements,
                periods=[period],
                org_units=org_units,
                include_cocs=True,
            )
            current_run.log_info(f"Analytics API returned {len(response) if isinstance(response, list) else 'non-list'} items")
        except Exception as e:
            raise Exception(f"Error retrieving data elements data: {e}") from e

        df_response = pl.DataFrame(response)
        current_run.log_info(f"Converted to DataFrame: {len(df_response)} rows")
        return self._map_to_dhis2_format_analytics(df_response)

    def _map_to_dhis2_format_analytics(
        self,
        data: pl.DataFrame,
    ) -> pl.DataFrame:
        # Return empty DataFrame with correct schema instead of None
        empty_schema = {
            "DATA_TYPE": pl.Utf8,
            "DX_UID": pl.Utf8,
            "PERIOD": pl.Utf8,
            "ORG_UNIT": pl.Utf8,
            "CATEGORY_OPTION_COMBO": pl.Utf8,
            "ATTRIBUTE_OPTION_COMBO": pl.Utf8,
            "RATE_TYPE": pl.Utf8,
            "DOMAIN_TYPE": pl.Utf8,
            "VALUE": pl.Utf8,
        }
        
        if data.is_empty():
            return pl.DataFrame(schema=empty_schema)

        try:
            # Use select() and with_columns() instead of passing Expr to DataFrame constructor
            data_format = data.select([
                pl.col("dx").alias("DX_UID"),
                pl.col("pe").alias("PERIOD"),
                pl.col("ou").alias("ORG_UNIT"),
                pl.col("co").alias("CATEGORY_OPTION_COMBO"),
                pl.col("value").alias("VALUE"),
            ]).with_columns([
                pl.lit("AGGREGATED").alias("DOMAIN_TYPE"),
                pl.lit("DATA_ELEMENT").alias("DATA_TYPE"),
                pl.lit(None, dtype=pl.Utf8).alias("ATTRIBUTE_OPTION_COMBO"),
                pl.lit(None, dtype=pl.Utf8).alias("RATE_TYPE"),
            ])
            return data_format

        except Exception as e:
            raise Exception(f"Unexpected Error while creating extract format table: {e}") from e


class DataElementsExtractor:
    """Handles downloading and formatting of data elements from DHIS2."""

    def __init__(self, extractor: "DHIS2Extractor"):
        self.extractor = extractor

    def download_period(
        self, data_elements: list[str], org_units: list[str], period: str, output_dir: Path, filename: str | None = None
    ) -> Path | None:
        """Download and handle data extracts for the specified period, saving them to the output directory.

        Parameters
        ----------
        data_elements : list[str]
            List of DHIS2 data element UIDs to extract.
        org_units : list[str]
            List of DHIS2 organization unit UIDs to extract data for.
        period : str
            DHIS2 period (valid format) to extract data for.
        output_dir : Path
            Directory where extracted data files will be saved.
        filename : str | None
            Optional filename for the extracted data file. If None, a default name will be used.

        Returns
        -------
        Path | None
            The path to the extracted data file, or None if no data was extracted.

        Raises
        ------
        Exception
            If an error occurs during the extract process.
        """
        try:
            current_run.log_info(f"Retrieving data elements extract for period : {period}")
            return self.extractor._handle_extract_for_period(
                handler=self,
                data_products=data_elements,
                org_units=org_units,
                period=period,
                output_dir=output_dir,
                filename=filename,
            )
        except Exception as e:
            raise Exception(f"Extract data elements download error : {e}") from e

    def _retrieve_data(self, data_elements: list[str], org_units: list[str], period: str) -> pl.DataFrame:
        if not self.extractor._valid_dhis2_period_format(period):
            raise ValueError(f"Invalid DHIS2 period format: {period}")
        try:
            response = self.extractor.dhis2_client.data_value_sets.get(
                data_elements=data_elements,
                periods=[period],
                org_units=org_units,
                last_updated=None,  # not implemented yet
            )
        except Exception as e:
            raise Exception(f"Error retrieving data elements data: {e}") from e

        return self.extractor._map_to_dhis2_format(pl.DataFrame(response), data_type="DATA_ELEMENT")


class IndicatorsExtractor:
    """Handles downloading and formatting of indicators from DHIS2."""

    def __init__(self, extractor: "DHIS2Extractor"):
        self.extractor = extractor

    def download_period(
        self, indicators: list[str], org_units: list[str], period: str, output_dir: Path, filename: str | None = None
    ) -> Path | None:
        """Download and handle data extracts for the specified periods, saving them to the output directory.

        Parameters
        ----------
        indicators : list[str]
            List of DHIS2 indicators UIDs to extract.
        org_units : list[str]
            List of DHIS2 organization unit UIDs to extract data for.
        period : str
            DHIS2 period (valid format) to extract data for.
        output_dir : Path
            Directory where extracted data files will be saved.
        filename : str | None
            Optional filename for the extracted data file. If None, a default name will be used.

        Returns
        -------
        Path | None
            The path to the extracted data file, or None if no data was extracted.

        Raises
        ------
        Exception
            If an error occurs during the extract process.
        """
        try:
            current_run.log_info(f"Retrieving indicators extract for period : {period}")
            return self.extractor._handle_extract_for_period(
                handler=self,
                indicators=indicators,
                org_units=org_units,
                period=period,
                output_dir=output_dir,
                filename=filename,
            )
        except Exception as e:
            raise Exception(f"Extract indicators download error : {e}") from e

    def _retrieve_data(self, indicators: list[str], org_units: list[str], period: str) -> pl.DataFrame:
        if not self.extractor._valid_dhis2_period_format(period):
            raise ValueError(f"Invalid DHIS2 period format: {period}")
        try:
            response = self.extractor.dhis2_client.analytics.get(
                indicators=indicators,
                periods=[period],
                org_units=org_units,
                include_cocs=False,
            )
        except Exception as e:
            raise Exception(f"Error retrieving indicators data: {e}") from e

        raw_data_formatted = pl.DataFrame(response).rename({"pe": "period", "ou": "orgUnit"})
        return self.extractor._map_to_dhis2_format(raw_data_formatted, data_type="INDICATOR")


class ReportingRatesExtractor:
    """Handles downloading and formatting of reporting rates from DHIS2."""

    def __init__(self, extractor: "DHIS2Extractor"):
        self.extractor = extractor

    def download_period(
        self,
        reporting_rates: list[str],
        org_units: list[str],
        period: str,
        output_dir: Path,
        filename: str | None = None,
    ) -> Path | None:
        """Download and handle data extracts for the specified periods, saving them to the output directory.

        Parameters
        ----------
        reporting_rates : list[str]
            List of DHIS2 reporting rates UIDs.RATE to extract.
        org_units : list[str]
            List of DHIS2 organization unit UIDs to extract data for.
        period : str
            DHIS2 period (valid format) to extract data for.
        output_dir : Path
            Directory where extracted data files will be saved.
        filename : str | None
            Optional filename for the extracted data file. If None, a default name will be used.

        Returns
        -------
        Path | None
            The path to the extracted data file, or None if no data was extracted.

        Raises
        ------
        Exception
            If an error occurs during the extract process.
        """
        try:
            current_run.log_info(f"Retrieving reporting rates extract for period : {period}")
            return self.extractor._handle_extract_for_period(
                handler=self,
                data_products=reporting_rates,
                org_units=org_units,
                period=period,
                output_dir=output_dir,
                filename=filename,
            )
        except Exception as e:
            raise Exception(f"Extract reporting rates download error : {e}") from e

    def _retrieve_data(self, reporting_rates: list[str], org_units: list[str], period: str) -> pl.DataFrame:
        if not self.extractor._valid_dhis2_period_format(period):
            raise ValueError(f"Invalid DHIS2 period format: {period}")
        try:
            response = self.extractor.dhis2_client.analytics.get(
                data_elements=reporting_rates,
                periods=[period],
                org_units=org_units,
                include_cocs=False,
            )
        except Exception as e:
            raise Exception(f"Error retrieving reporting rates data: {e}") from e

        raw_data_formatted = pl.DataFrame(response).rename({"pe": "period", "ou": "orgUnit"})
        return self.extractor._map_to_dhis2_format(raw_data_formatted, data_type="REPORTING_RATE")


class DHIS2Extractor:
    """Extracts data from DHIS2 using various handlers for data elements, indicators, and reporting rates.

    Attributes
    ----------
    client : object
        The DHIS2 client used for data extraction.
    queue : object | None
        Optional queue for managing extracted files.
    download_mode : str
        Mode for downloading files ("DOWNLOAD_REPLACE" or "DOWNLOAD_NEW").
    last_updated : None
        Placeholder for future use.
    return_existing_file : bool
        When DOWNLOAD_NEW mode is used:
            True: returns the path to existing files.
            False: returns None if the file already exists.
        Default is False.

    Handlers
    --------
    data_elements : DataElementsExtractor
        Handler for extracting data elements.
    indicators : IndicatorsExtractor
        Handler for extracting indicators.
    reporting_rates : ReportingRatesExtractor
        Handler for extracting reporting rates.
    """

    def __init__(
        self, dhis2_client: DHIS2, download_mode: str = "DOWNLOAD_REPLACE", return_existing_file: bool = False
    ):
        self.dhis2_client = dhis2_client
        if download_mode not in {"DOWNLOAD_REPLACE", "DOWNLOAD_NEW"}:
            raise ValueError("Invalid 'download_mode', use 'DOWNLOAD_REPLACE' or 'DOWNLOAD_NEW'.")
        self.download_mode = download_mode
        self.last_updated = None  # NOTE: Placeholder for future use
        self.data_elements = DataElementsExtractor(self)
        self.indicators = IndicatorsExtractor(self)
        self.reporting_rates = ReportingRatesExtractor(self)
        self.analytics_data_elements = AnanalyticsDataElementExtractor(self)
        self.return_existing_file = return_existing_file

    def _handle_extract_for_period(
        self,
        handler: DataElementsExtractor | IndicatorsExtractor | ReportingRatesExtractor | AnanalyticsDataElementExtractor,
        data_products: list[str],
        org_units: list[str],
        period: str,
        output_dir: Path,
        filename: str | None = None,
    ) -> Path | None:
        output_dir.mkdir(parents=True, exist_ok=True)
        if filename:
            extract_fname = output_dir / filename
        else:
            extract_fname = output_dir / f"data_{period}.parquet"

        # Skip if already exists and mode is DOWNLOAD_NEW
        if self.download_mode == "DOWNLOAD_NEW" and extract_fname.exists():
            current_run.log_info(f"Extract for period {period} already exists, download skipped.")
            return extract_fname if self.return_existing_file else None

        raw_data = handler._retrieve_data(data_products, org_units, period)

        if raw_data is None:
            current_run.log_info(f"Nothing to save for period {period}.")
            return None

        # Check if DataFrame is empty
        if raw_data.is_empty():
            current_run.log_info(f"Empty DataFrame retrieved for period {period}. Saving empty file with schema.")
            # Still save empty DataFrame to have a trace file

        if extract_fname.exists():
            current_run.log_info(f"Replacing extract for period {period}.")

        self.save_to_parquet(raw_data, extract_fname)
        current_run.log_info(f"Saved extract file for period {period}: {extract_fname} ({len(raw_data)} rows)")
        return extract_fname

    def _map_to_dhis2_format(
        self,
        dhis_data: pl.DataFrame,
        data_type: str = "DATA_ELEMENT",
        domain_type: str = "AGGREGATED",
    ) -> pl.DataFrame:
        """Maps DHIS2 data to a standardized data extraction table.

        Parameters
        ----------
        dhis_data : pl.DataFrame
            Input DataFrame containing DHIS2 data. Must include columns like `period`, `orgUnit`,
            `categoryOptionCombo(DATA_ELEMENT)`, `attributeOptionCombo(DATA_ELEMENT)`, `dataElement`
            and `value` based on the data type.
        data_type : str
            The type of data being mapped. Supported values are:
            - "DATA_ELEMENT": Includes `categoryOptionCombo` and maps `dataElement` to `dx_uid`.
            - "INDICATOR": Maps `dx` to `dx_uid`.
            - "REPORTING_RATE": Maps `dx` to `dx_uid` and `rate_type` by split the string by `.`.
            Default is "DATA_ELEMENT".
        domain_type : str, optional
            The domain of the data if its per period (Agg ex: monthly) or datapoint (Tracker ex: per day):
            - "AGGREGATED": For aggregated data (default).
            - "TRACKER": For tracker data.

        Returns
        -------
        pl.DataFrame
            A DataFrame formatted to SNIS standards, with the following columns:
            - "DATA_TYPE": The type of data (DATA_ELEMENT, REPORTING_RATE, or INDICATOR).
            - "DX_UID": Data element, dataset, or indicator UID.
            - "PERIOD": Reporting period.
            - "ORGUNIT": Organization unit.
            - "CATEGORYOPTIONCOMBO": (Only for DATA_ELEMENT) Category option combo UID.
            - "RATE_TYPE": (Only for REPORTING_RATE) Rate type.
            - "DOMAIN_TYPE": Data domain (AGGREGATED or TRACKER).
            - "VALUE": Data value.
        """
        # Return empty DataFrame with correct schema instead of None
        empty_schema = {
            "DATA_TYPE": pl.Utf8,
            "DX_UID": pl.Utf8,
            "PERIOD": pl.Utf8,
            "ORG_UNIT": pl.Utf8,
            "CATEGORY_OPTION_COMBO": pl.Utf8,
            "ATTRIBUTE_OPTION_COMBO": pl.Utf8,
            "RATE_TYPE": pl.Utf8,
            "DOMAIN_TYPE": pl.Utf8,
            "VALUE": pl.Utf8,
        }
        
        if dhis_data.is_empty():
            return pl.DataFrame(schema=empty_schema)

        if data_type not in ["DATA_ELEMENT", "REPORTING_RATE", "INDICATOR"]:
            raise ValueError("Incorrect 'data_type' configuration ('DATA_ELEMENT', 'REPORTING_RATE', 'INDICATOR').")

        try:
            # Use select() and with_columns() instead of passing Expr to DataFrame constructor
            if data_type == "DATA_ELEMENT":
                data_format = dhis_data.select([
                    pl.col("period").alias("PERIOD"),
                    pl.col("orgUnit").alias("ORG_UNIT"),
                    pl.col("value").alias("VALUE"),
                    pl.col("dataElement").alias("DX_UID"),
                    pl.col("categoryOptionCombo").alias("CATEGORY_OPTION_COMBO"),
                    pl.col("attributeOptionCombo").alias("ATTRIBUTE_OPTION_COMBO"),
                ]).with_columns([
                    pl.lit(domain_type).alias("DOMAIN_TYPE"),
                    pl.lit(data_type).alias("DATA_TYPE"),
                    pl.lit(None, dtype=pl.Utf8).alias("RATE_TYPE"),
                ])
            elif data_type == "REPORTING_RATE":
                # Split dx column by "." to get DX_UID and RATE_TYPE
                data_format = dhis_data.select([
                    pl.col("period").alias("PERIOD"),
                    pl.col("orgUnit").alias("ORG_UNIT"),
                    pl.col("value").alias("VALUE"),
                    pl.col("dx").str.split(".", inclusive=False).list.get(0).alias("DX_UID"),
                    pl.col("dx").str.split(".", inclusive=False).list.get(1).alias("RATE_TYPE"),
                ]).with_columns([
                    pl.lit(domain_type).alias("DOMAIN_TYPE"),
                    pl.lit(data_type).alias("DATA_TYPE"),
                    pl.lit(None, dtype=pl.Utf8).alias("CATEGORY_OPTION_COMBO"),
                    pl.lit(None, dtype=pl.Utf8).alias("ATTRIBUTE_OPTION_COMBO"),
                ])
            elif data_type == "INDICATOR":
                data_format = dhis_data.select([
                    pl.col("period").alias("PERIOD"),
                    pl.col("orgUnit").alias("ORG_UNIT"),
                    pl.col("value").alias("VALUE"),
                    pl.col("dx").alias("DX_UID"),
                ]).with_columns([
                    pl.lit(domain_type).alias("DOMAIN_TYPE"),
                    pl.lit(data_type).alias("DATA_TYPE"),
                    pl.lit(None, dtype=pl.Utf8).alias("CATEGORY_OPTION_COMBO"),
                    pl.lit(None, dtype=pl.Utf8).alias("ATTRIBUTE_OPTION_COMBO"),
                    pl.lit(None, dtype=pl.Utf8).alias("RATE_TYPE"),
                ])
            return data_format

        except Exception as e:
            raise Exception(f"Unexpected Error while creating extract format table: {e}") from e

    def _valid_dhis2_period_format(self, dhis2_period: str) -> bool:
        """Validate if the given period string is in a valid DHIS2 format.

        Returns
        -------
        bool
        True if valid, False otherwise.
        """
        # TODO: Expand this function to cover more DHIS2 period formats as needed
        return True

    @staticmethod
    def save_to_parquet(data: pl.DataFrame, filename: Path) -> None:
        """Safely saves a DataFrame to a Parquet file using a temporary file and atomic replace.

        Args:
            data (pl.DataFrame): The DataFrame to save.
            filename (Path): The path where the Parquet file will be saved.
        """
        try:
            if not isinstance(data, pl.DataFrame):
                raise TypeError("The 'data' parameter must be a polars DataFrame.")

            # Write to a temporary file in the same directory
            with tempfile.NamedTemporaryFile(suffix=".parquet", dir=filename.parent, delete=False) as tmp_file:
                temp_filename = Path(tmp_file.name)
                data.write_parquet(temp_filename)

            # Atomically replace the old file with the new one
            temp_filename.replace(filename)

        except Exception as e:
            # Clean up the temp file if it exists
            if "temp_filename" in locals() and temp_filename.exists():
                temp_filename.unlink()
            raise RuntimeError(f"Failed to save parquet file to {filename}") from e
