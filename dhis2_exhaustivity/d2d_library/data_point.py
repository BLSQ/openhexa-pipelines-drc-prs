class DataPoint:
    """Helper class definition to store/create the correct DataElement JSON format for exhaustivity push."""

    def __init__(self, row: dict):
        """Create a new data point instance.

        Parameters
        ----------
        row : dict
            Dictionary with keys: ['DX_UID', 'PERIOD', 'ORG_UNIT', 'VALUE']
        """
        self.dataElement = row.get("DX_UID")
        self.period = row.get("PERIOD")
        self.orgUnit = row.get("ORG_UNIT")
        self.value = row.get("VALUE")

    def to_json(self) -> dict:
        """Return a dictionary representation of the data point suitable for DHIS2 JSON format.

        Returns
        -------
        dict
            A dictionary with keys corresponding to DHIS2 data value fields.
        """
        return {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "value": self.value,
        }

    def to_delete_json(self) -> dict:
        """Return a dictionary for deletion in DHIS2 JSON format.

        Returns
        -------
        dict
            A dictionary with value set to empty string for deletion.
        """
        return {
            "dataElement": self.dataElement,
            "period": self.period,
            "orgUnit": self.orgUnit,
            "value": "",
            "comment": "deleted value",
        }

    def _check_attributes(self, exclude_value: bool = False) -> bool:
        """Check if mandatory attributes are not None."""
        attributes = [self.dataElement, self.period, self.orgUnit]
        if not exclude_value:
            attributes.append(self.value)
        return all(attr is not None for attr in attributes)

    def is_valid(self) -> bool:
        """Check if mandatory attributes are valid (not None)."""
        return self._check_attributes(exclude_value=False)

    def is_to_delete(self) -> bool:
        """Check if the data point is marked for deletion (value is None)."""
        return self._check_attributes(exclude_value=True) and self.value is None

    def __str__(self) -> str:
        return f"DataPoint(dx:{self.dataElement} pe:{self.period} ou:{self.orgUnit} val:{self.value})"
