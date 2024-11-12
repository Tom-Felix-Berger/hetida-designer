"""Documentation for to Wide Format

# to Wide Format

## Description
Uses the pandas pivot funtion to convert a long format MultiTsFrame to a wide format DataFrame.

## Inputs
* **mtsf_in_long_format** (Pandas DataFrame): The input DataFrame in the format of a MultiTSFrame: The DataFrame must contain the columns "timestamp", "metric", "value", and possibly more
(e.g. "longitude", "latitude"). The timestamp column should have dtype datetime64[ns, UTC], the "metric" column dtype string, and all other columns dtype float64.
* **handle_multiple_columns** (String):  Should be one of ["drop", "flatten", "hierarchical"]. Use this parameter to specify how to handle columns additional to "timestamp", "metric", and "value"
(e.g. "longitude", "latitude"). If this parameter is set to "drop", additional columns are ignored. Otherwise a MultiIndex is constructed. If "hierarchical", the MultiIndex is left hierarchical,
if "flatten" it's flattened two a one-level index using the flattening_delimiter (e.g., "value.a", "value.b", "value.c", "longitude.a", ...). Default: "drop".
* **flattening_delimiter** (String): The delimiter used to flatten the index if handle_multiple_columns is "flatten". Default: ".". 

## Outputs
* **mtsf_in_wide_format** (Pandas DataFrame): The output DataFrame in wide Format with one column for each metric if handle_multiple_columns = "drop" or multiple columns for each metric otherwise.

## Details
Converts a MultiTSFrame into a DataFrame with one timestamp column and another column for each unique value in the metric column of the original MultiTsFrame using the pandas function pivot.
Handles columns additional to "timestamp", "metric", and "value" (e.g. "longitude", "latitude") by either dropping them or constructing a MultiIndex. If there is a timestamp where not all metrics
have a value, the corresponding positions in the output DataFrame are filled with NaN. Raises a ComponentInputValidationException if the inputs are ill-formatted or conflicting.

"""

import pandas as pd
import numpy as np
from hdutils import parse_value  # noqa: E402
from hdutils import ComponentInputValidationException

# ***** DO NOT EDIT LINES BELOW *****
# These lines may be overwritten if component details or inputs/outputs change.
COMPONENT_INFO = {
    "inputs": {
        "mtsf_in_long_format": {"data_type": "MULTITSFRAME"},
        "handle_multiple_columns": {"data_type": "STRING", "default_value": "drop"},
        "flattening_delimiter": {"data_type": "STRING", "default_value": "."},
    },
    "outputs": {
        "mtsf_in_wide_format": {"data_type": "DATAFRAME"},
    },
    "name": "to Wide Format",
    "category": "Connectors",
    "description": "Uses the pandas pivot funtion to convert a long format MultiTsFrame to a wide format DataFrame.",  # noqa: E501
    "version_tag": "1.0.0",
    "id": "f175d452-7424-4744-bd8b-4fc82a619f25",
    "revision_group_id": "d2e1e78b-ff4a-4918-a104-345e673278dc",
    "state": "RELEASED",
    "released_timestamp": "2024-11-12T09:16:19.308269+00:00",
}

from hdutils import parse_default_value  # noqa: E402, F401


def main(
    *, mtsf_in_long_format, handle_multiple_columns="drop", flattening_delimiter="."
):
    # entrypoint function for this component
    # ***** DO NOT EDIT LINES ABOVE *****

    # "value" must be in the columns
    if "value" not in mtsf_in_long_format.columns:
        raise ComponentInputValidationException(
            'There is no column named "value" in the MultiTsFrame passed to mtsf_in_long_format, but there should be.',
            invalid_component_inputs=["mtsf_in_long_format"],
        )

    # handle_multiple_columns parameter must be in {"drop", "flatten", "hierarchical"}
    if handle_multiple_columns not in {"drop", "flatten", "hierarchical"}:
        raise ComponentInputValidationException(
            f'{handle_multiple_columns} is not a valid value for the handle_multiple_columns parameter. It should be set to "drop", "flatten", or "hierarchical" (default: "drop").',
            invalid_component_inputs=["handle_multiple_columns"],
        )

    # determine columns for the values of the pivoted dataframe
    if handle_multiple_columns == "drop":
        value_cols = "value"
    else:
        value_cols = [
            col
            for col in mtsf_in_long_format.columns
            if col not in ["timestamp", "metric"]
        ]

    # convert "metrics" column to dtype object instead of string, otherwise the column index of the resulting dataframe has dtype string, which is not standard
    mtsf_in_long_format["metric"] = mtsf_in_long_format["metric"].astype(object)

    # pivot mtsf putting the timestamp column as index and unique entries of the metric column as columns
    pivoted_df = mtsf_in_long_format.pivot(
        index="timestamp", columns="metric", values=value_cols
    ).reset_index()

    # flatten index if required
    if handle_multiple_columns == "flatten":
        pivoted_df.columns = [
            flattening_delimiter.join(col) if not col[0] == "timestamp" else "timestamp"
            for col in pivoted_df.columns.values
        ]

    # reset names of column index
    pivoted_df.columns.names = [None] * pivoted_df.columns.nlevels

    # sort dataframe and reset index
    pivoted_df = pivoted_df.sort_values(by="timestamp").reset_index(drop=True)

    return {"mtsf_in_wide_format": pivoted_df}


# Testing
try:  # This workaround is necessary since pytest might not be installed in the deploying systems
    import pytest
except ImportError:
    pass
else:
    # Fixtures
    @pytest.fixture
    def mtsf_in_long_format():
        long_mtsf = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2019-08-01T15:45:36.000Z",
                        "2019-08-01T15:45:36.000Z",
                        "2019-08-01T15:45:36.000Z",
                        "2019-08-01T15:48:36.000Z",
                        "2019-08-01T15:48:36.000Z",
                        "2019-08-01T15:51:36.000Z",
                        "2019-08-01T15:51:36.000Z",
                    ],
                    format="%Y-%m-%dT%H:%M:%S.%fZ",
                ).tz_localize("UTC"),
                "metric": pd.Series(
                    ["a", "b", "c", "a", "b", "a", "c"], dtype="string"
                ),
                "value": [1.1, 10.0, -3.5, 1.2, 20.0, 1.3, -5.0],
                "longitude": [111.111111] * 7,
                "latitude": [11.111111] * 7,
            }
        )

        return long_mtsf

    @pytest.fixture
    def mtsf_in_wide_format():
        timestamp = pd.to_datetime(
            [
                "2019-08-01T15:45:36.000Z",
                "2019-08-01T15:48:36.000Z",
                "2019-08-01T15:51:36.000Z",
            ],
            format="%Y-%m-%dT%H:%M:%S.%fZ",
        ).tz_localize("UTC")
        value_a = [1.1, 1.2, 1.3]
        value_b = [10.0, 20.0, np.nan]
        value_c = [-3.5, np.nan, -5.0]
        longitude_a = [111.111111] * 3
        longitude_b = [111.111111, 111.111111, np.nan]
        longitude_c = [111.111111, np.nan, 111.111111]
        latitude_a = [11.111111] * 3
        latitude_b = [11.111111, 11.111111, np.nan]
        latitude_c = [11.111111, np.nan, 11.111111]

        mtsf_drop = pd.DataFrame(
            {
                "timestamp": timestamp,
                "a": value_a,
                "b": value_b,
                "c": value_c,
            }
        )

        mtsf_flatten = pd.DataFrame(
            {
                "timestamp": timestamp,
                "value.a": value_a,
                "value.b": value_b,
                "value.c": value_c,
                "longitude.a": longitude_a,
                "longitude.b": longitude_b,
                "longitude.c": longitude_c,
                "latitude.a": latitude_a,
                "latitude.b": latitude_b,
                "latitude.c": latitude_c,
            }
        )

        mtsf_hierarchical = pd.DataFrame(
            {
                ("timestamp", ""): timestamp,
                ("value", "a"): value_a,
                ("value", "b"): value_b,
                ("value", "c"): value_c,
                ("longitude", "a"): longitude_a,
                ("longitude", "b"): longitude_b,
                ("longitude", "c"): longitude_c,
                ("latitude", "a"): latitude_a,
                ("latitude", "b"): latitude_b,
                ("latitude", "c"): latitude_c,
            }
        )

        return {
            "drop": mtsf_drop,
            "flatten": mtsf_flatten,
            "hierarchical": mtsf_hierarchical,
        }

    def test_run_from_test_wiring(mtsf_in_wide_format):
        kwargs = {
            inp_wiring["workflow_input_name"]: parse_value(
                inp_wiring["filters"]["value"],
                COMPONENT_INFO["inputs"][inp_wiring["workflow_input_name"]][
                    "data_type"
                ],
                nullable=True,
            )
            for inp_wiring in TEST_WIRING_FROM_PY_FILE_IMPORT["input_wirings"]
            if inp_wiring["adapter_id"] == "direct_provisioning"
        }

        assert main(**kwargs)["mtsf_in_wide_format"].equals(
            mtsf_in_wide_format[kwargs["handle_multiple_columns"]]
        )

    def test_run_with_multicols(mtsf_in_long_format, mtsf_in_wide_format):
        for multcols in ["drop", "flatten", "hierarchical"]:
            output_mtsf = main(
                mtsf_in_long_format=mtsf_in_long_format,
                handle_multiple_columns=multcols,
                flattening_delimiter=".",
            )["mtsf_in_wide_format"]

            assert output_mtsf.equals(mtsf_in_wide_format[multcols])

    def test_run_with_invalid_mtsf_in_long_format(mtsf_in_long_format):
        with pytest.raises(ComponentInputValidationException):
            invalid_mtsf = mtsf_in_long_format.rename(columns={"value": "not_value"})
            main(mtsf_in_long_format=invalid_mtsf)

    def test_run_with_invalid_handle_multiple_columns(mtsf_in_long_format):
        with pytest.raises(ComponentInputValidationException):
            main(
                mtsf_in_long_format=mtsf_in_long_format,
                handle_multiple_columns="invalid",
            )


TEST_WIRING_FROM_PY_FILE_IMPORT = {
    "input_wirings": [
        {
            "workflow_input_name": "mtsf_in_long_format",
            "adapter_id": "direct_provisioning",
            "filters": {
                "value": '{\n    "timestamp": [\n        "2019-08-01T15:45:36.000Z",\n        "2019-08-01T15:45:36.000Z",\n        "2019-08-01T15:45:36.000Z",\n        "2019-08-01T15:48:36.000Z",\n        "2019-08-01T15:48:36.000Z",\n        "2019-08-01T15:51:36.000Z",\n        "2019-08-01T15:51:36.000Z"\n    ],\n    "value": [\n        1.1,\n        10,\n        -3.5,\n        1.2,\n        20,\n        1.3,\n        -5\n    ],\n    "metric": [\n        "a",\n        "b",\n        "c",\n        "a",\n        "b",\n        "a",\n        "c"\n    ],\n    "longitude": [\n        "111.111111",\n        "111.111111",\n        "111.111111",\n        "111.111111",\n        "111.111111",\n        "111.111111",\n        "111.111111"\n    ],\n        "latitude": [\n        "11.111111",\n        "11.111111",\n        "11.111111",\n        "11.111111",\n        "11.111111",\n        "11.111111",\n        "11.111111"\n    ]\n}'
            },
        },
        {
            "workflow_input_name": "handle_multiple_columns",
            "adapter_id": "direct_provisioning",
            "use_default_value": True,
            "filters": {"value": "drop"},
        },
        {
            "workflow_input_name": "flattening_delimiter",
            "adapter_id": "direct_provisioning",
            "use_default_value": True,
            "filters": {"value": "."},
        },
    ]
}

