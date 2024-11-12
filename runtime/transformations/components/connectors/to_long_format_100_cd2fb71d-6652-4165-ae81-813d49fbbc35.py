"""Documentation for to Long Format

# to Long Format

## Description
Uses the pandas melt function to convert a wide format DataFrame to a long format MultiTsFrame.

## Inputs
* **mtsf_in_wide_format** (Pandas DataFrame): The input DataFrame which should consist of a timestamp column with dtype datetime64[ns,UTC] and for each metric
one column (e.g., "a", "b", "c") or multiple columns (e.g., "value.a", "value.b", "value.c", "longitude.a", "longitude.b", "longitude.c", ...) with dtype float64.
* **multiple_columns_from** (String): This parameter corresponds to the "handle_multiple_columns" parameter from the to Wide Format component and should be in 
{"drop", "flatten", "hierarchical"}. Use this parameter to specify if the passed DataFrame has only one column for each metric (in this case use "drop") or multiple,
and if it has multiple, whether the column index is flattened ("timestamp", "value.a", ...) or hierarchical (MultiIndex([("timestamp", ""), ("value", "a"), ...]). Default: "drop".
* **flattening_delimiter** (String): In case mtsf_in_wide_format has a flattened column index, specify the delimiter (e.g., ".") used for flattening. Default: ".".

## Outputs
* **mtsf_in_long_format** (Pandas DataFrame): The output MultiTsFrame constructed from the DataFrame.

## Details
Uses the pandas melt function to convert a wide format DataFrame to a long format MultiTsFrame, if required with multiple columns such as "timestamp", "metric", "value",
"longitude", "latitude". Raises a ComponentInputValidationException if the inputs are ill-formatted or conflicting.

"""

import pandas as pd
import numpy as np
from hdutils import parse_value  # noqa: E402
from hdutils import ComponentInputValidationException

# ***** DO NOT EDIT LINES BELOW *****
# These lines may be overwritten if component details or inputs/outputs change.
COMPONENT_INFO = {
    "inputs": {
        "mtsf_in_wide_format": {"data_type": "DATAFRAME"},
        "multiple_columns_from": {"data_type": "STRING", "default_value": "drop"},
        "flattening_delimiter": {"data_type": "STRING", "default_value": "."},
    },
    "outputs": {
        "mtsf_in_long_format": {"data_type": "DATAFRAME"},
    },
    "name": "to Long Format",
    "category": "Connectors",
    "description": "Uses the pandas melt function to convert a wide format DataFrame to a long format MultiTsFrame.",  # noqa: E501
    "version_tag": "1.0.0",
    "id": "cd2fb71d-6652-4165-ae81-813d49fbbc35",
    "revision_group_id": "8178c575-a23a-4a4d-a524-8a108764b822",
    "state": "RELEASED",
    "released_timestamp": "2024-11-12T09:16:15.976290+00:00",
}

from hdutils import parse_default_value  # noqa: E402, F401


def main(
    *, mtsf_in_wide_format, multiple_columns_from="drop", flattening_delimiter="."
):
    # entrypoint function for this component
    # ***** DO NOT EDIT LINES ABOVE *****

    # in order to not change the original input dataframe
    mtsf_in_wide_format = mtsf_in_wide_format.copy()

    # check whether mtsf_in_wide_format has "timestamp" column that is appropriate for mtsf
    if not "timestamp" in mtsf_in_wide_format.columns:
        raise ComponentInputValidationException(
            'There is no column "timestamp" in the DataFrame passed to mtsf_in_wide_format, but there should be.',
            invalid_component_inputs=["mtsf_in_wide_format"],
        )

    if mtsf_in_wide_format["timestamp"].isna().any():
        raise ComponentInputValidationException(
            'No missing values are allowed in the column "timestamp" of a MulitTSFrame.',
            invalid_component_inputs=["mtsf_in_wide_format"],
        )

    if not mtsf_in_wide_format["timestamp"].dtype == "datetime64[ns, UTC]":
        raise ComponentInputValidationException(
            f'{mtsf_in_wide_format["timestamp"].dtype} is an inappropriate dtype for the "timestamp"'
            + " column of a MultiTSFrame. It should be datetime64[ns, UTC] instead.",
            invalid_component_inputs=["mtsf_in_wide_format"],
        )

    # multiple_columns_from parameter must be in ["drop", "flatten", "hierarchical"]
    if multiple_columns_from not in ["drop", "flatten", "hierarchical"]:
        raise ComponentInputValidationException(
            f'"{multiple_columns_from}" is not a valid value for the handle_multiple_columns parameter. It should be set to "drop", "flatten", or "hierarchical" (default: "drop").',
            invalid_component_inputs=["multiple_columns_from"],
        )

    # columns must have the appropriate number of levels for multiple_columns_from parameter
    appr_levels = {"drop": 1, "flatten": 1, "hierarchical": 2}
    if not (a := appr_levels[multiple_columns_from]) == (
        n := mtsf_in_wide_format.columns.nlevels
    ):
        raise ComponentInputValidationException(
            (
                f"multiple_columns_from = {multiple_columns_from} requires the column index"
                + f"of mtsf_in_wide_format to have {a} levels, but it has {n}."
            ),
            invalid_component_inputs=["mtsf_in_wide_format", "multiple_columns_from"],
        )

    # check if dtypes of all columns except timestamp are float64
    level = None if not multiple_columns_from == "hierarchical" else 0
    other_dtypes = set(
        mtsf_in_wide_format.drop("timestamp", axis=1, level=level).dtypes.values
    )
    if not other_dtypes == {np.dtype("float64")}:
        raise ComponentInputValidationException(
            f'All columns except "timestamp" should be of dtype float64. {other_dtypes}',
            invalid_component_inputs=["mtsf_in_wide_format"],
        )

    # melt mtsf_in_wide_format according to multiple_columns_from
    if multiple_columns_from == "drop":
        melted_mtsf = mtsf_in_wide_format.melt(
            id_vars="timestamp",
            var_name="metric",
            value_name="value",
        )
        melted_mtsf = melted_mtsf.dropna()
        melted_mtsf = melted_mtsf.sort_values(by=["timestamp", "metric"]).reset_index(
            drop=True
        )
    else:
        # flatten column index if it is hierarchical
        if multiple_columns_from == "hierarchical":
            mtsf_in_wide_format.columns = [
                flattening_delimiter.join(col)
                if not col[0] == "timestamp"
                else "timestamp"
                for col in mtsf_in_wide_format.columns
            ]

        # find out which columns (except "timestamp" and "metric") the new dataframe should consist of
        seen = set()
        new_cols = [
            val
            for col in mtsf_in_wide_format.columns
            if not col == "timestamp"
            and (val := col.split(flattening_delimiter, maxsplit=1)[0]) not in seen
            and not seen.add(val)
        ]

        # for each of the new columns melt a df
        melted_list = []
        for col in new_cols:
            melted_df = pd.melt(
                mtsf_in_wide_format,
                id_vars=["timestamp"],
                value_vars=[
                    old_col
                    for old_col in mtsf_in_wide_format.columns
                    if col == old_col.split(flattening_delimiter)[0]
                ],
                var_name="metric",
                value_name=col,
            )

            melted_df["metric"] = (
                melted_df["metric"].str.split(flattening_delimiter).str[1]
            )

            # drop rows with missing values and add melted df to list
            melted_list.append(melted_df.dropna())

        # merge all melted dfs on timestamp and metric
        melted_mtsf = melted_list.pop(0)
        for m in melted_list:
            melted_mtsf = pd.merge(melted_mtsf, m, on=["timestamp", "metric"])

    # convert "metrics" column to dtype string, because it is constructed from the object type column index
    melted_mtsf["metric"] = melted_mtsf["metric"].astype("string")

    # sort and drop index
    melted_mtsf = melted_mtsf.sort_values(by=["timestamp", "metric"]).reset_index(
        drop=True
    )

    return {"mtsf_in_long_format": melted_mtsf}


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

    def test_run_from_test_wiring(mtsf_in_long_format):
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
        output_mtsf = main(**kwargs)["mtsf_in_long_format"]
        control_mtsf = mtsf_in_long_format.drop(["longitude", "latitude"], axis=1)

        assert output_mtsf.equals(control_mtsf)

    def test_run_with_multicols(mtsf_in_long_format, mtsf_in_wide_format):
        for multcols in ["drop", "flatten", "hierarchical"]:
            output_mtsf = main(
                mtsf_in_wide_format=mtsf_in_wide_format[multcols],
                multiple_columns_from=multcols,
                flattening_delimiter=".",
            )["mtsf_in_long_format"]

            control_mtsf = mtsf_in_long_format
            if multcols == "drop":
                control_mtsf = control_mtsf.drop(["longitude", "latitude"], axis=1)

            assert output_mtsf.equals(control_mtsf)

    def test_run_with_invalid_timestamp_column_name(mtsf_in_wide_format):
        input_mtsf = mtsf_in_wide_format["drop"].rename(
            columns={"timestamp": "not_timestamp"}
        )

        with pytest.raises(ComponentInputValidationException):
            main(mtsf_in_wide_format=input_mtsf)

    def test_run_with_invalid_timestamp_column_format(mtsf_in_wide_format):
        input_mtsf = mtsf_in_wide_format["drop"]
        input_mtsf["timestamp"] = input_mtsf["timestamp"].astype("string")
        input_mtsf.loc[0, "timestamp"] = "Not a datetime value"

        with pytest.raises(ComponentInputValidationException):
            main(mtsf_in_wide_format=input_mtsf)

    def test_run_with_nat_value_in_timestamp_column(mtsf_in_wide_format):
        input_mtsf = mtsf_in_wide_format["drop"]
        input_mtsf.loc[0, "timestamp"] = pd.NaT

        with pytest.raises(ComponentInputValidationException):
            main(mtsf_in_wide_format=input_mtsf)

    def test_run_with_invalid_multiple_columns_from(mtsf_in_wide_format):
        with pytest.raises(ComponentInputValidationException):
            main(
                mtsf_in_wide_format=mtsf_in_wide_format["drop"],
                multiple_columns_from="invalid_parameter_value",
            )

    def test_run_with_invalid_column_index_levels(mtsf_in_wide_format):
        parameter_index_levels_invalid = {1: "hierarchical", 2: "flatten"}
        parameter_index_levels_valid = {1: "flatten", 2: "hierarchical"}
        for levels in [1, 2]:
            with pytest.raises(ComponentInputValidationException):
                main(
                    mtsf_in_wide_format=mtsf_in_wide_format[
                        parameter_index_levels_invalid[levels]
                    ],
                    multiple_columns_from=parameter_index_levels_valid[levels],
                )


TEST_WIRING_FROM_PY_FILE_IMPORT = {
    "input_wirings": [
        {
            "workflow_input_name": "mtsf_in_wide_format",
            "adapter_id": "direct_provisioning",
            "filters": {
                "value": '{\n    "timestamp": [\n        "2019-08-01T15:45:36.000Z",\n        "2019-08-01T15:48:36.000Z",\n        "2019-08-01T15:51:36.000Z" ],\n    "a": [\n        1.1,\n        1.2,\n        1.3 ],\n    "b": [\n        10,\n        20,\n        null ],\n    "c": [\n        -3.5,\n        null,\n        -5]\n}'
            },
        },
        {
            "workflow_input_name": "multiple_columns_from",
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

