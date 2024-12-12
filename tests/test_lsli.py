import logging

import numpy as np
import pandas as pd

from lsli import config, main


def test_get_secrets_from_gcp_location(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}


def test_get_secrets_from_local_location(mocker):
    exists_mock = mocker.Mock(side_effect=[False, True])
    mocker.patch("pathlib.Path.exists", new=exists_mock)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}
    assert exists_mock.call_count == 2


class TestGraphQL:
    def test_load_records_from_graphql_extends_list(self, mocker):
        mocker.patch("lsli.main.RequestsHTTPTransport", autospec=True)
        mocker.patch("lsli.main.gql", autospec=True)

        client_mock = mocker.Mock(spec=main.Client)
        client_mock.execute.side_effect = [
            {"getLccrMapUGRC": [{"foo": "bar"}, {"foo": "baz"}]},
            {"getLccrMapUGRC": [{"foo": "boo"}, {"foo": "bat"}]},
            {"getLccrMapUGRC": []},
        ]
        mocker.patch("lsli.main.Client", return_value=client_mock)

        output_df = main._load_records_from_graphql("url", "query", 2)

        expected_df = pd.DataFrame(
            [
                {"foo": "bar"},
                {"foo": "baz"},
                {"foo": "boo"},
                {"foo": "bat"},
            ]
        )

        pd.testing.assert_frame_equal(output_df, expected_df)

    def test_load_records_from_graphql_stops_on_partial_length_result(self, mocker):
        mocker.patch("lsli.main.RequestsHTTPTransport", autospec=True)
        mocker.patch("lsli.main.gql", autospec=True)

        client_mock = mocker.Mock(spec=main.Client)
        client_mock.execute.side_effect = [
            {"getLccrMapUGRC": [{"foo": "bar"}, {"foo": "baz"}]},
            {"getLccrMapUGRC": [{"foo": "boo"}, {"foo": "bat"}]},
            {"getLccrMapUGRC": [{"foo": "bop"}]},
        ]
        mocker.patch("lsli.main.Client", return_value=client_mock)

        output_df = main._load_records_from_graphql("url", "query", 2)

        expected_df = pd.DataFrame(
            [
                {"foo": "bar"},
                {"foo": "baz"},
                {"foo": "boo"},
                {"foo": "bat"},
                {"foo": "bop"},
            ]
        )

        pd.testing.assert_frame_equal(output_df, expected_df)


class TestSpatializer:
    def test_spatialize_data_sorts_different_projections(self, mocker, caplog):
        spatial_df_mock = mocker.Mock()
        from_xy_mock = mocker.patch.object(main.pd.DataFrame.spatial, "from_xy", return_value=spatial_df_mock)
        concat_mock = mocker.patch.object(main.pd, "concat", autospec=True)

        caplog.set_level(logging.DEBUG)

        df = pd.DataFrame(
            {
                "latitude": [123, 40],
                "longitude": [0, 0],
            }
        )

        main._spatialize_data(df)

        #: test that right dataframe subsets are called in order
        assert np.array_equal(from_xy_mock.call_args_list[0][0][0].values, df[df["latitude"] < 100].values)
        assert np.array_equal(from_xy_mock.call_args_list[1][0][0].values, df[df["latitude"] >= 100].values)

        #: Make sure log messages reflect proper number of rows
        assert "1 rows with WGS84 coordinates" in caplog.text
        assert "1 rows with UTM coordinates" in caplog.text

        #: Check projection calls
        assert from_xy_mock.call_args_list[0].kwargs == {"sr": 4326}
        assert from_xy_mock.call_args_list[1].kwargs == {"sr": 26912}

        #: Check final projection and concatenation
        assert spatial_df_mock.spatial.project.call_args_list == [mocker.call(3857), mocker.call(3857)]
        concat_mock.assert_called_once_with([spatial_df_mock, spatial_df_mock])

    def test_spatialize_data_handles_no_utm_coords(self, mocker, caplog):
        spatial_df_mock = mocker.Mock()
        from_xy_mock = mocker.patch.object(main.pd.DataFrame.spatial, "from_xy", return_value=spatial_df_mock)
        concat_mock = mocker.patch.object(main.pd, "concat", autospec=True)

        caplog.set_level(logging.DEBUG)

        df = pd.DataFrame(
            {
                "latitude": [41, 40],
                "longitude": [0, 0],
            }
        )

        main._spatialize_data(df)

        #: Make sure full dataframe is used and there's only one call
        pd.testing.assert_frame_equal(from_xy_mock.call_args_list[0][0][0], df)
        assert from_xy_mock.call_args_list[0].kwargs == {"sr": 4326}
        from_xy_mock.assert_called_once()

        #: Make sure log shows only WGS84 processed
        assert "2 rows with WGS84 coordinates" in caplog.text
        assert "rows with UTM coordinates" not in caplog.text

        #: Make sure reprojection and concat only called once/with one item
        spatial_df_mock.spatial.project.assert_called_once_with(3857)
        concat_mock.assert_called_once_with([spatial_df_mock])


class TestGoogleSheetData:
    def test_load_dataframe_from_sheet_switches_header_and_fills_nas(self, mocker):
        input_data = pd.DataFrame(
            {
                "": ["name", "foo", "bar"],
                "bogus_row": ["value", "a", ""],
                "bogus_row_2": ["id", 10, 11],
            }
        )

        loader_mock = mocker.patch("lsli.main.extract.GSheetLoader")
        loader_mock.return_value.load_specific_worksheet_into_dataframe.return_value = input_data
        instance_mock = mocker.Mock(
            spec=main.GoogleSheetData, _credentials="credentials", _sheet_id="sheet_id", _sheet_name="sheet_name"
        )

        main.GoogleSheetData.load_systems_from_sheet(instance_mock)

        expected_df = pd.DataFrame(
            {
                "name": ["foo", "bar"],
                "value": ["a", np.nan],
                "id": [10, 11],
            },
            index=[1, 2],
        )

        pd.testing.assert_frame_equal(instance_mock.systems, expected_df)

    def test_clean_approved_systems_cleans_data(self, mocker):
        input_data = pd.DataFrame(
            {
                "PWS ID": ["Utah1234", np.nan, "4567"],
                "Time": ["1/23/2024 15:55", np.nan, "1/1/2024"],
                "System Name": ["foo", np.nan, "bar"],
                "Approved": ["Accept", np.nan, "Reject"],
                "SC, LC, on NTNC": ["SC", np.nan, np.nan],
                "extra column": ["yes", "no", "yes"],
            }
        )
        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.systems = input_data

        main.GoogleSheetData.clean_approved_systems(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [4567, 1234],
                "Time": ["1/1/2024", "1/23/2024 15:55"],
                "System Name": ["bar", "foo"],
                "Approved": ["Reject", "Accept"],
                "SC, LC, on NTNC": [np.nan, "SC"],
            },
            index=[2, 0],
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)
        expected_output["Time"] = pd.to_datetime(expected_output["Time"], format="mixed")

        pd.testing.assert_frame_equal(instance_mock.cleaned_systems_dataframe, expected_output)

    def test_clean_approved_systems_removes_earlier_duplicate(self, mocker):
        input_data = pd.DataFrame(
            {
                "PWS ID": ["Utah1234", "Utah1234", "4567"],
                "Time": ["1/23/2024 15:55", "1/1/2024 15:55", "1/1/2024"],
                "System Name": ["foo", "foo", "bar"],
                "Approved": ["Accept", "Reject", "Reject"],
                "SC, LC, on NTNC": ["SC", "SC", np.nan],
            }
        )
        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.systems = input_data

        main.GoogleSheetData.clean_approved_systems(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [4567, 1234],
                "Time": ["1/1/2024", "1/23/2024 15:55"],
                "System Name": ["bar", "foo"],
                "Approved": ["Reject", "Accept"],
                "SC, LC, on NTNC": [np.nan, "SC"],
            },
            index=[2, 0],
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)
        expected_output["Time"] = pd.to_datetime(expected_output["Time"], format="mixed")

        pd.testing.assert_frame_equal(instance_mock.cleaned_systems_dataframe, expected_output)

    def test_merge_systems_and_geometries_drops_and_reports_no_matches(self, mocker, caplog):
        systems = pd.DataFrame(
            {
                "PWSID": [1234, 4567, 8910],
                "System Name": ["foo", "bar", "baz"],
                "SC, LC, on NTNC": ["SC", "LC", "NTNC"],
            }
        )

        areas = pd.DataFrame(
            {
                "PWSID": [4567, 8910],
                "Area": ["bar", "baz"],
                "FID": [2, 3],
            }
        )

        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.cleaned_systems_dataframe = systems
        instance_mock.cleaned_water_service_areas = areas

        main.GoogleSheetData.merge_systems_and_geometries(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [4567, 8910],
                "System Name": ["bar", "baz"],
                "SC, LC, on NTNC": ["LC", "NTNC"],
                "Area": ["bar", "baz"],
                "FID": [2.0, 3.0],
            },
            index=[1, 2],
        )
        expected_missing_geometries_dict = {
            1234: ("foo", "SC"),
        }
        expected_message = "The following PWSIDs were not found in the service areas layer: 1234"

        pd.testing.assert_frame_equal(instance_mock.final_systems, expected_output)
        assert instance_mock.missing_geometries == expected_missing_geometries_dict
        assert expected_message in caplog.text

    def test_clean_dataframe_for_agol(self, mocker):
        input_data = pd.DataFrame(
            {
                "PWSID": [1234, 4567, 8910],
                "System Name": ["foo", "bar", "baz"],
                "SC, LC, on NTNC": ["SC", "LC", "NTNC"],
                "Shape__Area": [1, 2, 3],
                "Shape__Length": [4, 5, 6],
                "SHAPE": ["shape1", "shape2", "shape3"],
            }
        )

        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.final_systems = input_data

        main.GoogleSheetData.clean_dataframe_for_agol(instance_mock)

        expected_output = pd.DataFrame(
            {
                "pwsid": [1234, 4567, 8910],
                "system_name": ["foo", "bar", "baz"],
                "sc__lc__on_ntnc": ["SC", "LC", "NTNC"],
                "SHAPE": ["shape1", "shape2", "shape3"],
            },
        )

        pd.testing.assert_frame_equal(instance_mock.final_systems, expected_output)
