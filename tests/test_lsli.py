import logging

import numpy as np
import pandas as pd

from lsli import main


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


class TestPointData:
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

        point_data_mock = mocker.Mock(spec=main.PointData)

        main.PointData.load_records_from_graphql(point_data_mock, "url", "query", 2)

        expected_df = pd.DataFrame(
            [
                {"foo": "bar"},
                {"foo": "baz"},
                {"foo": "boo"},
                {"foo": "bat"},
            ]
        )

        pd.testing.assert_frame_equal(point_data_mock.records, expected_df)

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

        point_data_mock = mocker.Mock(spec=main.PointData)
        main.PointData.load_records_from_graphql(point_data_mock, "url", "query", 2)

        expected_df = pd.DataFrame(
            [
                {"foo": "bar"},
                {"foo": "baz"},
                {"foo": "boo"},
                {"foo": "bat"},
                {"foo": "bop"},
            ]
        )

        pd.testing.assert_frame_equal(point_data_mock.records, expected_df)

    def test_spatialize_data_logs_and_drops_na_coords(self, mocker, caplog):
        gdf_class_mock = mocker.patch("lsli.main.gpd.GeoDataFrame", autospec=True)
        points_from_xy_mock = mocker.patch.object(main.gpd, "points_from_xy")
        concat_mock = mocker.patch.object(main.pd, "concat", autospec=True)
        point_data_mock = mocker.Mock(spec=main.PointData)
        mocker.patch.object(main.pd.DataFrame.spatial, "from_geodataframe", autospec=True)

        caplog.set_level(logging.DEBUG)

        df = pd.DataFrame(
            {
                "latitude": [np.nan, 40.0],
                "longitude": [-112.0, -111.0],
            }
        )
        point_data_mock.records = df

        main.PointData.spatialize_point_data(point_data_mock)

        missing_rows = pd.DataFrame({"latitude": [np.nan], "longitude": [-112.0]})

        #: Make sure NA row is logged
        pd.testing.assert_frame_equal(point_data_mock.missing_coords, missing_rows)

        #: Make sure full dataframe is used and there's only one call
        pd.testing.assert_series_equal(points_from_xy_mock.call_args_list[0][0][0], df.dropna()["longitude"])
        pd.testing.assert_series_equal(points_from_xy_mock.call_args_list[0][0][1], df.dropna()["latitude"])
        assert gdf_class_mock.call_args_list[0].kwargs["crs"] == 4326
        points_from_xy_mock.assert_called_once()

        #: Make sure log shows only WGS84 processed
        assert "1 rows with WGS84 coordinates" in caplog.text
        assert "rows with UTM coordinates" not in caplog.text
        assert "1 rows with missing coordinates" in caplog.text

        #: Make sure reprojection and concat only called once/with one item
        assert len(gdf_class_mock.method_calls) == 1
        assert gdf_class_mock.method_calls[0] == mocker.call().to_crs(3857, inplace=True)
        concat_mock.assert_called_once_with([gdf_class_mock.return_value])

    def test_spatialize_data_sorts_different_projections(self, mocker, caplog):
        gdf_class_mock = mocker.patch("lsli.main.gpd.GeoDataFrame", autospec=True)
        points_from_xy_mock = mocker.patch.object(main.gpd, "points_from_xy")
        concat_mock = mocker.patch.object(main.pd, "concat", autospec=True)
        point_data_mock = mocker.Mock(spec=main.PointData)
        mocker.patch.object(main.pd.DataFrame.spatial, "from_geodataframe", autospec=True)

        caplog.set_level(logging.DEBUG)

        df = pd.DataFrame(
            {
                "latitude": [123.0, 40.0],
                "longitude": [0.0, 0.0],
            }
        )
        point_data_mock.records = df

        main.PointData.spatialize_point_data(point_data_mock)

        #: test that right dataframe subsets are called in order
        pd.testing.assert_series_equal(
            points_from_xy_mock.call_args_list[0][0][0], df[df["latitude"] < 100]["longitude"]
        )
        pd.testing.assert_series_equal(
            points_from_xy_mock.call_args_list[0][0][1], df[df["latitude"] < 100]["latitude"]
        )
        pd.testing.assert_series_equal(
            points_from_xy_mock.call_args_list[1][0][0], df[df["latitude"] >= 100]["longitude"]
        )
        pd.testing.assert_series_equal(
            points_from_xy_mock.call_args_list[1][0][1], df[df["latitude"] >= 100]["latitude"]
        )

        #: Make sure log messages reflect proper number of rows
        assert "1 rows with WGS84 coordinates" in caplog.text
        assert "1 rows with UTM coordinates" in caplog.text

        #: Check projection calls
        assert gdf_class_mock.call_args_list[0].kwargs["crs"] == 4326
        assert gdf_class_mock.call_args_list[1].kwargs["crs"] == 26912

        #: Check final projection and concatenation
        assert len(gdf_class_mock.method_calls) == 2
        assert gdf_class_mock.method_calls[0] == mocker.call().to_crs(3857, inplace=True)
        assert gdf_class_mock.method_calls[1] == mocker.call().to_crs(3857, inplace=True)
        concat_mock.assert_called_once_with([gdf_class_mock.return_value, gdf_class_mock.return_value])

    def test_spatialize_data_handles_no_utm_coords(self, mocker, caplog):
        gdf_class_mock = mocker.patch("lsli.main.gpd.GeoDataFrame", autospec=True)
        points_from_xy_mock = mocker.patch.object(main.gpd, "points_from_xy")
        concat_mock = mocker.patch.object(main.pd, "concat", autospec=True)
        point_data_mock = mocker.Mock(spec=main.PointData)
        mocker.patch.object(main.pd.DataFrame.spatial, "from_geodataframe", autospec=True)

        caplog.set_level(logging.DEBUG)

        df = pd.DataFrame(
            {
                "latitude": [41.0, 40.0],
                "longitude": [0.0, 0.0],
            }
        )
        point_data_mock.records = df

        main.PointData.spatialize_point_data(point_data_mock)

        #: Make sure full dataframe is used and there's only one call
        pd.testing.assert_series_equal(points_from_xy_mock.call_args_list[0][0][0], df["longitude"])
        pd.testing.assert_series_equal(points_from_xy_mock.call_args_list[0][0][1], df["latitude"])
        assert gdf_class_mock.call_args_list[0].kwargs["crs"] == 4326
        points_from_xy_mock.assert_called_once()

        #: Make sure log shows only WGS84 processed
        assert "2 rows with WGS84 coordinates" in caplog.text
        assert "rows with UTM coordinates" not in caplog.text

        #: Make sure reprojection and concat only called once/with one item
        assert len(gdf_class_mock.method_calls) == 1
        assert gdf_class_mock.method_calls[0] == mocker.call().to_crs(3857, inplace=True)
        concat_mock.assert_called_once_with([gdf_class_mock.return_value])

    def test_clean_point_data_cleans_data(self, mocker):
        point_data_mock = mocker.Mock(spec=main.PointData)
        point_data_mock.spatial_records = pd.DataFrame(
            {
                "serviceline_material_cassification": ["foo", "bar"],
                "pws_zipcode": ["84093", "84093-1234"],
                "pws_population": ["1000", "2000"],
                "system_id": ["1234", "5678"],
            }
        )

        main.PointData.clean_point_data(point_data_mock)

        expected_df = pd.DataFrame(
            {
                "serviceline_material_cassificat": ["foo", "bar"],
                "pws_zipcode": [84093, 84093],
                "pws_population": [1000, 2000],
                "system_id": [1234, 5678],
            }
        )
        expected_df["pws_zipcode"] = expected_df["pws_zipcode"].astype("Int64")
        expected_df["pws_population"] = expected_df["pws_population"].astype("Int64")
        expected_df["system_id"] = expected_df["system_id"].astype("Int64")

        pd.testing.assert_frame_equal(point_data_mock.spatial_records, expected_df)


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
            spec=main.GoogleSheetData,
            _credentials="credentials",
            _systems_sheet_id="sheet_id",
            _systems_sheet_name="sheet_name",
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
                "submitted_time": ["1/1/2024", "1/23/2024 15:55"],
                "System Name": ["bar", "foo"],
                "Approved": ["Reject", "Accept"],
                "SC, LC, on NTNC": [np.nan, "SC"],
                "area_type": ["Approved System", "Approved System"],
            },
            index=[2, 0],
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)
        expected_output["submitted_time"] = pd.to_datetime(expected_output["submitted_time"], format="mixed")

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
                "submitted_time": ["1/1/2024", "1/23/2024 15:55"],
                "System Name": ["bar", "foo"],
                "Approved": ["Reject", "Accept"],
                "SC, LC, on NTNC": [np.nan, "SC"],
                "area_type": ["Approved System", "Approved System"],
            },
            index=[2, 0],
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)
        expected_output["submitted_time"] = pd.to_datetime(expected_output["submitted_time"], format="mixed")

        pd.testing.assert_frame_equal(instance_mock.cleaned_systems_dataframe, expected_output)

    def test_clean_approved_systems_removes_and_logs_invalid_pwsids(self, mocker):
        input_data = pd.DataFrame(
            {
                "PWS ID": ["Utah1234", "Valley Water System"],
                "Time": ["1/23/2024 15:55", "1/1/2024 15:55"],
                "System Name": ["foo", "foo"],
                "Approved": ["Accept", "Reject"],
                "SC, LC, on NTNC": ["SC", "SC"],
            }
        )
        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.systems = input_data

        main.GoogleSheetData.clean_approved_systems(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": ["1234"],
                "submitted_time": ["1/23/2024 15:55"],
                "System Name": ["foo"],
                "Approved": ["Accept"],
                "SC, LC, on NTNC": ["SC"],
                "area_type": ["Approved System"],
            },
            index=[0],
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)
        expected_output["submitted_time"] = pd.to_datetime(expected_output["submitted_time"], format="mixed")

        pd.testing.assert_frame_equal(instance_mock.cleaned_systems_dataframe, expected_output)
        assert instance_mock.invalid_pwsids == ["Valley Water System"]

    def test_merge_systems_and_geometries_drops_and_reports_no_matches(self, mocker, caplog):
        systems = pd.DataFrame(
            {
                "PWSID": [1234, 4567, 8910],
                "System Name": ["foo", "bar", "baz"],
                "SC, LC, on NTNC": ["SC", "LC", "NTNC"],
                "area_type": ["Approved System", "Approved System", "Approved System"],
            }
        )

        links = pd.DataFrame(
            {
                "PWSID": [1112, 1314],
                "System Name": ["boo", "bee"],
                "Interactive map link": ["link1", "link2"],
                "area_type": ["Link", "Link"],
            }
        )

        areas = pd.DataFrame(
            {
                "PWSID": [4567, 8910, 1112],
                "Area": ["bar", "baz", "boo"],
                "FID": [2, 3, 4],
            }
        )

        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.cleaned_systems_dataframe = systems
        instance_mock.cleaned_water_service_areas = areas
        instance_mock.links = links

        main.GoogleSheetData.merge_systems_and_geometries(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [4567, 8910, 1112],
                "System Name": ["bar", "baz", "boo"],
                "SC, LC, on NTNC": ["LC", "NTNC", None],
                "area_type": ["Approved System", "Approved System", "Link"],
                "Interactive map link": [None, None, "link1"],
                "Area": ["bar", "baz", "boo"],
                "FID": [2.0, 3.0, 4.0],
            },
            index=[1, 2, 3],
        )
        expected_missing_geometries_dict = {
            1234: ("foo", "SC", "Approved System"),
            1314: ("bee", np.nan, "Link"),
        }
        expected_message = "The following PWSIDs from the approved systems sheet and/or interactive maps sheet were not found in the service areas layer: 1234, 1314"

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

    def test_clean_system_links_cleans_pwsid_and_renames(self, mocker):
        input_data = pd.DataFrame(
            {
                "PWSID": ["Utah1234", "UTAH4567"],
                "Water Systme Name": ["foo", "bar"],
                "Interactive map link": ["link1", "link2"],
            }
        )

        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.links = input_data

        main.GoogleSheetData.clean_system_links(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [1234, 4567],
                "System Name": ["foo", "bar"],
                "link": ["link1", "link2"],
                "area_type": ["Link", "Link"],
            }
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)

        pd.testing.assert_frame_equal(instance_mock.links, expected_output)

    def test_clean_system_links_warns_logs_and_drops_duplicate_pwsids(self, mocker, caplog):
        input_data = pd.DataFrame(
            {
                "PWSID": ["Utah1234", "UTAH1234"],
                "Water Systme Name": ["foo", "bar"],
                "Interactive map link": ["link1", "link2"],
                "extra column": ["yes", "no"],
            }
        )

        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.links = input_data

        main.GoogleSheetData.clean_system_links(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [1234],
                "System Name": ["bar"],
                "link": ["link2"],
                "area_type": ["Link"],
            },
            index=[1],
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)

        pd.testing.assert_frame_equal(instance_mock.links, expected_output)
        assert "Duplicate PWSIDs found in the interactive maps sheet: 1234, 1234" in caplog.text
        assert instance_mock.duplicate_link_pwsids == {"foo": 1234, "bar": 1234}

    def test_clean_system_links_removes_empty_rows(self, mocker):
        input_data = pd.DataFrame(
            {
                "PWSID": ["Utah1234", ""],
                "Water Systme Name": ["foo", ""],
                "Interactive map link": ["link1", ""],
            }
        )

        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.links = input_data

        main.GoogleSheetData.clean_system_links(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [1234],
                "System Name": ["foo"],
                "link": ["link1"],
                "area_type": ["Link"],
            },
            index=[0],
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)

        pd.testing.assert_frame_equal(instance_mock.links, expected_output)

    def test_clean_system_links_subsets_columns(self, mocker):
        input_data = pd.DataFrame(
            {
                "PWSID": ["Utah1234", "UTAH4567"],
                "Water Systme Name": ["foo", "bar"],
                "Interactive map link": ["link1", "link2"],
                "extra column": ["yes", "no"],
            }
        )

        instance_mock = mocker.Mock(spec=main.GoogleSheetData)
        instance_mock.links = input_data

        main.GoogleSheetData.clean_system_links(instance_mock)

        expected_output = pd.DataFrame(
            {
                "PWSID": [1234, 4567],
                "System Name": ["foo", "bar"],
                "link": ["link1", "link2"],
                "area_type": ["Link", "Link"],
            }
        )
        expected_output["PWSID"] = expected_output["PWSID"].astype(int)

        pd.testing.assert_frame_equal(instance_mock.links, expected_output)
