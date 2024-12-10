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
