#!/usr/bin/env python
# * coding: utf8 *
"""
Run the lsli script as a cloud function.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import arcgis
import pandas as pd
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from palletjack import extract, load, transform, utils
from supervisor.message_handlers import SendGridHandler
from supervisor.models import MessageDetails, Supervisor

#: This makes it work when calling with just `python <file>`/installing via pip and in the gcf framework, where
#: the relative imports fail because of how it's calling the function.
try:
    from . import config, version
except ImportError:
    import config
    import version

module_logger = logging.getLogger(config.SKID_NAME)


def _get_secrets():
    """A helper method for loading secrets from either a GCF mount point or the local src/lsli/secrets/secrets.json file

    Raises:
        FileNotFoundError: If the secrets file can't be found.

    Returns:
        dict: The secrets .json loaded as a dictionary
    """

    secret_folder = Path("/secrets")

    #: Try to get the secrets from the Cloud Function mount point
    if secret_folder.exists():
        return json.loads(Path("/secrets/app/secrets.json").read_text(encoding="utf-8"))

    #: Otherwise, try to load a local copy for local development
    secret_folder = Path(__file__).parent / "secrets"
    if secret_folder.exists():
        return json.loads((secret_folder / "secrets.json").read_text(encoding="utf-8"))

    raise FileNotFoundError("Secrets folder not found; secrets not loaded.")


def _initialize(log_path, sendgrid_api_key):
    """A helper method to set up logging and supervisor

    Args:
        log_path (Path): File path for the logfile to be written
        sendgrid_api_key (str): The API key for sendgrid for this particular application

    Returns:
        Supervisor: The supervisor object used for sending messages
    """

    module_logger.setLevel(config.LOG_LEVEL)
    palletjack_logger = logging.getLogger("palletjack")
    palletjack_logger.setLevel(config.LOG_LEVEL)

    cli_handler = logging.StreamHandler(sys.stdout)
    cli_handler.setLevel(config.LOG_LEVEL)
    formatter = logging.Formatter(
        fmt="%(levelname)-7s %(asctime)s %(name)15s:%(lineno)5s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    cli_handler.setFormatter(formatter)

    log_handler = logging.FileHandler(log_path, mode="w")
    log_handler.setLevel(config.LOG_LEVEL)
    log_handler.setFormatter(formatter)

    module_logger.addHandler(cli_handler)
    module_logger.addHandler(log_handler)
    palletjack_logger.addHandler(cli_handler)
    palletjack_logger.addHandler(log_handler)

    #: Log any warnings at logging.WARNING
    #: Put after everything else to prevent creating a duplicate, default formatter
    #: (all log messages were duplicated if put at beginning)
    logging.captureWarnings(True)

    module_logger.debug("Creating Supervisor object")
    skid_supervisor = Supervisor(handle_errors=False)
    sendgrid_settings = config.SENDGRID_SETTINGS
    sendgrid_settings["api_key"] = sendgrid_api_key
    skid_supervisor.add_message_handler(
        SendGridHandler(
            sendgrid_settings=sendgrid_settings, client_name=config.SKID_NAME, client_version=version.__version__
        )
    )

    return skid_supervisor


def _remove_log_file_handlers(log_name, loggers):
    """A helper function to remove the file handlers so the tempdir will close correctly

    Args:
        log_name (str): The logfiles filename
        loggers (List<str>): The loggers that are writing to log_name
    """

    for logger in loggers:
        for handler in logger.handlers:
            try:
                if log_name in handler.stream.name:
                    logger.removeHandler(handler)
                    handler.close()
            except Exception:
                pass


def process():
    """The main function that does all the work."""

    #: Set up secrets, tempdir, supervisor, and logging
    start = datetime.now()

    secrets = SimpleNamespace(**_get_secrets())

    with TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        log_name = f'{config.LOG_FILE_NAME}_{start.strftime("%Y%m%d-%H%M%S")}.txt'
        log_path = tempdir_path / log_name

        skid_supervisor = _initialize(log_path, secrets.SENDGRID_API_KEY)

        #: Get our GIS object via the ArcGIS API for Python
        gis = arcgis.gis.GIS(config.AGOL_ORG, secrets.AGOL_USER, secrets.AGOL_PASSWORD)

        module_logger.info("Loading data from graphql endpoint...")
        records_df = _load_records_from_graphql(secrets.GRAPHQL_URL, config.GRAPHQl_QUERY, config.GRAPHQL_LIMIT)

        module_logger.info("Transforming data...")
        spatial_records = _spatialize_data(records_df)
        spatial_records.rename(
            columns={"serviceline_material_cassification": "serviceline_material_cassificat"}, inplace=True
        )

        #: Strip off trailing digits for any zipcodes in ZIP+4 format
        spatial_records["pws_zipcode"] = spatial_records["pws_zipcode"].astype(str).str[:5].astype("Int64")

        cleaned_spatial_records = transform.DataCleaning.switch_to_nullable_int(
            spatial_records, ["pws_population", "system_id"]
        )

        module_logger.info("Loading data...")
        loader = load.ServiceUpdater(gis, config.FEATURE_LAYER_ITEMID, working_dir=tempdir_path)
        features_loaded = loader.truncate_and_load(cleaned_spatial_records)

        end = datetime.now()

        summary_message = MessageDetails()
        summary_message.subject = f"{config.SKID_NAME} Update Summary"
        summary_rows = [
            f'{config.SKID_NAME} update {start.strftime("%Y-%m-%d")}',
            "=" * 20,
            "",
            f'Start time: {start.strftime("%H:%M:%S")}',
            f'End time: {end.strftime("%H:%M:%S")}',
            f"Duration: {str(end-start)}",
            f"Features loaded: {features_loaded:,}",
        ]

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = tempdir_path / log_name

        skid_supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        _remove_log_file_handlers(log_name, loggers)


def _load_records_from_graphql(url: str, query: str, limit: int) -> pd.DataFrame:
    """Load records from a GraphQL endpoint in chunks

    Args:
        url (str): GraphQL endpoint URL
        query (str): GraphQL query string
        limit (int): The max number of records to return per chunk

    Returns:
        pd.DataFrame: GraphQL records as a DataFrame
    """

    transport = RequestsHTTPTransport(
        url=url,
        verify=True,
        retries=3,
    )
    client = Client(transport=transport, fetch_schema_from_transport=True)
    query = gql(query)

    result_length = limit
    offset = 0
    records = []

    while result_length == limit:
        result = client.execute(query, variable_values={"offset": offset, "limit": limit})
        result_length = len(result["getLccrMapUGRC"])
        module_logger.debug("Offset: %s, Length: %s", format(offset, ","), format(result_length, ","))
        offset += limit
        records.extend(result["getLccrMapUGRC"])

    return pd.DataFrame(records)


def _spatialize_data(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a dataframe to a spatially-enabled dataframe accounting for both WGS84 and UTM NAD83 coordinates

    Any rows with latitude < 100 are assumed to be WGS84, while rows with latitude > 100 are assumed to be UTM NAD83.

    Args:
        df (pd.DataFrame): Input Dataframe with "latitude" and "longitude" columns

    Returns:
        pd.DataFrame: Spatially-enabled DataFrame in Web Mercator (EPSG:3857)
    """

    web_mercator_dfs = []

    wgs_data = df[df["latitude"] < 100]
    if not wgs_data.empty:
        module_logger.debug("Loading %s rows with WGS84 coordinates", format(len(wgs_data), ","))
        wgs_spatial = pd.DataFrame.spatial.from_xy(wgs_data, "longitude", "latitude", sr=4326)
        module_logger.debug("Projecting WGS84 data to Web Mercator")
        wgs_spatial.spatial.project(3857)
        web_mercator_dfs.append(wgs_spatial)

    utm_data = df[df["latitude"] > 100]
    if not utm_data.empty:
        module_logger.debug("Loading %s rows with UTM coordinates", format(len(utm_data), ","))
        utm_spatial = pd.DataFrame.spatial.from_xy(utm_data, "longitude", "latitude", sr=26912)
        module_logger.debug("Projecting UTM data to Web Mercator")
        utm_spatial.spatial.project(3857)
        web_mercator_dfs.append(utm_spatial)

    return pd.concat(web_mercator_dfs)


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    process()
