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
import numpy as np
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

        module_logger.info("Loading point data...")
        loader = load.ServiceUpdater(gis, config.POINTS_FEATURE_LAYER_ITEMID, working_dir=tempdir_path)
        features_loaded = loader.truncate_and_load(cleaned_spatial_records)

        module_logger.info("Loading system area data from Google Sheet...")
        sheet_data = GoogleSheetData(secrets.SERVICE_ACCOUNT_JSON, secrets.SHEET_ID, secrets.SHEET_NAME)
        sheet_data.load_approved_systems()
        sheet_data.load_system_geometries(config.SERVICE_AREAS_SERVICE_URL)
        sheet_data.merge_systems_and_geometries()
        sheet_data.clean_dataframe_for_agol()
        service_area_loader = load.ServiceUpdater(
            gis, config.SERVICE_AREAS_FEATURE_LAYER_ITEMID, working_dir=tempdir_path
        )
        areas_loaded = service_area_loader.truncate_and_load(sheet_data.final_systems)

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
            f"Points loaded: {features_loaded}",
            f"Areas loaded: {areas_loaded}",
        ]

        if sheet_data.missing_geometries:
            summary_rows.append("Missing Geometries:")
            for pwsid, (name, status) in sheet_data.missing_geometries.items():
                summary_rows.append(f"{pwsid}: {name} ({status})")

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


class GoogleSheetData:
    """Represents data about whole systems loaded from a Google Sheet"""

    systems_dataframe = pd.DataFrame()
    cleaned_water_service_areas = pd.DataFrame()
    missing_geometries = {}
    final_systems = pd.DataFrame()

    def __init__(self, credentials: str, sheet_id: str, sheet_name: str):
        self._credentials = credentials
        self._sheet_id = sheet_id
        self._sheet_name = sheet_name

    def _load_dataframe_from_sheet(self) -> pd.DataFrame:
        """Load data from a Google sheet using palletjack using the second row as the header

        Returns:
            pd.DataFrame: The desired tab of the Google Sheet as a DataFrame
        """

        gsheet_extractor = extract.GSheetLoader(self._credentials)
        systems = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self._sheet_id, self._sheet_name, by_title=True
        )

        #: The loader treats the first row of the sheet as the header, but in this case it's the second row
        #: So, the first row of the dataframe is the second row of the sheet and should be used as the header
        systems.columns = systems.iloc[0]
        systems.columns.name = None
        systems = systems[1:]
        systems.replace("", np.nan, inplace=True)

        return systems

    def load_approved_systems(self) -> None:
        #: TODO: add check for rows with invalid PWSID, remove and report them
        systems = self._load_dataframe_from_sheet()

        #: Remove rows w/o PWS ID, clean up PWS ID and time
        non_na_systems = systems.dropna(subset=["PWS ID"])[
            ["PWS ID", "Time", "System Name", "Approved", "SC, LC, on NTNC"]
        ]
        non_na_systems["PWS ID"] = non_na_systems["PWS ID"].astype(str).str.lower().str.strip("utah").astype(int)
        non_na_systems["Time"] = pd.to_datetime(non_na_systems["Time"], format="mixed")
        non_na_systems.rename(columns={"PWS ID": "PWSID"}, inplace=True)

        #: Only use the most recent approval for each system
        self.systems_dataframe = non_na_systems.sort_values("Time").drop_duplicates(subset="PWSID", keep="last")

    def load_system_geometries(self, service_areas_service_url: str) -> None:
        """Load the system area geometries from the specified Feature Service URL

        Args:
            service_areas_service_url (str): Full REST endpoint URL, including the layer number
        """

        water_service_areas = arcgis.features.FeatureLayer(service_areas_service_url).query(as_df=True)
        self.cleaned_water_service_areas = water_service_areas[water_service_areas["DWSYSNUM"] != " "].copy()
        self.cleaned_water_service_areas["PWSID"] = (
            self.cleaned_water_service_areas["DWSYSNUM"].str.lower().str.strip("utahz").astype(int)
        )

    def merge_systems_and_geometries(self) -> None:
        """Merge geometries to system data, logging any systems that don't have a matching geometry"""

        merged = self.systems_dataframe.merge(self.cleaned_water_service_areas, on="PWSID", how="left")
        no_area = merged[merged["FID"].isna()]
        if not no_area.empty:
            self.missing_geometries = {
                row["PWSID"]: (row["System Name"], row["SC, LC, on NTNC"]) for _, row in no_area.iterrows()
            }
            module_logger.warning(
                "The following PWSIDs were not found in the service areas layer: %s",
                ", ".join(no_area["PWSID"].astype(str).tolist()),
            )
        self.final_systems = merged.dropna(subset=["FID"])

    def clean_dataframe_for_agol(self) -> None:
        """AGOL-ize and lowercase the column names and remove the area and length columns"""

        cleaned_columns = {
            original_name: agol_name.lower()
            for original_name, agol_name in utils.rename_columns_for_agol(self.final_systems.columns).items()
        }
        cleaned_columns.pop("SHAPE")
        self.final_systems.rename(columns=cleaned_columns, inplace=True)
        self.final_systems.drop(columns=["shape__area", "shape__length"], inplace=True)


#: Putting this here means you can call the file via `python main.py` and it will run. Useful for pre-GCF testing.
if __name__ == "__main__":
    process()
