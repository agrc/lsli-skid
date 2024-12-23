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
import geopandas as gpd
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
        point_data = PointData()
        point_data.load_records_from_graphql(secrets.GRAPHQL_URL, config.GRAPHQl_QUERY, config.GRAPHQL_LIMIT)

        module_logger.info("Transforming data...")
        point_data.spatialize_point_data()
        point_data.clean_point_data()

        module_logger.info("Loading point data...")
        loader = load.ServiceUpdater(gis, config.POINTS_FEATURE_LAYER_ITEMID, working_dir=tempdir_path)
        features_loaded = loader.truncate_and_load(point_data.spatial_records)

        module_logger.info("Loading system area data from Google Sheet...")
        sheet_data = GoogleSheetData(
            secrets.SERVICE_ACCOUNT_JSON, secrets.SHEET_ID, secrets.SHEET_NAME, secrets.LINKS_ID, secrets.LINKS_NAME
        )
        sheet_data.load_systems_from_sheet()
        sheet_data.clean_approved_systems()
        sheet_data.load_system_links_from_gsheet()
        sheet_data.clean_system_links()
        sheet_data.load_system_geometries(config.SERVICE_AREA_GEOMETRIES_SERVICE_URL)
        sheet_data.merge_systems_and_geometries()
        sheet_data.clean_dataframe_for_agol()

        module_logger.info("Loading system area data to AGOL...")
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

        if not point_data.missing_coords.empty:
            summary_rows.append(f"\n{len(point_data.missing_coords)} Point records are missing coordinates")
            summary_rows.append("-" * 20)
            summary_rows.append(point_data.missing_coords[["pws_id", "pws_name"]].value_counts().to_string(index=False))

        if sheet_data.invalid_pwsids:
            summary_rows.append(f"\n{len(sheet_data.invalid_pwsids)} Invalid PWSIDs found:")
            summary_rows.append("-" * 20)
            summary_rows.extend(sheet_data.invalid_pwsids)

        if sheet_data.duplicate_link_pwsids:
            summary_rows.append(
                f"\n{len(sheet_data.duplicate_link_pwsids)} Duplicate PWSIDs found in the interactive maps sheet:"
            )
            summary_rows.append("-" * 20)
            for name, pwsid in sheet_data.duplicate_link_pwsids.items():
                summary_rows.append(f"{name}: {pwsid}")

        if sheet_data.missing_geometries:
            summary_rows.append(f"\n{len(sheet_data.missing_geometries)} Systems are missing geometries:")
            summary_rows.append("-" * 20)
            for pwsid, (name, classification, area_type) in sheet_data.missing_geometries.items():
                summary_rows.append(f"{pwsid}: {name} (classification: {classification}, type: {area_type})")

        summary_message.message = "\n".join(summary_rows)
        summary_message.attachments = tempdir_path / log_name

        skid_supervisor.notify(summary_message)

        #: Remove file handler so the tempdir will close properly
        loggers = [logging.getLogger(config.SKID_NAME), logging.getLogger("palletjack")]
        _remove_log_file_handlers(log_name, loggers)


class PointData:
    def __init__(self):
        self.records = pd.DataFrame()
        self.spatial_records = pd.DataFrame()
        self.missing_coords = pd.DataFrame()

    def load_records_from_graphql(self, url: str, query: str, limit: int) -> None:
        """Load records from a GraphQL endpoint in chunks

        Args:
            url (str): GraphQL endpoint URL
            query (str): GraphQL query string
            limit (int): The max number of records to return per chunk
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
        records_list = []

        while result_length == limit:
            result = client.execute(query, variable_values={"offset": offset, "limit": limit}, parse_result=True)
            result_length = len(result["getLccrMapUGRC"])
            module_logger.debug("Offset: %s, Length: %s", format(offset, ","), format(result_length, ","))
            offset += limit
            records_list.extend(result["getLccrMapUGRC"])

        self.records = pd.DataFrame(records_list)

    def spatialize_point_data(self) -> None:
        """Convert a dataframe to a spatially-enabled dataframe accounting for both WGS84 and UTM NAD83 coordinates, logging and dropping any missing coordinates

        Any rows with latitude < 100 are assumed to be WGS84, while rows with latitude > 100 are assumed to be UTM
        NAD83.

        Args:
            df (pd.DataFrame): Input Dataframe with "latitude" and "longitude" columns
        """

        self.missing_coords = self.records[self.records["latitude"].isna() | self.records["longitude"].isna()].copy()
        if not self.missing_coords.empty:
            module_logger.warning("%s rows with missing coordinates", len(self.missing_coords))
        self.records.dropna(subset=["latitude", "longitude"], inplace=True)

        web_mercator_dfs = []

        wgs_data = self.records[self.records["latitude"] < 100]
        if not wgs_data.empty:
            module_logger.debug("Loading %s rows with WGS84 coordinates", format(len(wgs_data), ","))
            wgs_spatial = gpd.GeoDataFrame(
                wgs_data, geometry=gpd.points_from_xy(wgs_data["longitude"], wgs_data["latitude"]), crs=4326
            )
            module_logger.debug("Projecting WGS84 data to Web Mercator")
            wgs_spatial.to_crs(3857, inplace=True)
            web_mercator_dfs.append(wgs_spatial)

        utm_data = self.records[self.records["latitude"] > 100]
        if not utm_data.empty:
            module_logger.debug("Loading %s rows with UTM coordinates", format(len(utm_data), ","))
            utm_spatial = gpd.GeoDataFrame(
                utm_data, geometry=gpd.points_from_xy(utm_data["longitude"], utm_data["latitude"]), crs=26912
            )
            module_logger.debug("Projecting UTM data to Web Mercator")
            utm_spatial.to_crs(3857, inplace=True)
            web_mercator_dfs.append(utm_spatial)

        self.spatial_records = pd.concat(web_mercator_dfs)
        self.spatial_records.rename_geometry("SHAPE", inplace=True)
        self.spatial_records = pd.DataFrame.spatial.from_geodataframe(self.spatial_records)

    def clean_point_data(self) -> None:
        """Rename columns for AGOL, convert to 5-digit ZIPs, and convert column dtypes"""

        self.spatial_records.rename(
            columns={"serviceline_material_cassification": "serviceline_material_cassificat"}, inplace=True
        )

        #: Strip off trailing digits for any zipcodes in ZIP+4 format
        self.spatial_records["pws_zipcode"] = self.spatial_records["pws_zipcode"].astype(str).str[:5].astype("Int64")

        self.spatial_records = transform.DataCleaning.switch_to_nullable_int(
            self.spatial_records, ["pws_population", "system_id"]
        )


class GoogleSheetData:
    """Represents data about whole systems loaded from a Google Sheet"""

    systems = pd.DataFrame()
    links = pd.DataFrame()
    cleaned_systems_dataframe = pd.DataFrame()
    cleaned_water_service_areas = pd.DataFrame()
    final_systems = pd.DataFrame()

    missing_geometries = {}
    invalid_pwsids = []
    duplicate_link_pwsids = {}

    def __init__(
        self,
        credentials: str,
        systems_sheet_id: str,
        systems_sheet_name: str,
        links_sheet_id: str,
        links_sheet_name: str,
    ):
        self._credentials = credentials
        self._systems_sheet_id = systems_sheet_id
        self._systems_sheet_name = systems_sheet_name
        self._links_sheet_id = links_sheet_id
        self._links_sheet_name = links_sheet_name

    def load_systems_from_sheet(self) -> pd.DataFrame:
        """Load data from a Google sheet via palletjack using the second row as the header"""

        module_logger.debug("Loading systems from Google Sheet...")
        gsheet_extractor = extract.GSheetLoader(self._credentials)
        self.systems = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self._systems_sheet_id, self._systems_sheet_name, by_title=True
        )

        #: The loader treats the first row of the sheet as the header, but in this case it's the second row
        #: So, the first row of the dataframe is the second row of the sheet and should be used as the header
        self.systems.columns = self.systems.iloc[0]
        self.systems.columns.name = None
        self.systems = self.systems[1:]
        self.systems.replace("", np.nan, inplace=True)

    def clean_approved_systems(self) -> None:
        """Clean up the PWS IDs, log any invalid IDs, and drop all but the most recent entry for each PWS ID"""

        module_logger.debug("Cleaning approved systems data...")
        #: Sheet has lots of empty rows due to formatting
        non_na_systems = self.systems.dropna(subset=["PWS ID"])[
            ["PWS ID", "Time", "System Name", "Approved", "SC, LC, on NTNC"]
        ]

        #: Check for pwsids that dont have digits and report
        non_na_systems["PWS ID"] = non_na_systems["PWS ID"].astype(str)
        invalid_pwsids = non_na_systems[non_na_systems["PWS ID"].str.match(r"^[^\d]*$")]
        if not invalid_pwsids.empty:
            module_logger.warning(
                "The following PWSIDs are invalid: %s", ", ".join(invalid_pwsids["PWS ID"].astype(str).tolist())
            )
            self.invalid_pwsids = invalid_pwsids["PWS ID"].tolist()
            non_na_systems = non_na_systems[~non_na_systems["PWS ID"].str.match(r"^[^\d]*$")]

        #: Clean pwsid, time
        non_na_systems["PWS ID"] = non_na_systems["PWS ID"].str.lower().str.strip("utah").astype(int)
        non_na_systems["Time"] = pd.to_datetime(non_na_systems["Time"], format="mixed")
        non_na_systems.rename(columns={"PWS ID": "PWSID", "Time": "submitted_time"}, inplace=True)
        non_na_systems["area_type"] = "Approved System"

        #: Only use the most recent approval for each system
        self.cleaned_systems_dataframe = non_na_systems.sort_values("submitted_time").drop_duplicates(
            subset="PWSID", keep="last"
        )

    def load_system_links_from_gsheet(self) -> None:
        """Load the interactive maps sheet from Google Sheets using a new extractor"""

        module_logger.debug("Loading interactive map links sheet from Google Sheets...")
        gsheet_extractor = extract.GSheetLoader(self._credentials)
        self.links = gsheet_extractor.load_specific_worksheet_into_dataframe(
            self._links_sheet_id, self._links_sheet_name, by_title=True
        )

    def clean_system_links(self) -> None:
        """Format the PWSID, rename columns, and log & drop duplicate PWSIDs"""

        module_logger.debug("Cleaning interactive map links data...")

        #: Drop empty rows, unneeded columns
        self.links.replace("", np.nan, inplace=True)
        self.links.dropna(subset=["PWSID"], inplace=True)
        self.links = self.links[["PWSID", "Water Systme Name", "Interactive map link"]].copy()

        #: Clean PWSID, drop duplicates, rename columns
        self.links["PWSID"] = self.links["PWSID"].str.lower().str.strip("utah").astype(int)
        self.links.rename(columns={"Water Systme Name": "System Name"}, inplace=True)
        duplicated_links = self.links[self.links["PWSID"].duplicated(keep=False)]

        if not duplicated_links.empty:
            module_logger.warning(
                "Duplicate PWSIDs found in the interactive maps sheet: %s",
                ", ".join(duplicated_links["PWSID"].astype(str).tolist()),
            )
            self.duplicate_link_pwsids = {row["System Name"]: row["PWSID"] for _, row in duplicated_links.iterrows()}

        self.links.drop_duplicates(subset="PWSID", keep="last", inplace=True)
        self.links["area_type"] = "Link"
        self.links.rename(columns={"Interactive map link": "link"}, inplace=True)
        self.links = self.links.reindex(columns=["PWSID", "System Name", "link", "area_type"])

    def load_system_geometries(self, service_areas_service_url: str) -> None:
        """Load the system area geometries from the specified Feature Service URL, converting PWSIDs to ints

        Args:
            service_areas_service_url (str): Full REST endpoint URL, including the layer number
        """

        module_logger.debug("Loading system area geometries from %s...", service_areas_service_url)
        water_service_areas = arcgis.features.FeatureLayer(service_areas_service_url).query(as_df=True)
        self.cleaned_water_service_areas = water_service_areas[water_service_areas["DWSYSNUM"] != " "].copy()
        self.cleaned_water_service_areas["PWSID"] = (
            self.cleaned_water_service_areas["DWSYSNUM"].str.lower().str.strip("utahz").astype(int)
        )

    def merge_systems_and_geometries(self) -> None:
        """Merge geometries to system data, logging any systems that don't have a matching geometry"""

        module_logger.debug("Merging approved systems and interactive map links data with service areas...")
        all_systems = pd.concat([self.cleaned_systems_dataframe, self.links], ignore_index=True)

        merged = all_systems.merge(self.cleaned_water_service_areas, on="PWSID", how="left")
        no_area = merged[merged["FID"].isna()]
        if not no_area.empty:
            self.missing_geometries = {
                row["PWSID"]: (row["System Name"], row["SC, LC, on NTNC"], row["area_type"])
                for _, row in no_area.sort_values(by="PWSID").iterrows()
            }
            module_logger.warning(
                "The following PWSIDs from the approved systems sheet and/or interactive maps sheet were not found in the service areas layer: %s",
                ", ".join(no_area["PWSID"].astype(str).tolist()),
            )
        self.final_systems = merged.dropna(subset=["FID"])

    def clean_dataframe_for_agol(self) -> None:
        """AGOL-ize and lowercase the column names and remove the area and length columns"""

        module_logger.debug("Cleaning dataframe for AGOL...")
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
