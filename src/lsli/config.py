"""
config.py: Configuration values. Secrets to be handled with Secrets Manager
"""

import logging
import socket

SKID_NAME = "lsli_skid"

AGOL_ORG = "https://ddwlead-hub.maps.arcgis.com"
SENDGRID_SETTINGS = {  #: Settings for SendGridHandler
    "from_address": "noreply@utah.gov",
    "to_addresses": ["jdadams@utah.gov", "aamirgol@utah.gov"],
    "prefix": f"{SKID_NAME} on {socket.gethostname()}: ",
}
LOG_LEVEL = logging.DEBUG
LOG_FILE_NAME = "log"

GRAPHQl_QUERY = """
        query GetLccrMapUGRC($offset: Int, $limit: Int) {
            getLccrMapUGRC(offset: $offset, limit: $limit) {
                system_id
                pws_id
                pws_name
                pws_county
                pws_population
                serviceline_id
                pws_address
                pws_city
                pws_zipcode
                latitude
                longitude
                serviceline_address
                serviceline_zipcode
                sensitive_population
                system_owned_material
                previously_lead
                so_year_installed
                co_year_installed
                so_basis_classification
                co_basis_classification
                co_material
                so_material
                serviceline_material_cassification
            }
        }
        """
GRAPHQL_LIMIT = 5000

POINTS_FEATURE_LAYER_ITEMID = "7d081afc93624d87af7bdf9aaee5163f"
# SERVICE_AREAS_FEATURE_LAYER_ITEMID = "7591b5684ef34e1fbff8e931ce5acc2e"  #: testing layer
SERVICE_AREAS_FEATURE_LAYER_ITEMID = "6d130188959146b395c91718ce2a5f0c"
SERVICE_AREA_GEOMETRIES_SERVICE_URL = (
    "https://services.arcgis.com/ZzrwjTRez6FJiOq4/arcgis/rest/services/CulinaryWaterServiceAreas/FeatureServer/0/"
)
