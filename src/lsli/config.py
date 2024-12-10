"""
config.py: Configuration values. Secrets to be handled with Secrets Manager
"""

import logging
import socket

SKID_NAME = "lsli_skid"

AGOL_ORG = "https://ddwlead-hub.maps.arcgis.com"
SENDGRID_SETTINGS = {  #: Settings for SendGridHandler
    "from_address": "noreply@utah.gov",
    "to_addresses": "jdadams@utah.gov",
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
GRAPHQL_LIMIT = 8000

FEATURE_LAYER_ITEMID = "7d081afc93624d87af7bdf9aaee5163f"
