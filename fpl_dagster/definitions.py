from dagster import configured, Definitions
from dagster_gcp.gcs import gcs_resource
from dagster_gcp import bigquery_resource
import os
from assets.player_info import player_info
from assets.gw_summary import gw_summary
from assets.bigquery_gw_summary import bigquery_gw_summary
from resources.google_cloud_config import google_cloud_config
from resources.gcs_parquet_io_manager import gcs_parquet_io_manager


#Fixed variables (Regardless of environment)

deployment_environment = os.getenv("DAGSTER_DEPLOYMENT", "development")


#Configure resources for the deployment environments (development/production)
resource_env = {
    "development":{
        "google_config": google_cloud_config.configured(
            {
                "project_bucket": {"env": "PROJECT_BUCKET_DEV"},
                "project_dataset": {"env": "PROJECT_DATASET_DEV"}
            }
        ),
        "gcs_io_manager": gcs_parquet_io_manager,
        "gcs": gcs_resource,
        "bq_res":bigquery_resource
    },
    "production":{
            "google_config": google_cloud_config.configured(
            {
                "project_bucket": {"env": "PROJECT_BUCKET_PROD"},
                "project_dataset": {"env": "PROJECT_DATASET_PROD"}
            }
        ),
        "gcs_io_manager": gcs_parquet_io_manager,
        "gcs": gcs_resource,
        "bq_res":bigquery_resource
    }

}



#Define definitions using the assets and resources for the specified deployment_environment
defos = Definitions(
    assets=[player_info, gw_summary, bigquery_gw_summary],
    resources=resource_env[deployment_environment])