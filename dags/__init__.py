from dagster import define_asset_job
from dagster import Definitions
from dagster import load_assets_from_package_module
from dagster import ScheduleDefinition

from . import assets

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=load_assets_from_package_module(assets), schedules=[daily_refresh_schedule]
)