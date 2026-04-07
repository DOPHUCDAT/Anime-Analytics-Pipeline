import sys
from dataclasses import dataclass
from pathlib import Path

import dagster as dg
import dlt
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_dlt import DagsterDltResource, dlt_assets

# --- Path setup ---
sys.path.insert(0, str(Path(__file__).parents[1]))
from data_extract_load.load_data import anime_source


@dataclass
class PipelineConfig:
    """ "Configuration for the DLT pipeline."""

    pipeline_name: str = "raw_anime"
    dataset_name: str = "bronze"
    destination: str = "postgres"


@dataclass
class DbtConfig:
    project_dir: Path
    profiles_dir: Path


class ResourceFactory:
    """Resource factory to create DLT and DBT resources."""

    @staticmethod
    def create_dlt_resource() -> DagsterDltResource:
        return DagsterDltResource()

    @staticmethod
    def create_dbt_resource(config: DbtConfig) -> DbtCliResource:
        project = DbtProject(
            project_dir=config.project_dir,
            profiles_dir=config.profiles_dir,
        )
        project.prepare_if_dev()
        return DbtCliResource(project_dir=project)


def build_dlt_assets(config: PipelineConfig):
    """Build DLT assets for fetching anime data."""
    pipeline = dlt.pipeline(
        pipeline_name=config.pipeline_name,
        dataset_name=config.dataset_name,
        destination=config.destination,
    )

    @dlt_assets(
        dlt_source=anime_source(),
        dlt_pipeline=pipeline,
    )
    def dlt_load(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
        yield from dlt.run(context=context)

    return dlt_load


def build_dbt_assets(dbt_project: DbtProject):
    """Build DBT assets for transforming anime data."""

    @dbt_assets(
        manifest=dbt_project.manifest_path,
    )
    def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    return dbt_models


class JobFactory:
    """Job factory to create Dagster jobs for DLT and DBT."""

    @staticmethod
    def create_dlt_job():
        return dg.define_asset_job(
            "job_dlt",
            selection=dg.AssetSelection.keys("dlt_anime_source_raw_anime"),
        )

    @staticmethod
    def create_dbt_job():
        return dg.define_asset_job(
            "job_dbt",
            selection=dg.AssetSelection.key_prefixes("silver", "gold", "mart"),
        )


class ScheduleFactory:
    """Schedule factory to create schedules for Dagster jobs."""

    @staticmethod
    def create_dlt_schedule(job):
        return dg.ScheduleDefinition(
            job=job,
            cron_schedule="8 0 * * *",
        )


def build_sensor():
    """Build a sensor to trigger the DBT job after DLT assets are updated."""

    @dg.asset_sensor(
        asset_key=dg.AssetKey("dlt_anime_source_raw_anime"),
        job_name="job_dbt",
    )
    def dlt_load_sensor():
        yield dg.RunRequest()

    return dlt_load_sensor


def build_definitions():
    """Build Dagster definitions for the DLT and DBT pipeline."""
    # Config
    pipeline_config = PipelineConfig()
    dbt_config = DbtConfig(
        project_dir=Path(__file__).parents[1] / "data_transformation",
        profiles_dir=Path.home() / ".dbt",
    )

    # Resources
    dlt_resource = ResourceFactory.create_dlt_resource()
    dbt_project = DbtProject(
        project_dir=dbt_config.project_dir,
        profiles_dir=dbt_config.profiles_dir,
    )
    dbt_project.prepare_if_dev()
    dbt_resource = DbtCliResource(project_dir=dbt_project)

    # Assets
    dlt_asset = build_dlt_assets(pipeline_config)
    dbt_asset = build_dbt_assets(dbt_project)

    # Jobs
    job_dlt = JobFactory.create_dlt_job()
    job_dbt = JobFactory.create_dbt_job()

    # Schedule
    schedule_dlt = ScheduleFactory.create_dlt_schedule(job_dlt)

    # Sensor
    sensor = build_sensor()

    return dg.Definitions(
        assets=[dlt_asset, dbt_asset],
        resources={"dlt": dlt_resource, "dbt": dbt_resource},
        jobs=[job_dlt, job_dbt],
        schedules=[schedule_dlt],
        sensors=[sensor],
    )


defs = build_definitions()
