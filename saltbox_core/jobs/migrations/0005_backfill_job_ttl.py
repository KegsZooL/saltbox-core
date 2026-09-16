from typing import ClassVar

from saltbox_core.config import SETTINGS
from saltbox_sdk.migrations.base_migration import BaseMigration
from saltbox_sdk.migrations.stages.base import BaseMigrationStage
from saltbox_sdk.migrations.stages.mongo_fields import MongoSetFieldStage


class Migration(BaseMigration):
    dependencies: ClassVar[list[str]] = ['saltbox_core.jobs.migrations.0004_backfill_job_return_job_id']
    stages: ClassVar[list[BaseMigrationStage]] = [
        MongoSetFieldStage(
            collection_name='jobs',
            field_name='ttl',
            value=SETTINGS.jobs_default_ttl,
            mongo_filter={'ttl': None},
        ),
    ]
