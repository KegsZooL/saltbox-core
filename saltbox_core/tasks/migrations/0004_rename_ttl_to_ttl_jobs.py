from typing import ClassVar

from saltbox_sdk.migrations.base_migration import BaseMigration
from saltbox_sdk.migrations.stages.base import BaseMigrationStage
from saltbox_sdk.migrations.stages.mongo_fields import MongoRenameFieldStage


class Migration(BaseMigration):
    dependencies: ClassVar[list[str]] = ['saltbox_core.tasks.migrations.0003_release_locked_task_minions']
    stages: ClassVar[list[BaseMigrationStage]] = [
        MongoRenameFieldStage(
            collection_name='tasks',
            field_name_old='ttl',
            field_name_new='ttl_jobs',
        ),
    ]
