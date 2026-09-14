from typing import ClassVar

from saltbox_sdk.db.mongo.config import get_mongo_db
from saltbox_sdk.migrations.base_migration import BaseMigration
from saltbox_sdk.migrations.stages.base import BaseMigrationStage, RunPythonMigrationStage

LEGACY_TEMPLATE_FILTER: dict = {'repo_id': {'$exists': True}}


async def remove_legacy_repo_templates() -> str:
    collection = get_mongo_db().get_collection('task_templates')

    matched_count = await collection.count_documents(LEGACY_TEMPLATE_FILTER)

    if not matched_count:
        return 'Nothing to remove'

    result = await collection.delete_many(LEGACY_TEMPLATE_FILTER)

    return f'Removed {result.deleted_count} of {matched_count} task templates with the legacy "repo_id" field'


class Migration(BaseMigration):
    dependencies: ClassVar[list[str]] = ['saltbox_core.task_templates.migrations.0002_task_template_add_fields']
    stages: ClassVar[list[BaseMigrationStage]] = [
        RunPythonMigrationStage(callback=remove_legacy_repo_templates),
    ]
