from typing import Any, ClassVar

from bson import ObjectId
from pymongo import UpdateOne

from saltbox_sdk.db.mongo.config import get_mongo_db
from saltbox_sdk.migrations.base_migration import BaseMigration
from saltbox_sdk.migrations.stages.base import BaseMigrationStage, RunPythonMigrationStage
from saltbox_sdk.utilities.helpers import utc_now

ITEM_KEYS = {'_id', 'is_system', 'updated_at', 'data'}
BULK_SIZE = 500


def wrap_item(item: dict[str, Any]) -> dict[str, Any] | None:
    if set(item) == ITEM_KEYS:
        if isinstance(item['_id'], ObjectId):
            return None
        return {**item, '_id': ObjectId(item['_id'])}

    data = dict(item)
    item_id = data.pop('_id', None)

    return {
        '_id': ObjectId(item_id) if item_id is not None else ObjectId(),
        'is_system': data.pop('is_system', True),
        'updated_at': data.pop('updated_at', None) or utc_now(),
        'data': data,
    }


async def wrap_extra_static_items() -> str:
    collection = get_mongo_db().get_collection('minions')
    minions = await collection.find({'extra_static': {'$exists': True, '$ne': {}}}, {'extra_static': 1}).to_list()

    operations: list[UpdateOne] = []
    wrapped_items = 0

    for minion in minions:
        extra_static: dict[str, Any] = minion['extra_static']
        changed = False

        for categories in extra_static.values():
            for name, items in categories.items():
                new_items = []
                for item in items:
                    wrapped = wrap_item(item)
                    new_items.append(wrapped or item)
                    if wrapped is not None:
                        changed = True
                        wrapped_items += 1
                categories[name] = new_items

        if changed:
            operations.append(UpdateOne({'_id': minion['_id']}, {'$set': {'extra_static': extra_static}}))

        if len(operations) >= BULK_SIZE:
            await collection.bulk_write(operations)
            operations = []

    if operations:
        await collection.bulk_write(operations)

    return f'Wrapped {wrapped_items} extra_static item(s)'


class Migration(BaseMigration):
    dependencies: ClassVar[list[str]] = ['saltbox_core.minion_collections.migrations.0001_init']
    stages: ClassVar[list[BaseMigrationStage]] = [
        RunPythonMigrationStage(callback=wrap_extra_static_items),
    ]
