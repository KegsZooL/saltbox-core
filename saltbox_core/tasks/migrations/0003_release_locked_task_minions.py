from typing import ClassVar

from saltbox_sdk.db.mongo.config import get_mongo_db
from saltbox_sdk.migrations.base_migration import BaseMigration
from saltbox_sdk.migrations.stages.base import BaseMigrationStage, RunPythonMigrationStage
from saltbox_sdk.migrations.stages.mongo_fields import MongoSetFieldStage

LOCKED_TASK_MINIONS_PIPELINE: list[dict] = [
    {'$match': {'status': 'in_work'}},
    {
        '$lookup': {
            'from': 'minions',
            'localField': 'minion_inner_id',
            'foreignField': '_id',
            'as': 'minion',
        }
    },
    {'$unwind': '$minion'},
    {
        '$lookup': {
            'from': 'job_returns',
            'let': {
                'task_id': {'$toString': '$task_id'},
                'minion_id': '$minion.minion_id',
                'salt_master': '$minion.master',
            },
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {'$eq': ['$source.type', 'task']},
                                {'$eq': ['$source.id', '$$task_id']},
                                {'$eq': ['$minion_id', '$$minion_id']},
                                {'$eq': ['$salt_master', '$$salt_master']},
                                {'$eq': ['$status', 'waiting']},
                            ]
                        }
                    }
                },
                {'$limit': 1},
                {'$project': {'_id': 1}},
            ],
            'as': 'unfinished_returns',
        }
    },
    {'$match': {'unfinished_returns': {'$size': 0}}},
    {'$project': {'status': {'$literal': 'failed'}, 'finished_dt': '$$NOW', 'modified': '$$NOW'}},
    {
        '$merge': {
            'into': 'task_minions',
            'on': '_id',
            'whenMatched': 'merge',
            'whenNotMatched': 'discard',
        }
    },
]


async def release_locked_task_minions() -> str:
    collection = get_mongo_db().get_collection('task_minions')

    matched_count = await collection.count_documents({'status': 'in_work'})

    if not matched_count:
        return 'Nothing to release'

    cursor = await collection.aggregate(LOCKED_TASK_MINIONS_PIPELINE)
    await cursor.to_list()

    left_count = await collection.count_documents({'status': 'in_work'})

    return f'Released {matched_count - left_count} of {matched_count} `in_work` task minions, {left_count} still busy'


class Migration(BaseMigration):
    dependencies: ClassVar[list[str]] = ['saltbox_core.tasks.migrations.0002_backfill_default_source']
    stages: ClassVar[list[BaseMigrationStage]] = [
        RunPythonMigrationStage(callback=release_locked_task_minions),
        MongoSetFieldStage(
            collection_name='task_minions',
            field_name='status',
            value='pending',
            mongo_filter={'status': 'busy'},
        ),
    ]
