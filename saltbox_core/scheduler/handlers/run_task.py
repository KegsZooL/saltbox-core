from faststream.rabbit.annotations import ContextRepo

from saltbox_core.tasks.schemas.task import TaskCreateInputSchema, TaskType
from saltbox_core.tasks.services.task import TaskService
from saltbox_sdk.db.schemas_base import SYSTEM_SHORT_USER, Source
from saltbox_sdk.scheduler.messages import RunTaskEventBusMessage


async def run_task_handler(message: RunTaskEventBusMessage, context: ContextRepo) -> dict:
    task_service: TaskService = context.get('task_service')

    task_type = message.data.get('task_type', TaskType.classic)

    if task_type == TaskType.policy:
        msg = f'Task type "{task_type}" is not supported for scheduled tasks'
        raise Exception(msg)

    task_id = await task_service.create(
        data=TaskCreateInputSchema.model_validate(
            {
                'task_type': task_type,
                'description': message.data.get('description', ''),
                'weight': message.data.get('weight', 1000),
                'requirements': message.data.get('requirements', []),
                'collection_id': message.data.get('collection_id'),
                'collection_slug': message.data.get('collection_slug'),
                'query': message.data.get('query', {}),
                'minions': message.data.get('minions', []),
                'task_template_id': message.data.get('task_template_id'),
                'fun': message.data.get('fun'),
                'data': message.data.get('data'),
                'batch_size': message.data.get('batch_size'),
                'max_jobs_count_at_same_time': message.data.get('max_jobs_count_at_same_time'),
                'max_retries': message.data.get('max_retries'),
                'retry_delay': message.data.get('retry_delay'),
                'ttl_jobs': message.data.get('ttl_jobs'),
                'ttl_task': message.data.get('ttl_task'),
                'save_pillars_as_default': message.data.get('save_pillars_as_default', False),
                'user': message.user if message.user else SYSTEM_SHORT_USER,
                'source': Source(type='scheduler', id=message.task_id),
            }
        )
    )

    await task_service.run(query=task_id)

    return {'task_id': str(task_id)}
