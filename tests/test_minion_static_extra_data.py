import pytest

from saltbox_core.minion_collections.repositories.extra_data import ExtraDataRepository
from saltbox_core.minion_collections.repositories.extra_data_category import ExtraDataCategoryRepository
from saltbox_core.minion_collections.repositories.minion import MinionRepository
from saltbox_core.minion_collections.schemas.minion import GrainsSchema, MinionCreateSchema
from saltbox_core.minion_collections.services.minion import MinionService
from saltbox_sdk.db.mongo.schemas_base import PyObjectId
from saltbox_sdk.exceptions import ObjectNotFoundException, PermissionDeniedException
from saltbox_sdk.utilities.helpers import utc_now


def _build_minion_service(mocked_db):
    category_repo = ExtraDataCategoryRepository(mocked_db)
    extra_data_repo = ExtraDataRepository(mocked_db, extra_data_category_repository=category_repo)
    minion_repo = MinionRepository(mocked_db, extra_data_repository=extra_data_repo)

    return MinionService(minion_repo), minion_repo


async def _create_minion(minion_repo):
    return await minion_repo.create(MinionCreateSchema(minion_id='m1', master='master1', grains=GrainsSchema()))


@pytest.mark.asyncio
async def test_add_static_extra_data_item(mocked_db):
    minion_service, minion_repo = _build_minion_service(mocked_db)
    minion_id = await _create_minion(minion_repo)

    item = await minion_service.add_static_extra_data_item(minion_id, 'manual', 'notes', {'text': 'hello'})

    assert item['is_system'] is False
    assert item['data'] == {'text': 'hello'}

    stored = await minion_repo.get_static_extra_data_item(minion_id, 'manual', 'notes', item['_id'])
    assert stored is not None
    assert stored['data'] == {'text': 'hello'}


@pytest.mark.asyncio
async def test_add_static_extra_data_item_to_missing_minion(mocked_db):
    minion_service, _minion_repo = _build_minion_service(mocked_db)

    with pytest.raises(ObjectNotFoundException):
        await minion_service.add_static_extra_data_item(PyObjectId(), 'manual', 'notes', {'text': 'hello'})


@pytest.mark.asyncio
async def test_delete_manual_static_extra_data_item(mocked_db):
    minion_service, minion_repo = _build_minion_service(mocked_db)
    minion_id = await _create_minion(minion_repo)

    item = await minion_service.add_static_extra_data_item(minion_id, 'manual', 'notes', {'text': 'hello'})
    await minion_service.delete_static_extra_data_item(minion_id, 'manual', 'notes', item['_id'])

    assert await minion_repo.get_static_extra_data_item(minion_id, 'manual', 'notes', item['_id']) is None


@pytest.mark.asyncio
async def test_cannot_delete_system_item(mocked_db):
    minion_service, minion_repo = _build_minion_service(mocked_db)
    minion_id = await _create_minion(minion_repo)

    system_item = {'_id': PyObjectId(), 'is_system': True, 'updated_at': utc_now(), 'data': {'model': 'x86'}}
    await minion_repo.push_static_extra_data_item(minion_id, 'inventory', 'cpu', system_item)

    with pytest.raises(PermissionDeniedException):
        await minion_service.delete_static_extra_data_item(minion_id, 'inventory', 'cpu', system_item['_id'])

    assert await minion_repo.get_static_extra_data_item(minion_id, 'inventory', 'cpu', system_item['_id'])


@pytest.mark.asyncio
async def test_delete_missing_item_raises_not_found(mocked_db):
    minion_service, minion_repo = _build_minion_service(mocked_db)
    minion_id = await _create_minion(minion_repo)

    with pytest.raises(ObjectNotFoundException):
        await minion_service.delete_static_extra_data_item(minion_id, 'manual', 'notes', PyObjectId())
