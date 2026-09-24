import importlib
from datetime import UTC, datetime

from bson import ObjectId

migration = importlib.import_module('saltbox_core.minion_collections.migrations.0002_wrap_extra_static_items')

UPDATED_AT = datetime(2026, 9, 1, tzinfo=UTC)


def test_wraps_legacy_flat_item():
    wrapped = migration.wrap_item({'model': 'x86', 'updated_at': UPDATED_AT})

    assert isinstance(wrapped['_id'], ObjectId)
    assert wrapped['is_system'] is True
    assert wrapped['updated_at'] == UPDATED_AT
    assert wrapped['data'] == {'model': 'x86'}


def test_wraps_flat_item_keeping_its_id_and_flag():
    item_id = ObjectId()

    wrapped = migration.wrap_item({'text': 'hi', '_id': str(item_id), 'is_system': False, 'updated_at': UPDATED_AT})

    assert wrapped == {'_id': item_id, 'is_system': False, 'updated_at': UPDATED_AT, 'data': {'text': 'hi'}}


def test_already_wrapped_item_is_left_alone():
    item = {'_id': ObjectId(), 'is_system': True, 'updated_at': UPDATED_AT, 'data': {'model': 'x86'}}

    assert migration.wrap_item(item) is None


def test_wrapped_item_with_string_id_gets_object_id():
    item_id = ObjectId()

    wrapped = migration.wrap_item({'_id': str(item_id), 'is_system': True, 'updated_at': UPDATED_AT, 'data': {}})

    assert wrapped['_id'] == item_id
