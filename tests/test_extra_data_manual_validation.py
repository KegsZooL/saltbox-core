from datetime import UTC, datetime

import pytest

from saltbox_core.minion_collections.schemas.extra_data_category import ExtraDataCategoryModel
from saltbox_core.minion_collections.services.extra_data_category import ExtraDataCategoryService
from saltbox_sdk.db.mongo.schemas_base import PyObjectId
from saltbox_sdk.exceptions import SaltBoxValidationException


def _category(extra_fields_policy='ignore'):
    return ExtraDataCategoryModel(
        _id=PyObjectId(),
        created=datetime(2026, 9, 1, tzinfo=UTC),
        modified=datetime(2026, 9, 1, tzinfo=UTC),
        source='manual',
        name='assets',
        type='static',
        extra_fields_policy=extra_fields_policy,
        fields=[
            {'name': 'owner', 'types': ['str']},
            {'name': 'cores', 'types': ['int', 'none']},
            {'name': 'weight', 'types': ['float']},
            {'name': 'is_laptop', 'types': ['bool']},
            {'name': 'bought_at', 'types': ['datetime']},
            {'name': 'anything', 'types': []},
        ],
    )


def test_valid_data_is_cleaned():
    data = {
        'owner': 'Иванова',
        'cores': None,
        'weight': 2,
        'is_laptop': True,
        'bought_at': '2026-09-01T10:00:00Z',
        'anything': [1, 'two'],
    }

    cleaned = ExtraDataCategoryService.clean_manual_data(_category(), data)

    assert cleaned == {**data, 'bought_at': datetime(2026, 9, 1, 10, tzinfo=UTC)}


@pytest.mark.parametrize(
    ('data', 'error'),
    [
        ({'owner': 42}, '`owner`: expected str'),
        ({'cores': True}, '`cores`: expected int | none'),
        ({'cores': 2.5}, '`cores`: expected int | none'),
        ({'weight': False}, '`weight`: expected float'),
        ({'bought_at': 'вчера'}, '`bought_at`: expected datetime'),
        ({'room': '204'}, '`room`: unknown field'),
    ],
)
def test_invalid_data_is_rejected(data, error):
    with pytest.raises(SaltBoxValidationException, match=error):
        ExtraDataCategoryService.clean_manual_data(_category(), data)


def test_all_errors_are_reported_together():
    with pytest.raises(SaltBoxValidationException) as exc_info:
        ExtraDataCategoryService.clean_manual_data(_category(), {'owner': 1, 'room': '204'})

    assert '`owner`' in exc_info.value.detail
    assert '`room`' in exc_info.value.detail


@pytest.mark.parametrize('extra_fields_policy', ['save_to_category', 'save_to_minion'])
def test_unknown_fields_are_kept_when_policy_saves_them(extra_fields_policy):
    cleaned = ExtraDataCategoryService.clean_manual_data(_category(extra_fields_policy), {'room': '204'})

    assert cleaned == {'room': '204'}
