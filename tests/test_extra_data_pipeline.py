import pytest

from saltbox_core.minion_collections.repositories.minion import MinionRepository
from saltbox_sdk.event_bus.schemas import ExtraDataCategoryType


def _build(group_by_fields):
    return MinionRepository.build_extra_data_pipeline(
        category_source='manual',
        category_name='notes',
        category_type=ExtraDataCategoryType.STATIC,
        group_by_fields=group_by_fields,
    )


@pytest.mark.parametrize('group_by_fields', [[], ['text']])
def test_grouped_pipeline_counts_minions_even_without_category_fields(group_by_fields):
    group_stages = [stage['$group'] for stage in _build(group_by_fields) if '$group' in stage]

    assert len(group_stages) == 1
    assert '_minions' in group_stages[0]


def test_ungrouped_pipeline_does_not_count_minions():
    group_stages = [stage['$group'] for stage in _build(None) if '$group' in stage]

    assert group_stages == [{'_id': '$$ROOT'}]
