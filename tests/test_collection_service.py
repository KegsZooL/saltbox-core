from datetime import UTC, datetime, timedelta

import pytest

from saltbox_core.minion_collections.repositories.collection import CollectionRepository
from saltbox_core.minion_collections.schemas.collection import CollectionCreateSchema
from saltbox_core.minion_collections.services.collection import CollectionService
from saltbox_sdk.db.mongo.schemas_base import EmptyModel, PyObjectId, SortOrder


async def _create_sibling(repo, *, parent_id, slug, created):
    """Insert a sibling collection directly, bypassing auto `created`/`order` assignment,
    so tests can control creation order and reproduce the `order == 0` tie-break case."""
    document = {
        'parent_id': parent_id,
        'title': slug,
        'slug': slug,
        'description': '',
        'query': {},
        'order': 0,
        'owner_id': 'user1',
        'parent_slug': None,
        'parent_title': None,
        'created': created,
        'modified': created,
    }
    result = await repo.collection.insert_one(document=document)
    return PyObjectId(result.inserted_id)


async def _displayed_order(repo, *, parent_id) -> list:
    """Same sort used by the API to render the tree (`/tree` and `/move` responses)."""
    children = await repo.get_children(
        target=parent_id,
        sort={'order': SortOrder.ASC, 'created': SortOrder.DESC},
        projection_model=EmptyModel,
    )
    return [child.id for child in children]


@pytest.mark.asyncio
async def test_move_does_not_reorder_untouched_siblings(mocked_db):
    """Regression test: moving one collection must not change the relative order of its
    untouched siblings, even when they all share `order == 0` (e.g. right after creation).
    """
    repo = CollectionRepository(mocked_db)
    service = CollectionService(repo)

    root_id = await repo.create(CollectionCreateSchema(title='Root', slug='root', query={}, owner_id='user1'))

    base_time = datetime(2024, 1, 1, tzinfo=UTC)
    sibling_ids = {}
    for index, name in enumerate(['c1', 'c2', 'c3', 'c4', 'c5']):
        sibling_ids[name] = await _create_sibling(
            repo, parent_id=root_id, slug=name, created=base_time + timedelta(minutes=index)
        )

    displayed_before = await _displayed_order(repo, parent_id=root_id)
    # Ties are broken by `created` DESC, so the newest sibling is displayed first.
    assert displayed_before == [sibling_ids[name] for name in ['c5', 'c4', 'c3', 'c2', 'c1']]

    untouched_before = [doc_id for doc_id in displayed_before if doc_id != sibling_ids['c1']]

    await service.move(target_id=sibling_ids['c1'], parent_id=root_id, insert_before_id=sibling_ids['c4'])

    displayed_after = await _displayed_order(repo, parent_id=root_id)
    untouched_after = [doc_id for doc_id in displayed_after if doc_id != sibling_ids['c1']]

    assert untouched_after == untouched_before
    assert displayed_after == [sibling_ids[name] for name in ['c5', 'c1', 'c4', 'c3', 'c2']]
