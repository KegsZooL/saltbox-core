import re
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, ClassVar, cast, overload

import pymongo
from fastapi import Depends
from pymongo.asynchronous.client_session import AsyncClientSession as MongoAsyncClientSession
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.database import AsyncDatabase as MongoAsyncDatabase
from pymongo.operations import _IndexKeyHint

from saltbox_core.minion_collections.repositories.extra_data import ExtraDataRepository, get_extra_data_repository
from saltbox_core.minion_collections.schemas.minion import MinionModel
from saltbox_sdk.db.mongo.aggregations import (
    AddFieldsAggregationStage,
    AggregatedField,
    AggregationsStore,
    GroupAggregationStage,
    LookupAggregationStage,
    ProjectAggregationStage,
    UnsetAggregationStage,
)
from saltbox_sdk.db.mongo.config import get_mongo
from saltbox_sdk.db.mongo.repository_base import BaseMongoRepository, ProjectionModel
from saltbox_sdk.db.mongo.schemas_base import PyObjectId, SortOrder
from saltbox_sdk.event_bus.schemas import ExtraDataCategoryType


class MinionRepository(BaseMongoRepository[MinionModel]):
    def __init__(
        self, database: MongoAsyncDatabase[Any], extra_data_repository: ExtraDataRepository, **kwargs: Any
    ) -> None:
        super().__init__(database=database)
        self.extra_data_repository = extra_data_repository

    async def last_activity_seconds_query_override(
        self, field_name: str, field_match: re.Match, field_value: Any, full_raw_query: dict
    ) -> dict[str, Any]:
        field_name_override = 'last_activity'

        if isinstance(field_value, dict):
            lookup = cast(str, next(iter(field_value)))
            value = field_value[lookup]

            if lookup in ['$in', '$nin']:
                return {
                    field_name_override: {
                        lookup: [datetime.now(UTC) - timedelta(seconds=float(item_val)) for item_val in value]
                    }
                }
            else:
                lookup = {'$lt': '$gt', '$lte': '$gte', '$gt': '$lt', '$gte': '$lte'}.get(lookup, lookup)
                return {field_name_override: {lookup: datetime.now(UTC) - timedelta(seconds=float(value))}}
        else:
            return {field_name_override: datetime.now(UTC) - timedelta(seconds=float(field_value))}

    async def extra_static_query_override(
        self, field_name: str, field_match: re.Match, field_value: Any, full_raw_query: dict
    ) -> dict[str, Any]:
        source: str = field_match.group('source')
        name: str = field_match.group('name')
        sub_field: str = field_match.group('sub_field')

        return {f'extra_static.{source}.{name}.{sub_field}': field_value}

    async def extra_aggregated_query_override(
        self, field_name: str, field_match: re.Match, field_value: Any, full_raw_query: dict
    ) -> dict[str, Any]:
        source: str = field_match.group('source')
        name: str = field_match.group('name')
        sub_field: str = field_match.group('sub_field')

        minion_ids = await self.extra_data_repository.get_minion_ids_by_filter(
            source=source, name=name, query={sub_field: field_value}
        )

        return {'_id': {'$in': minion_ids}}

    async def extra_query_override(
        self, field_name: str, field_match: re.Match, field_value: Any, full_raw_query: dict
    ) -> dict[str, Any]:
        source: str = field_match.group('source')
        name: str = field_match.group('name')

        category = await self.extra_data_repository.extra_data_category_repository.get({'source': source, 'name': name})

        if category.type == ExtraDataCategoryType.STATIC:
            return await self.extra_static_query_override(
                field_name=field_name, field_match=field_match, field_value=field_value, full_raw_query=full_raw_query
            )
        elif category.type == ExtraDataCategoryType.AGGREGATED:
            return await self.extra_aggregated_query_override(
                field_name=field_name, field_match=field_match, field_value=field_value, full_raw_query=full_raw_query
            )

        raise KeyError

    @overload
    async def get_by_master_and_id(self, master: str, minion_id: str) -> MinionModel: ...

    @overload
    async def get_by_master_and_id(
        self, master: str, minion_id: str, projection_model: type[ProjectionModel]
    ) -> ProjectionModel: ...

    async def get_by_master_and_id(
        self, master: str, minion_id: str, projection_model: type[ProjectionModel] | None = None
    ) -> ProjectionModel | MinionModel:
        query = {'master': master, 'minion_id': minion_id}

        if projection_model:
            return await self.get(query=query, projection_model=projection_model)
        else:
            return await self.get(query=query)

    async def get_ids(
        self, query: dict[str, Any], *, session: MongoAsyncClientSession | None = None
    ) -> list[PyObjectId]:
        docs = await self.collection.find(filter=query, projection={'_id': 1}, session=session).to_list()
        return [doc['_id'] for doc in docs]

    @staticmethod
    def build_extra_data_pipeline(
        *,
        query: dict[str, Any] | None = None,
        category_source: str,
        category_name: str,
        category_type: ExtraDataCategoryType,
        search_str: str | None = None,
        escape_search_str: bool = True,
        group_by_fields: list[str] | None = None,
        field_names: list[str] | None = None,
        limit: int = 0,
        skip: int = 0,
        sort: dict[str, SortOrder] | None = None,
    ) -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = []

        if query:
            pipeline.append({'$match': query})

        source_input = {
            '$filter': {
                'input': {'$objectToArray': {'$ifNull': ['$extra_static', {}]}},
                'as': 'src_pair',
                'cond': {'$eq': ['$$src_pair.k', category_source]},
            }
        }
        name_input = {
            '$filter': {
                'input': {'$objectToArray': '$$categories'},
                'as': 'cat_pair',
                'cond': {'$eq': ['$$cat_pair.k', category_name]},
            }
        }

        entry_filter = {'$filter': {'input': '$minions', 'as': 'm', 'cond': {'$eq': ['$$m.minion_id', '$$mid']}}}
        merge_with_meta = {'$mergeObjects': ['$data', '$_entry.data', {'_source': '$source', '_name': '$name'}]}
        lookup_pipeline: list[dict[str, Any]] = [
            {'$match': {'source': category_source, 'name': category_name}},
            {'$addFields': {'_entry': {'$first': entry_filter}}},
            {'$replaceRoot': {'newRoot': merge_with_meta}},
        ]

        static_map = {
            '$map': {
                'input': '$$this.v',
                'as': 'it',
                'in': {'$mergeObjects': ['$$it', {'_source': '$$src_name', '_name': '$$this.k'}]},
            }
        }
        inner_reduce = {
            '$reduce': {'input': name_input, 'initialValue': [], 'in': {'$concatArrays': ['$$value', static_map]}}
        }
        outer_reduce = {
            '$reduce': {
                'input': source_input,
                'initialValue': [],
                'in': {
                    '$let': {
                        'vars': {'src_name': '$$this.k', 'categories': '$$this.v'},
                        'in': {'$concatArrays': ['$$value', inner_reduce]},
                    }
                },
            }
        }

        skip_lookup = category_type == ExtraDataCategoryType.STATIC

        if skip_lookup:
            pipeline.extend(
                [
                    {'$addFields': {'_static_items': outer_reduce}},
                    {'$project': {'_id': 1 if group_by_fields else 0, 'items': '$_static_items'}},
                    {'$unwind': '$items'},
                ]
            )
        else:
            pipeline.extend(
                [
                    {
                        '$lookup': {
                            'from': 'minion_extra_data',
                            'localField': '_id',
                            'foreignField': 'minions.minion_id',
                            'let': {'mid': '$_id'},
                            'pipeline': lookup_pipeline,
                            'as': '_aggregated_items',
                        }
                    },
                    {'$addFields': {'_static_items': outer_reduce}},
                    {
                        '$project': {
                            '_id': 1 if group_by_fields else 0,
                            'items': {'$concatArrays': ['$_static_items', '$_aggregated_items']},
                        }
                    },
                    {'$unwind': '$items'},
                ]
            )

        if group_by_fields:
            group_key: dict[str, Any] = {field: {'$ifNull': [f'$items.{field}', None]} for field in group_by_fields}
            group_key['_source'] = '$items._source'
            group_key['_name'] = '$items._name'

            pipeline.extend(
                [
                    {'$group': {'_id': group_key, '_minions': {'$addToSet': '$_id'}}},
                    {
                        '$replaceRoot': {
                            'newRoot': {'$mergeObjects': ['$_id', {'_minions_count': {'$size': '$_minions'}}]}
                        }
                    },
                ]
            )
        else:
            pipeline.extend(
                [
                    {'$replaceRoot': {'newRoot': '$items'}},
                    {'$group': {'_id': '$$ROOT'}},
                    {'$replaceRoot': {'newRoot': '$_id'}},
                ]
            )

        if search_str:
            kv_value = {'$convert': {'input': '$$kv.v', 'to': 'string', 'onError': '', 'onNull': ''}}
            regex = re.escape(search_str) if escape_search_str else search_str
            search_match = {'$regexMatch': {'input': kv_value, 'regex': regex, 'options': 'i'}}
            pipeline.append(
                {
                    '$match': {
                        '$expr': {
                            '$anyElementTrue': {
                                '$map': {'input': {'$objectToArray': '$$ROOT'}, 'as': 'kv', 'in': search_match}
                            }
                        }
                    }
                }
            )

        full_sort = {'_source': SortOrder.ASC, '_name': SortOrder.ASC, **(sort or {})}
        tiebreaker_fields = group_by_fields or field_names or []
        full_sort.update({field: SortOrder.ASC for field in tiebreaker_fields if field not in full_sort})

        data_branch: list[dict[str, Any]] = [{'$sort': full_sort}]
        if skip:
            data_branch.append({'$skip': skip})
        if limit:
            data_branch.append({'$limit': limit})

        pipeline.append({'$facet': {'total': [{'$count': 'total'}], 'data': data_branch}})

        return pipeline

    async def get_extra_data_list_paginated(
        self,
        *,
        query: dict[str, Any] | None = None,
        category_source: str,
        category_name: str,
        category_type: ExtraDataCategoryType,
        search_str: str | None = None,
        escape_search_str: bool = True,
        group_by_fields: list[str] | None = None,
        field_names: list[str] | None = None,
        limit: int = 0,
        skip: int = 0,
        sort: dict[str, SortOrder] | None = None,
        session: MongoAsyncClientSession | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        pipeline = self.build_extra_data_pipeline(
            query=query,
            category_source=category_source,
            category_name=category_name,
            category_type=category_type,
            search_str=search_str,
            escape_search_str=escape_search_str,
            group_by_fields=group_by_fields,
            field_names=field_names,
            limit=limit,
            skip=skip,
            sort=sort,
        )
        cursor = await self.collection.aggregate(pipeline=pipeline, session=session, allowDiskUse=True)
        facet_result = await cursor.to_list()
        facet = facet_result[0] if facet_result else {'total': [], 'data': []}
        total = facet['total'][0]['total'] if facet['total'] else 0

        return total, facet['data']

    class Meta:
        collection_name = 'minions'
        auto_now_add_fields: ClassVar[list[str]] = ['created']
        auto_now_fields: ClassVar[list[str]] = ['modified']
        query_overrides: ClassVar[dict[re.Pattern, str]] = {
            re.compile(r'^last_activity_seconds$'): 'last_activity_seconds_query_override',
            re.compile(
                r'^extra\.static\.(?P<source>[^.]+)\.(?P<name>[^.]+)\.(?P<sub_field>.+)'
            ): 'extra_static_query_override',
            re.compile(
                r'^extra\.aggregated\.(?P<source>[^.]+)\.(?P<name>[^.]+)\.(?P<sub_field>.+)$'
            ): 'extra_aggregated_query_override',
            re.compile(r'^extra\.(?P<source>[^.]+)\.(?P<name>[^.]+)\.(?P<sub_field>.+)$'): 'extra_query_override',
        }
        collection_index_to_keys: ClassVar[dict[str, _IndexKeyHint]] = {
            'minion_id_master_unique_index_asc': [('minion_id', pymongo.ASCENDING), ('master', pymongo.ASCENDING)],
            'created_asc': [('created', pymongo.ASCENDING)],
            'last_activity_asc': [('last_activity', pymongo.ASCENDING)],
            'grains_wildcard': [('grain.$**', pymongo.ASCENDING)],
            'extra_static_wildcard': [('extra_static.$**', pymongo.ASCENDING)],
        }
        aggregations: ClassVar[AggregationsStore] = AggregationsStore(
            aggregations=[
                AggregatedField(
                    field_name='extra_aggregated',
                    stages=[
                        LookupAggregationStage(
                            from_collection='minion_extra_data',
                            local_field='_id',
                            foreign_field='minions.minion_id',
                            let={'minion_id': '$_id'},
                            pipeline=[
                                AddFieldsAggregationStage(
                                    fields={
                                        '_minion_entry': {
                                            '$first': {
                                                '$filter': {
                                                    'input': '$minions',
                                                    'as': 'm',
                                                    'cond': {'$eq': ['$$m.minion_id', '$$minion_id']},
                                                }
                                            }
                                        }
                                    }
                                ),
                                AddFieldsAggregationStage(
                                    fields={'_merged_value': {'$mergeObjects': ['$data', '$_minion_entry.data']}}
                                ),
                                GroupAggregationStage(
                                    group_id={'source': '$source', 'name': '$name'},
                                    fields={
                                        'values': {'$push': '$_merged_value'},
                                    },
                                ),
                                GroupAggregationStage(
                                    group_id='$_id.source',
                                    fields={
                                        'names': {'$push': {'k': '$_id.name', 'v': '$values'}},
                                    },
                                ),
                                ProjectAggregationStage(
                                    fields={'_id': 0, 'k': '$_id', 'v': {'$arrayToObject': '$names'}}
                                ),
                            ],
                            as_field='_extra_grouped',
                        ),
                        AddFieldsAggregationStage(fields={'extra_aggregated': {'$arrayToObject': '$_extra_grouped'}}),
                        UnsetAggregationStage(fields=['_extra_grouped']),
                    ],
                ),
                AggregatedField(
                    field_name='extra',
                    stages=[
                        AddFieldsAggregationStage(
                            fields={
                                'extra': {
                                    '$arrayToObject': {
                                        '$reduce': {
                                            'input': {
                                                '$concatArrays': [
                                                    {'$objectToArray': {'$ifNull': ['$extra_static', {}]}},
                                                    {'$objectToArray': '$extra_aggregated'},
                                                ]
                                            },
                                            'initialValue': [],
                                            'in': {
                                                '$let': {
                                                    'vars': {'idx': {'$indexOfArray': ['$$value.k', '$$this.k']}},
                                                    'in': {
                                                        '$cond': [
                                                            {'$eq': ['$$idx', -1]},
                                                            {'$concatArrays': ['$$value', ['$$this']]},
                                                            {
                                                                '$concatArrays': [
                                                                    {'$slice': ['$$value', '$$idx']},
                                                                    [
                                                                        {
                                                                            'k': '$$this.k',
                                                                            'v': {
                                                                                '$mergeObjects': [
                                                                                    {
                                                                                        '$arrayElemAt': [
                                                                                            '$$value.v',
                                                                                            '$$idx',
                                                                                        ]
                                                                                    },
                                                                                    '$$this.v',
                                                                                ]
                                                                            },
                                                                        }
                                                                    ],
                                                                    {
                                                                        '$slice': [
                                                                            '$$value',
                                                                            {'$add': ['$$idx', 1]},
                                                                            {'$size': '$$value'},
                                                                        ]
                                                                    },
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                }
                                            },
                                        }
                                    }
                                }
                            }
                        )
                    ],
                    parent_aggregations=['extra_aggregated'],
                ),
            ],
        )


def get_minion_repository(
    db: Annotated[AsyncDatabase, Depends(get_mongo)],
    extra_data_repository: Annotated[ExtraDataRepository, Depends(get_extra_data_repository)],
) -> MinionRepository:
    return MinionRepository(database=db, extra_data_repository=extra_data_repository)
