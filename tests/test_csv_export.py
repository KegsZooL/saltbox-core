import csv
import io
from datetime import UTC, datetime

import pytest

from saltbox_sdk.fastapi_utils.csv_export import iter_csv, to_csv_value


async def _rows(rows):
    for row in rows:
        yield row


async def _collect(columns, rows, chunk_rows=1000):
    return [chunk async for chunk in iter_csv(columns, _rows(rows), chunk_rows=chunk_rows)]


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        (None, ''),
        (42, 42),
        (True, True),
        ('Бухгалтерия', 'Бухгалтерия'),
        (datetime(2026, 9, 24, 10, tzinfo=UTC), '2026-09-24T10:00:00+00:00'),
        ({'cores': 4, 'name': 'Xeon'}, '{"cores": 4, "name": "Xeon"}'),
        (['ssd', 'диск'], '["ssd", "диск"]'),
        ('=HYPERLINK("http://evil")', '\'=HYPERLINK("http://evil")'),
        ('+7 (495) 123-45-67', "'+7 (495) 123-45-67"),
        ('-1', "'-1"),
        ('@SUM(A1)', "'@SUM(A1)"),
    ],
)
def test_to_csv_value(value, expected):
    assert to_csv_value(value) == expected


@pytest.mark.asyncio
async def test_iter_csv_writes_header_and_rows_by_column_keys():
    columns = [('owner', 'owner'), ('minions_count', '_minions_count')]
    rows = [{'owner': 'Иванова', '_minions_count': 2, 'ignored': 'x'}, {'_minions_count': 1}]

    content = ''.join(await _collect(columns, rows))

    assert content.startswith('\ufeff')
    assert list(csv.reader(io.StringIO(content.removeprefix('\ufeff')))) == [
        ['owner', 'minions_count'],
        ['Иванова', '2'],
        ['', '1'],
    ]


@pytest.mark.asyncio
async def test_iter_csv_yields_in_chunks():
    rows = [{'n': i} for i in range(5)]

    chunks = await _collect([('n', 'n')], rows, chunk_rows=2)

    assert chunks[0] == '\ufeffn\r\n'
    assert len(chunks) == 4
    assert list(csv.reader(io.StringIO(''.join(chunks[1:])))) == [['0'], ['1'], ['2'], ['3'], ['4']]


@pytest.mark.asyncio
async def test_iter_csv_without_rows_yields_header_only():
    assert ''.join(await _collect([('n', 'n')], [])) == '\ufeffn\r\n'
