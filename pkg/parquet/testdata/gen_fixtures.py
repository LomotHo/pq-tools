#!/usr/bin/env python3
"""Generate parquet test fixtures with various schema complexities."""

import pyarrow as pa
import pyarrow.parquet as pq
import os

OUTDIR = os.path.dirname(os.path.abspath(__file__))


def write(name, table, **kwargs):
    path = os.path.join(OUTDIR, name)
    pq.write_table(table, path, **kwargs)
    f = pq.ParquetFile(path)
    print(f"  {name}: {f.metadata.num_rows} rows, {f.metadata.num_columns} cols, {os.path.getsize(path)} bytes")


def gen_flat():
    schema = pa.schema([
        ("id", pa.string()),
        ("name", pa.string()),
        ("age", pa.int32()),
        ("score", pa.float64()),
        ("active", pa.bool_()),
    ])
    data = {
        "id": [f"id_{i}" for i in range(100)],
        "name": [f"user_{i}" for i in range(100)],
        "age": [20 + i % 50 for i in range(100)],
        "score": [i * 0.1 for i in range(100)],
        "active": [i % 2 == 0 for i in range(100)],
    }
    write("flat.parquet", pa.table(data, schema=schema))


def gen_nested_struct():
    schema = pa.schema([
        ("id", pa.string()),
        ("info", pa.struct([
            ("name", pa.string()),
            ("address", pa.struct([
                ("city", pa.string()),
                ("zip", pa.string()),
            ])),
        ])),
    ])
    rows = []
    for i in range(50):
        rows.append({
            "id": f"id_{i}",
            "info": {
                "name": f"user_{i}",
                "address": {"city": f"city_{i % 5}", "zip": f"{10000 + i}"},
            },
        })
    ids = [r["id"] for r in rows]
    infos = [r["info"] for r in rows]
    write("nested_struct.parquet", pa.table({"id": ids, "info": infos}, schema=schema))


def gen_list_primitive():
    schema = pa.schema([
        ("id", pa.string()),
        ("tags", pa.list_(pa.string())),
        ("scores", pa.list_(pa.float32())),
    ])
    data = {
        "id": [f"id_{i}" for i in range(50)],
        "tags": [[f"tag_{j}" for j in range(i % 4 + 1)] for i in range(50)],
        "scores": [[float(j) * 0.1 for j in range(i % 3 + 1)] for i in range(50)],
    }
    write("list_primitive.parquet", pa.table(data, schema=schema))


def gen_list_struct():
    schema = pa.schema([
        ("id", pa.string()),
        ("items", pa.list_(pa.struct([
            ("name", pa.string()),
            ("value", pa.int64()),
        ]))),
    ])
    data = {
        "id": [f"id_{i}" for i in range(50)],
        "items": [
            [{"name": f"item_{j}", "value": j * 10} for j in range(i % 3 + 1)]
            for i in range(50)
        ],
    }
    write("list_struct.parquet", pa.table(data, schema=schema))


def gen_map_simple():
    schema = pa.schema([
        ("id", pa.string()),
        ("props", pa.map_(pa.string(), pa.string())),
        ("metrics", pa.map_(pa.string(), pa.int64())),
    ])
    data = {
        "id": [f"id_{i}" for i in range(50)],
        "props": [
            {f"key_{j}": f"val_{j}" for j in range(i % 3 + 1)}
            for i in range(50)
        ],
        "metrics": [
            {f"m_{j}": j * 100 for j in range(i % 2 + 1)}
            for i in range(50)
        ],
    }
    write("map_simple.parquet", pa.table(data, schema=schema))


def gen_map_nested():
    schema = pa.schema([
        ("id", pa.string()),
        ("data", pa.map_(pa.string(), pa.struct([
            ("score", pa.float64()),
            ("label", pa.string()),
        ]))),
    ])
    data = {
        "id": [f"id_{i}" for i in range(50)],
        "data": [
            {f"k_{j}": {"score": float(j), "label": f"lbl_{j}"} for j in range(i % 3 + 1)}
            for i in range(50)
        ],
    }
    write("map_nested.parquet", pa.table(data, schema=schema))


def gen_deeply_nested():
    """Mimics the real cc-pipeline parquet schema structure."""
    schema = pa.schema([
        ("id", pa.string()),
        ("data", pa.list_(pa.struct([
            ("meta", pa.struct([
                ("url", pa.string()),
                ("host", pa.string()),
                ("language", pa.string()),
                ("scores", pa.map_(pa.string(), pa.float64())),
                ("pipelines", pa.list_(pa.struct([
                    ("module", pa.string()),
                    ("version", pa.string()),
                ]))),
                ("extra_map", pa.map_(pa.string(), pa.struct([
                    ("alt_text", pa.string()),
                    ("width", pa.int32()),
                    ("height", pa.int32()),
                ]))),
            ])),
            ("text", pa.string()),
            ("images", pa.list_(pa.struct([
                ("url", pa.string()),
                ("name", pa.string()),
                ("meta", pa.struct([
                    ("width", pa.int32()),
                    ("height", pa.int32()),
                    ("tags", pa.map_(pa.string(), pa.float32())),
                ])),
            ]))),
        ]))),
        ("trace_id", pa.string()),
        ("source", pa.string()),
    ])
    data = {
        "id": [f"id_{i}" for i in range(20)],
        "data": [
            [{
                "meta": {
                    "url": f"https://example.com/{i}",
                    "host": "example.com",
                    "language": "en",
                    "scores": {"quality": 0.8 + i * 0.01, "relevance": 0.5},
                    "pipelines": [
                        {"module": "extract", "version": "1.0"},
                        {"module": "clean", "version": "2.1"},
                    ],
                    "extra_map": {
                        "img1": {"alt_text": "photo", "width": 800, "height": 600},
                    },
                },
                "text": f"Sample text content for document {i}. " * 3,
                "images": [
                    {
                        "url": f"https://img.example.com/{i}_0.jpg",
                        "name": f"image_{i}_0",
                        "meta": {"width": 640, "height": 480, "tags": {"quality": 0.9}},
                    },
                ],
            }]
            for i in range(20)
        ],
        "trace_id": [f"trace_{i}" for i in range(20)],
        "source": ["test"] * 20,
    }
    write("deeply_nested.parquet", pa.table(data, schema=schema))


def gen_nullable():
    schema = pa.schema([
        ("id", pa.string()),
        ("name", pa.string()),
        ("age", pa.int32()),
        ("score", pa.float64()),
        ("tags", pa.list_(pa.string())),
        ("info", pa.struct([("city", pa.string()), ("zip", pa.string())])),
    ])
    data = {
        "id": ["a", "b", "c", "d", "e"],
        "name": ["Alice", None, "Charlie", None, "Eve"],
        "age": [25, None, 30, 35, None],
        "score": [1.0, 2.0, None, None, 5.0],
        "tags": [["a", "b"], None, ["c"], None, []],
        "info": [
            {"city": "NYC", "zip": "10001"},
            None,
            {"city": None, "zip": "30001"},
            None,
            {"city": "LA", "zip": None},
        ],
    }
    write("nullable.parquet", pa.table(data, schema=schema))


def gen_multi_rowgroup():
    schema = pa.schema([
        ("id", pa.string()),
        ("value", pa.int64()),
    ])
    path = os.path.join(OUTDIR, "multi_rowgroup.parquet")
    writer = pq.ParquetWriter(path, schema)
    for group in range(3):
        offset = group * 30
        t = pa.table({
            "id": [f"id_{offset + i}" for i in range(30)],
            "value": [offset + i for i in range(30)],
        }, schema=schema)
        writer.write_table(t)
    writer.close()
    f = pq.ParquetFile(path)
    print(f"  multi_rowgroup.parquet: {f.metadata.num_rows} rows, {f.metadata.num_row_groups} row groups, {os.path.getsize(path)} bytes")


def gen_empty():
    schema = pa.schema([
        ("id", pa.string()),
        ("value", pa.int64()),
    ])
    write("empty.parquet", pa.table({"id": pa.array([], type=pa.string()), "value": pa.array([], type=pa.int64())}, schema=schema))


def gen_large():
    n = 10000
    schema = pa.schema([
        ("id", pa.string()),
        ("name", pa.string()),
        ("age", pa.int32()),
        ("score", pa.float64()),
        ("active", pa.bool_()),
    ])
    data = {
        "id": [f"id_{i}" for i in range(n)],
        "name": [f"user_{i}" for i in range(n)],
        "age": [20 + i % 50 for i in range(n)],
        "score": [i * 0.01 for i in range(n)],
        "active": [i % 2 == 0 for i in range(n)],
    }
    write("large.parquet", pa.table(data, schema=schema))


if __name__ == "__main__":
    print("Generating test fixtures...")
    gen_flat()
    gen_nested_struct()
    gen_list_primitive()
    gen_list_struct()
    gen_map_simple()
    gen_map_nested()
    gen_deeply_nested()
    gen_nullable()
    gen_multi_rowgroup()
    gen_empty()
    gen_large()
    print("Done.")
