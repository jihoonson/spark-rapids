#!/usr/bin/env python3
#
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import os
import random
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_SCHEMA_FILE = (
    "/home/jihoons/Projects/bulk-upload-nvspark/halo_input_schema.txt"
)


@dataclass(frozen=True)
class FieldNode:
    name: str
    data_type: "DataTypeNode"

    def to_arrow(self) -> pa.Field:
        return pa.field(self.name, self.data_type.to_arrow(), nullable=True)


class DataTypeNode:
    def to_arrow(self) -> pa.DataType:
        raise NotImplementedError


@dataclass(frozen=True)
class StructNode(DataTypeNode):
    fields: tuple[FieldNode, ...]

    def to_arrow(self) -> pa.StructType:
        return pa.struct([field.to_arrow() for field in self.fields])

    def to_schema(self) -> pa.Schema:
        return pa.schema([field.to_arrow() for field in self.fields])


@dataclass(frozen=True)
class ArrayNode(DataTypeNode):
    element_type: DataTypeNode

    def to_arrow(self) -> pa.ListType:
        return pa.list_(pa.field("element", self.element_type.to_arrow(), nullable=True))


@dataclass(frozen=True)
class MapNode(DataTypeNode):
    key_type: DataTypeNode
    value_type: DataTypeNode

    def to_arrow(self) -> pa.MapType:
        return pa.map_(
            pa.field("key", self.key_type.to_arrow(), nullable=False),
            pa.field("value", self.value_type.to_arrow(), nullable=True),
        )


@dataclass(frozen=True)
class PrimitiveNode(DataTypeNode):
    name: str
    args: tuple[int, ...] = ()

    def to_arrow(self) -> pa.DataType:
        name = self.name.lower()
        if name in ("string", "char", "varchar"):
            return pa.string()
        if name in ("boolean", "bool"):
            return pa.bool_()
        if name in ("tinyint", "byte"):
            return pa.int8()
        if name in ("smallint", "short"):
            return pa.int16()
        if name in ("int", "integer"):
            return pa.int32()
        if name in ("bigint", "long"):
            return pa.int64()
        if name == "float":
            return pa.float32()
        if name == "double":
            return pa.float64()
        if name == "binary":
            return pa.binary()
        if name == "date":
            return pa.date32()
        if name in ("timestamp", "timestamp_ntz"):
            return pa.timestamp("us")
        if name == "timestamp_ltz":
            return pa.timestamp("us", tz="UTC")
        if name == "decimal":
            precision, scale = self.args
            return pa.decimal128(precision, scale)
        if name in ("void", "null"):
            return pa.null()
        raise ValueError(f"unsupported primitive type: {self.name}")


class DdlParser:
    def __init__(self, text: str):
        self.text = text
        self.pos = 0

    def parse(self) -> StructNode:
        data_type = self.parse_type()
        self.skip_ws()
        if self.pos != len(self.text):
            raise ValueError(f"unexpected trailing input at byte {self.pos}")
        if not isinstance(data_type, StructNode):
            raise ValueError("root schema must be a struct<...> type")
        return data_type

    def parse_type(self) -> DataTypeNode:
        ident = self.parse_identifier().lower()
        if ident == "struct":
            self.expect("<")
            fields = []
            self.skip_ws()
            while not self.consume(">"):
                name = self.parse_identifier()
                self.expect(":")
                fields.append(FieldNode(name, self.parse_type()))
                self.skip_ignored_nullability()
                self.skip_ws()
                if self.consume(">"):
                    break
                self.expect(",")
            return StructNode(tuple(fields))

        if ident == "array":
            self.expect("<")
            element_type = self.parse_type()
            self.expect(">")
            return ArrayNode(element_type)

        if ident == "map":
            self.expect("<")
            key_type = self.parse_type()
            self.expect(",")
            value_type = self.parse_type()
            self.expect(">")
            return MapNode(key_type, value_type)

        if ident == "decimal":
            self.expect("(")
            precision = self.parse_int()
            self.expect(",")
            scale = self.parse_int()
            self.expect(")")
            return PrimitiveNode(ident, (precision, scale))

        if ident in ("char", "varchar") and self.peek() == "(":
            self.expect("(")
            _ = self.parse_int()
            self.expect(")")
            return PrimitiveNode(ident)

        return PrimitiveNode(ident)

    def parse_identifier(self) -> str:
        self.skip_ws()
        if self.peek() == "`":
            self.pos += 1
            chars = []
            while self.pos < len(self.text):
                ch = self.text[self.pos]
                if ch == "`":
                    if self.pos + 1 < len(self.text) and self.text[self.pos + 1] == "`":
                        chars.append("`")
                        self.pos += 2
                        continue
                    self.pos += 1
                    return "".join(chars)
                chars.append(ch)
                self.pos += 1
            raise ValueError("unterminated backtick identifier")

        match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", self.text[self.pos :])
        if not match:
            raise ValueError(f"expected identifier at byte {self.pos}")
        self.pos += len(match.group(0))
        return match.group(0)

    def parse_int(self) -> int:
        self.skip_ws()
        match = re.match(r"[0-9]+", self.text[self.pos :])
        if not match:
            raise ValueError(f"expected integer at byte {self.pos}")
        self.pos += len(match.group(0))
        return int(match.group(0))

    def skip_ignored_nullability(self) -> None:
        checkpoint = self.pos
        try:
            self.skip_ws()
            if self.consume_word("not"):
                self.skip_ws()
                if not self.consume_word("null"):
                    self.pos = checkpoint
            elif self.consume_word("null"):
                pass
        except ValueError:
            self.pos = checkpoint

    def skip_ws(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def peek(self) -> Optional[str]:
        self.skip_ws()
        if self.pos >= len(self.text):
            return None
        return self.text[self.pos]

    def consume(self, expected: str) -> bool:
        self.skip_ws()
        if self.text.startswith(expected, self.pos):
            self.pos += len(expected)
            return True
        return False

    def consume_word(self, expected: str) -> bool:
        self.skip_ws()
        end = self.pos + len(expected)
        if self.text[self.pos : end].lower() != expected:
            return False
        if end < len(self.text) and re.match(r"[A-Za-z0-9_]", self.text[end]):
            return False
        self.pos = end
        return True

    def expect(self, expected: str) -> None:
        if not self.consume(expected):
            raise ValueError(f"expected {expected!r} at byte {self.pos}")


def has_named_children(data_type: DataTypeNode) -> bool:
    if isinstance(data_type, StructNode):
        return bool(data_type.fields)
    if isinstance(data_type, ArrayNode):
        return has_named_children(data_type.element_type)
    if isinstance(data_type, MapNode):
        return has_named_children(data_type.key_type) or has_named_children(data_type.value_type)
    return False


def collect_missing_candidates(
    schema: StructNode,
    min_missing_depth: int,
    max_missing_depth: int,
    prefix: tuple[str, ...] = (),
) -> dict[int, list[tuple[str, ...]]]:
    candidates = []
    for field in schema.fields:
        path = prefix + (field.name,)
        if min_missing_depth <= len(path) <= max_missing_depth and has_named_children(
            field.data_type
        ):
            candidates.append(path)
        if len(path) < max_missing_depth and isinstance(field.data_type, StructNode):
            child_candidates = collect_missing_candidates(
                field.data_type, min_missing_depth, max_missing_depth, path
            )
            for depth, paths in child_candidates.items():
                candidates.extend(paths)

    by_depth = defaultdict(list)
    for path in candidates:
        by_depth[len(path)].append(path)
    return dict(by_depth)


def is_ancestor_or_same(parent: tuple[str, ...], child: tuple[str, ...]) -> bool:
    return len(parent) <= len(child) and child[: len(parent)] == parent


def paths_overlap(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    return is_ancestor_or_same(left, right) or is_ancestor_or_same(right, left)


def remove_path(schema: StructNode, path: tuple[str, ...]) -> StructNode:
    if not path:
        return schema

    new_fields = []
    head = path[0]
    for field in schema.fields:
        if field.name != head:
            new_fields.append(field)
            continue

        if len(path) == 1:
            continue

        if not isinstance(field.data_type, StructNode):
            raise ValueError(f"cannot remove nested field below non-struct path {'.'.join(path)}")

        new_fields.append(FieldNode(field.name, remove_path(field.data_type, path[1:])))

    return StructNode(tuple(new_fields))


def remove_paths(schema: StructNode, paths: Iterable[tuple[str, ...]]) -> StructNode:
    result = schema
    for path in sorted(paths):
        result = remove_path(result, path)
    return result


def collect_schema_paths(schema: StructNode) -> set[tuple[str, ...]]:
    paths = set()
    for field in schema.fields:
        collect_field_paths(field, (), paths)
    return paths


def collect_field_paths(
    field: FieldNode, prefix: tuple[str, ...], paths: set[tuple[str, ...]]
) -> None:
    path = prefix + (field.name,)
    paths.add(path)
    collect_type_paths(field.data_type, path, paths)


def collect_type_paths(
    data_type: DataTypeNode, prefix: tuple[str, ...], paths: set[tuple[str, ...]]
) -> None:
    if isinstance(data_type, StructNode):
        for field in data_type.fields:
            collect_field_paths(field, prefix, paths)
    elif isinstance(data_type, ArrayNode):
        collect_type_paths(data_type.element_type, prefix + ("[]",), paths)
    elif isinstance(data_type, MapNode):
        collect_type_paths(data_type.key_type, prefix + ("{}", "key"), paths)
        collect_type_paths(data_type.value_type, prefix + ("{}", "value"), paths)


def merged_schema_paths(full_schema: StructNode, missing_sets: Sequence[set[tuple[str, ...]]]):
    merged = set()
    for missing_paths in missing_sets:
        merged.update(collect_schema_paths(remove_paths(full_schema, missing_paths)))
    return merged


def collect_empty_struct_paths(
    schema: StructNode, prefix: tuple[str, ...] = ()
) -> list[tuple[str, ...]]:
    empty_paths = []
    for field in schema.fields:
        path = prefix + (field.name,)
        if isinstance(field.data_type, StructNode):
            if not field.data_type.fields:
                empty_paths.append(path)
            else:
                empty_paths.extend(collect_empty_struct_paths(field.data_type, path))
    return empty_paths


def ensure_writable_file_schemas(
    full_schema: StructNode, missing_sets: list[set[tuple[str, ...]]]
) -> None:
    while True:
        changed = False
        for file_missing_paths in missing_sets:
            empty_struct_paths = collect_empty_struct_paths(
                remove_paths(full_schema, file_missing_paths)
            )
            if not empty_struct_paths:
                continue

            empty_struct_path = empty_struct_paths[0]
            direct_child_omissions = [
                path for path in file_missing_paths if path[:-1] == empty_struct_path
            ]
            descendant_omissions = [
                path
                for path in file_missing_paths
                if is_ancestor_or_same(empty_struct_path, path)
            ]
            repair_candidates = direct_child_omissions or descendant_omissions
            if not repair_candidates:
                raise ValueError(
                    "could not repair empty struct path "
                    f"{'.'.join(empty_struct_path)}"
                )
            file_missing_paths.remove(sorted(repair_candidates)[0])
            changed = True
            break

        if not changed:
            return


def ensure_merged_schema_coverage(
    full_schema: StructNode, missing_sets: list[set[tuple[str, ...]]]
) -> None:
    full_paths = collect_schema_paths(full_schema)
    while True:
        missing_paths = sorted(full_paths - merged_schema_paths(full_schema, missing_sets))
        if not missing_paths:
            return

        removed_any = False
        for missing_path in missing_paths:
            for file_missing_paths in missing_sets:
                covering_omissions = [
                    omitted
                    for omitted in sorted(file_missing_paths, reverse=True)
                    if is_ancestor_or_same(omitted, missing_path)
                ]
                if covering_omissions:
                    file_missing_paths.remove(covering_omissions[0])
                    removed_any = True
                    break
            if removed_any:
                break

        if not removed_any:
            raise ValueError(
                "could not adjust missing fields while preserving merged schema coverage"
            )


def draw_path(
    rng: random.Random,
    pool: Sequence[tuple[str, ...]],
    selected: set[tuple[str, ...]],
) -> Optional[tuple[str, ...]]:
    choices = list(pool)
    rng.shuffle(choices)
    for path in choices:
        if all(not paths_overlap(path, existing) for existing in selected):
            return path
    return None


def choose_missing_paths(
    full_schema: StructNode,
    candidates_by_depth: dict[int, list[tuple[str, ...]]],
    num_files: int,
    missing_per_file: int,
    seed: int,
) -> list[set[tuple[str, ...]]]:
    if num_files == 1:
        return [set()]
    if missing_per_file == 0:
        return [set() for _ in range(num_files)]

    candidates = [
        path for paths in candidates_by_depth.values() for path in paths
    ]
    if len(candidates) < 2:
        raise ValueError("need at least two omission candidates to preserve merged schema")

    max_missing = min(missing_per_file, len(candidates) - 1)
    rng = random.Random(seed)
    depths = sorted(candidates_by_depth)
    randomized_depths = list(depths)
    rng.shuffle(randomized_depths)

    missing_sets = []
    for file_index in range(num_files):
        selected = set()
        primary_depth = randomized_depths[file_index % len(randomized_depths)]
        allowed_depths = [primary_depth]
        if len(depths) > 1 and max_missing > 1:
            other_depths = [depth for depth in depths if depth != primary_depth]
            allowed_depths.append(rng.choice(other_depths))

        primary_path = draw_path(rng, candidates_by_depth[primary_depth], selected)
        if primary_path is not None:
            selected.add(primary_path)

        while len(selected) < max_missing:
            depth = rng.choice(allowed_depths)
            path = draw_path(rng, candidates_by_depth[depth], selected)
            if path is None:
                fallback_candidates = [
                    candidate
                    for allowed_depth in allowed_depths
                    for candidate in candidates_by_depth[allowed_depth]
                ]
                path = draw_path(rng, fallback_candidates, selected)
            if path is None:
                break
            selected.add(path)

        missing_sets.append(selected)

    common = set.intersection(*missing_sets)
    for path in common:
        missing_sets[0].remove(path)

    ensure_writable_file_schemas(full_schema, missing_sets)
    ensure_merged_schema_coverage(full_schema, missing_sets)
    return missing_sets


def make_all_null_table(schema: StructNode, rows: int) -> pa.Table:
    arrow_schema = schema.to_schema()
    arrays = [pa.nulls(rows, type=field.type) for field in arrow_schema]
    return pa.Table.from_arrays(arrays, schema=arrow_schema)


def write_manifest(
    output_path: Path,
    schema_file: str,
    num_files: int,
    rows_per_file: int,
    min_missing_depth: int,
    max_missing_depth: int,
    missing_sets: Sequence[set[tuple[str, ...]]],
) -> None:
    manifest = {
        "schema_file": schema_file,
        "num_files": num_files,
        "rows_per_file": rows_per_file,
        "missing_depth_range": [min_missing_depth, max_missing_depth],
        "files": [
            {
                "path": f"part-{file_index:05d}.parquet",
                "missing_depths": sorted({len(path) for path in paths}),
                "missing_paths": [".".join(path) for path in sorted(paths)],
            }
            for file_index, paths in enumerate(missing_sets)
        ],
    }
    with (output_path / "_schema_evolution_manifest.json").open("w", encoding="utf-8") as out:
        json.dump(manifest, out, indent=2)
        out.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate local Parquet files whose merged schema is the HALO input schema, "
            "while each individual file omits selected near-top-level non-leaf fields."
        )
    )
    parser.add_argument("output_path", help="Directory where Parquet files will be written")
    parser.add_argument("num_files", type=int, help="Number of Parquet files to create")
    parser.add_argument("rows_per_file", type=int, help="Number of rows in each Parquet file")
    parser.add_argument(
        "--schema-file",
        default=DEFAULT_SCHEMA_FILE,
        help=f"Spark DDL schema file. Default: {DEFAULT_SCHEMA_FILE}",
    )
    parser.add_argument(
        "--min-missing-depth",
        type=int,
        default=2,
        help="Minimum field-path depth to omit. Default: 2",
    )
    parser.add_argument(
        "--max-missing-depth",
        type=int,
        default=4,
        help="Maximum field-path depth to omit. Default: 4",
    )
    parser.add_argument(
        "--missing-per-file",
        type=int,
        default=6,
        help="Number of non-leaf fields to omit from each file when possible. Default: 6",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Seed used to choose omitted field paths. Default: 0",
    )
    parser.add_argument(
        "--compression",
        default="snappy",
        help="Parquet compression codec. Default: snappy",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace output_path if it already exists",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.num_files < 1:
        raise ValueError("num_files must be at least 1")
    if args.rows_per_file < 0:
        raise ValueError("rows_per_file must be non-negative")
    if args.min_missing_depth < 1:
        raise ValueError("min_missing_depth must be at least 1")
    if args.max_missing_depth < args.min_missing_depth:
        raise ValueError("max_missing_depth must be >= min_missing_depth")
    if args.missing_per_file < 0:
        raise ValueError("missing_per_file must be non-negative")


def main() -> None:
    args = parse_args()
    validate_args(args)

    schema_text = Path(os.path.expanduser(args.schema_file)).read_text(encoding="utf-8").strip()
    full_schema = DdlParser(schema_text).parse()
    candidates_by_depth = collect_missing_candidates(
        full_schema, args.min_missing_depth, args.max_missing_depth
    )
    candidates = [
        path for paths in candidates_by_depth.values() for path in paths
    ]
    if not candidates and args.missing_per_file:
        raise ValueError(
            "no non-leaf omission candidates found between depths "
            f"{args.min_missing_depth} and {args.max_missing_depth}"
        )

    missing_sets = choose_missing_paths(
        full_schema, candidates_by_depth, args.num_files, args.missing_per_file, args.seed
    )

    output_path = Path(os.path.expanduser(args.output_path))
    if output_path.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_path} already exists; pass --overwrite to replace it")
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True)

    for file_index, missing_paths in enumerate(missing_sets):
        file_schema = remove_paths(full_schema, missing_paths)
        table = make_all_null_table(file_schema, args.rows_per_file)
        pq.write_table(
            table,
            output_path / f"part-{file_index:05d}.parquet",
            compression=args.compression,
            use_deprecated_int96_timestamps=True,
        )

    write_manifest(
        output_path,
        args.schema_file,
        args.num_files,
        args.rows_per_file,
        args.min_missing_depth,
        args.max_missing_depth,
        missing_sets,
    )

    omitted_counts = [len(paths) for paths in missing_sets]
    omitted_depths = sorted({len(path) for paths in missing_sets for path in paths})
    print(f"Wrote {args.num_files} Parquet files to {output_path}")
    print(f"Rows per file: {args.rows_per_file}")
    print(f"Missing paths per file: min={min(omitted_counts)}, max={max(omitted_counts)}")
    print(f"Missing path depths used: {omitted_depths}")
    print(f"Manifest: {output_path / '_schema_evolution_manifest.json'}")


if __name__ == "__main__":
    main()
