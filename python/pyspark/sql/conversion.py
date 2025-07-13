#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import array
import datetime
import decimal
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Sequence

from pyspark.errors import PySparkValueError
from pyspark.sql.pandas.types import _dedup_names, _deduplicate_field_names, to_arrow_schema
from pyspark.sql.pandas.utils import require_minimum_pyarrow_version
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DataType,
    DecimalType,
    MapType,
    NullType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    UserDefinedType,
    VariantType,
    VariantVal,
    _create_row,
)

if TYPE_CHECKING:
    import pyarrow as pa


class LocalDataToArrowConversion:
    """
    Conversion from local data (except pandas DataFrame and numpy ndarray) to Arrow.
    """

    @staticmethod
    def _need_converter(
            dataType: DataType,
            nullable: bool = True,
    ) -> bool:
        if not nullable:
            # always check the nullability
            return True
        elif isinstance(dataType, NullType):
            # always check the nullability
            return True
        elif isinstance(dataType, StructType):
            # Struct maybe rows, should convert to dict.
            return True
        elif isinstance(dataType, ArrayType):
            return LocalDataToArrowConversion._need_converter(
                dataType.elementType, dataType.containsNull
            )
        elif isinstance(dataType, MapType):
            # Different from PySpark, here always needs conversion,
            # since an Arrow Map requires a list of tuples.
            return True
        elif isinstance(dataType, BinaryType):
            return True
        elif isinstance(dataType, (TimestampType, TimestampNTZType)):
            # Always truncate
            return True
        elif isinstance(dataType, DecimalType):
            # Convert Decimal('NaN') to None
            return True
        elif isinstance(dataType, StringType):
            # Coercion to StringType is allowed
            return True
        elif isinstance(dataType, UserDefinedType):
            return True
        elif isinstance(dataType, VariantType):
            return True
        else:
            return False

    @staticmethod
    def _create_converter(
            dataType: DataType,
            nullable: bool = True,
    ) -> Optional[Callable[[Any], Any]]:
        assert dataType is not None and isinstance(dataType, DataType)
        assert isinstance(nullable, bool)

        def _converter(dt: DataType) -> Optional[Callable[[Any], Any]]:
            if isinstance(dt, NullType):

                def convert_null(value: Any) -> Any:
                    raise PySparkValueError(f"input for {dt} must be None, but got {value}")

                return convert_null

            elif isinstance(dt, StructType):
                field_names = dt.fieldNames()
                dedup_field_names = _dedup_names(dt.names)

                field_data_types = [field.dataType for field in dt.fields]
                field_nullables = [field.nullable for field in dt.fields]
                field_convs = [_converter(data_type) for data_type in field_data_types]

                def convert_struct(value: Any) -> Any:
                    assert isinstance(value, (tuple, dict)) or hasattr(
                        value, "__dict__"
                    ), f"{type(value)} {value}"

                    _dict = {}
                    if (
                            not isinstance(value, Row)
                            and not isinstance(value, tuple)  # inherited namedtuple
                            and hasattr(value, "__dict__")
                    ):
                        value = value.__dict__
                    if isinstance(value, dict):
                        for i, field in enumerate(field_names):
                            v = value.get(field)
                            if not field_nullables[i] and v is None:
                                raise PySparkValueError(
                                    f"input for {field_data_types[i]} must not be None"
                                )
                            _dict[dedup_field_names[i]] = (
                                field_convs[i](v)
                                if field_convs[i] is not None and v is not None
                                else v
                            )
                    else:
                        if len(value) != len(field_names):
                            raise PySparkValueError(
                                errorClass="AXIS_LENGTH_MISMATCH",
                                messageParameters={
                                    "expected_length": str(len(field_names)),
                                    "actual_length": str(len(value)),
                                },
                            )
                        for i in range(len(field_names)):
                            v = value[i]
                            if not field_nullables[i] and v is None:
                                raise PySparkValueError(
                                    f"input for {field_data_types[i]} must not be None"
                                )
                            _dict[dedup_field_names[i]] = (
                                field_convs[i](v)
                                if field_convs[i] is not None and v is not None
                                else v
                            )

                    return _dict

                return convert_struct

            elif isinstance(dt, ArrayType):
                element_type = dt.elementType
                element_conv = _converter(element_type)

                if dt.containsNull:

                    def convert_array(value: Any) -> Any:
                        assert isinstance(value, (list, array.array))
                        return [
                            element_conv(v) if element_conv is not None and v is not None else v
                            for v in value
                        ]

                else:

                    def convert_array(value: Any) -> Any:
                        assert isinstance(value, (list, array.array))
                        if any(v is None for v in value):
                            raise PySparkValueError(f"input for {element_type} must not be None")
                        return [element_conv(v) if element_conv is not None else v for v in value]

                return convert_array

            elif isinstance(dt, MapType):
                key_type = dt.keyType
                key_conv = _converter(key_type)
                value_type = dt.valueType
                value_conv = _converter(value_type)

                if dt.valueContainsNull:

                    def convert_map(value: Any) -> Any:
                        assert isinstance(value, dict)
                        if any(k is None for k in value.keys()):
                            raise PySparkValueError(f"input for {key_type} must not be None")
                        return [
                            (
                                key_conv(k) if key_conv is not None else k,
                                value_conv(v) if value_conv is not None and v is not None else v,
                            )
                            for k, v in value.items()
                        ]

                else:

                    def convert_map(value: Any) -> Any:
                        assert isinstance(value, dict)
                        if any(k is None for k in value.keys()):
                            raise PySparkValueError(f"input for {key_type} must not be None")
                        if any(v is None for v in value.values()):
                            raise PySparkValueError(f"input for {value_type} must not be None")
                        return [
                            (
                                key_conv(k) if key_conv is not None else k,
                                value_conv(v) if value_conv is not None else v,
                            )
                            for k, v in value.items()
                        ]

                return convert_map

            elif isinstance(dt, BinaryType):

                def convert_binary(value: Any) -> Any:
                    assert isinstance(value, (bytes, bytearray))
                    return bytes(value)

                return convert_binary

            elif isinstance(dt, TimestampType):

                def convert_timestamp(value: Any) -> Any:
                    assert isinstance(value, datetime.datetime)
                    return value.astimezone(datetime.timezone.utc)

                return convert_timestamp

            elif isinstance(dt, TimestampNTZType):

                def convert_timestamp_ntz(value: Any) -> Any:
                    assert isinstance(value, datetime.datetime) and value.tzinfo is None
                    return value

                return convert_timestamp_ntz

            elif isinstance(dt, DecimalType):

                def convert_decimal(value: Any) -> Any:
                    assert isinstance(value, decimal.Decimal)
                    return None if value.is_nan() else value

                return convert_decimal

            elif isinstance(dt, StringType):

                def convert_string(value: Any) -> Any:
                    if isinstance(value, bool):
                        # To match the PySpark Classic which convert bool to string in
                        # the JVM side (python.EvaluatePython.makeFromJava)
                        return str(value).lower()
                    else:
                        return str(value)

                return convert_string

            elif isinstance(dt, UserDefinedType):
                udt: UserDefinedType = dt

                conv = _converter(udt.sqlType())

                def convert_udt(value: Any) -> Any:
                    return conv(udt.serialize(value)) if conv is not None else udt.serialize(value)

                return convert_udt

            elif isinstance(dt, VariantType):

                def convert_variant(value: Any) -> Any:
                    if isinstance(value, VariantVal):
                        return VariantType().toInternal(value)
                    else:
                        raise PySparkValueError(errorClass="MALFORMED_VARIANT")

                return convert_variant

            else:
                return None

        conv = _converter(dataType)

        if nullable and conv is None:
            return None

        else:

            def convert(value: Any) -> Any:
                if not nullable and value is None:
                    raise PySparkValueError(f"input for {dataType} must not be None")
                else:
                    return conv(value) if conv is not None and value is not None else value

            return convert

    @staticmethod
    def convert(data: Sequence[Any], schema: StructType, use_large_var_types: bool) -> "pa.Table":
        require_minimum_pyarrow_version()
        import pyarrow as pa

        assert isinstance(data, list) and len(data) > 0

        assert schema is not None and isinstance(schema, StructType)

        column_names = schema.fieldNames()
        len_column_names = len(column_names)

        def to_row(item: Any) -> tuple:
            if item is None:
                return tuple([None for _ in range(len_column_names)])
            if isinstance(item, (Row, tuple)):
                if len(item) != len_column_names:
                    raise PySparkValueError(
                        errorClass="AXIS_LENGTH_MISMATCH",
                        messageParameters={
                            "expected_length": str(len_column_names),
                            "actual_length": str(len(item)),
                        },
                    )
                return item
            elif isinstance(item, VariantVal):
                raise PySparkValueError("Rows cannot be of type VariantVal")
            else:
                if hasattr(item, "__dict__"):
                    item = item.__dict__
                return tuple([item.get(col) for col in column_names])

        rows = [to_row(item) for item in data]

        column_convs = [
            LocalDataToArrowConversion._create_converter(field.dataType, field.nullable)
            for field in schema.fields
        ]

        pylist = [
            [
                column_convs[i](row[i])
                if column_convs[i] is not None and row[i] is not None
                else row[i]
                for row in rows
            ]
            for i in range(len_column_names)
        ]

        pa_schema = to_arrow_schema(
            StructType(
                [
                    StructField(
                        field.name, _deduplicate_field_names(field.dataType), field.nullable
                    )
                    for field in schema.fields
                ]
            ),
            prefers_large_types=use_large_var_types,
        )

        return pa.Table.from_arrays(pylist, schema=pa_schema)


class ArrowTableToRowsConversion:
    """
    Conversion from Arrow Table to Rows.
    """

    @staticmethod
    def _need_converter(dataType: DataType) -> bool:
        if isinstance(dataType, NullType):
            return True
        elif isinstance(dataType, StructType):
            return True
        elif isinstance(dataType, ArrayType):
            return ArrowTableToRowsConversion._need_converter(dataType.elementType)
        elif isinstance(dataType, MapType):
            # Different from PySpark, here always needs conversion,
            # since the input from Arrow is a list of tuples.
            return True
        elif isinstance(dataType, BinaryType):
            return True
        elif isinstance(dataType, (TimestampType, TimestampNTZType)):
            # Always remove the time zone info for now
            return True
        elif isinstance(dataType, UserDefinedType):
            return True
        elif isinstance(dataType, VariantType):
            return True
        else:
            return False

    @staticmethod
    def _create_converter(dataType: DataType) -> Callable:
        assert dataType is not None and isinstance(dataType, DataType)

        if not ArrowTableToRowsConversion._need_converter(dataType):
            return lambda value: value

        if isinstance(dataType, NullType):
            return lambda value: None

        elif isinstance(dataType, StructType):
            field_names = dataType.names
            dedup_field_names = _dedup_names(field_names)

            field_convs = [
                ArrowTableToRowsConversion._create_converter(f.dataType) for f in dataType.fields
            ]

            def convert_struct(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, dict)

                    _values = [
                        field_convs[i](value.get(name, None))
                        for i, name in enumerate(dedup_field_names)
                    ]
                    return _create_row(field_names, _values)

            return convert_struct

        elif isinstance(dataType, ArrayType):
            element_conv = ArrowTableToRowsConversion._create_converter(dataType.elementType)

            def convert_array(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, list)
                    return [element_conv(v) for v in value]

            return convert_array

        elif isinstance(dataType, MapType):
            key_conv = ArrowTableToRowsConversion._create_converter(dataType.keyType)
            value_conv = ArrowTableToRowsConversion._create_converter(dataType.valueType)

            def convert_map(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, list)
                    assert all(isinstance(t, tuple) and len(t) == 2 for t in value)
                    return dict((key_conv(t[0]), value_conv(t[1])) for t in value)

            return convert_map

        elif isinstance(dataType, BinaryType):

            def convert_binary(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, bytes)
                    return bytearray(value)

            return convert_binary

        elif isinstance(dataType, TimestampType):

            def convert_timestample(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value.astimezone().replace(tzinfo=None)

            return convert_timestample

        elif isinstance(dataType, TimestampNTZType):

            def convert_timestample_ntz(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    assert isinstance(value, datetime.datetime)
                    return value

            return convert_timestample_ntz

        elif isinstance(dataType, UserDefinedType):
            udt: UserDefinedType = dataType

            conv = ArrowTableToRowsConversion._create_converter(udt.sqlType())

            def convert_udt(value: Any) -> Any:
                if value is None:
                    return None
                else:
                    return udt.deserialize(conv(value))

            return convert_udt

        elif isinstance(dataType, VariantType):

            def convert_variant(value: Any) -> Any:
                if value is None:
                    return None
                elif (
                        isinstance(value, dict)
                        and all(key in value for key in ["value", "metadata"])
                        and all(isinstance(value[key], bytes) for key in ["value", "metadata"])
                ):
                    return VariantVal(value["value"], value["metadata"])
                else:
                    raise PySparkValueError(errorClass="MALFORMED_VARIANT")

            return convert_variant

        else:
            return lambda value: value

    @staticmethod
    def convert(table: "pa.Table", schema: StructType) -> List[Row]:
        require_minimum_pyarrow_version()
        import pyarrow as pa

        assert isinstance(table, pa.Table)

        assert schema is not None and isinstance(schema, StructType)

        field_converters = [
            ArrowTableToRowsConversion._create_converter(f.dataType) for f in schema.fields
        ]

        columnar_data = [column.to_pylist() for column in table.columns]

        rows: List[Row] = []
        for i in range(0, table.num_rows):
            values = [field_converters[j](columnar_data[j][i]) for j in range(table.num_columns)]
            rows.append(_create_row(fields=schema.fieldNames(), values=values))
        return rows
