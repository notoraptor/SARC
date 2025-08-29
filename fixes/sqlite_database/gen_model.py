import inspect
import datetime
import types
from typing import get_origin, get_args, Union
from pydantic import BaseModel
from pydantic_mongo import PydanticObjectId

from sarc.client.job import SlurmJob, SlurmState

PYTHON_TO_SQLITE: dict[type, str] = {
    int: "INTEGER",
    str: "TEXT",
    float: "REAL",
    bool: "INTEGER",
    bytes: "BLOB",
    datetime.date: "DATE",
    datetime.datetime: "DATETIME",
    list: "TEXT",  # Stored as JSON strings
    dict: "TEXT",  # Stored as JSON strings
    SlurmState: "TEXT",
}


def map_type_to_sqlite(py_type: type) -> str:
    """Maps a Python type to its SQLite equivalent."""
    return PYTHON_TO_SQLITE[py_type]  # Default to TEXT


def get_base_type(annotation: type) -> type:
    """
    Extracts the base type from a complex annotation.
    e.g., 'int | None' -> int, 'str' -> str
    """
    origin = get_origin(annotation)

    if origin is Union or origin is types.UnionType:
        args = get_args(annotation)
        non_none_args = [t for t in args if t is not type(None)]
        if len(non_none_args) == 1:
            return non_none_args[0]

    return annotation


def flatten_model_fields(
    model_class: type[BaseModel], prefix: str = ""
) -> list[tuple[str, str, bool]]:
    """
    Recursively flattens a Pydantic model.
    Returns a list of tuples: (flattened_col_name, sqlite_type, is_nullable)
    """
    columns = []

    for field_name, field_info in model_class.model_fields.items():
        col_name = f"{prefix}{field_name}"

        is_nullable = not field_info.is_required()
        base_type = get_base_type(field_info.annotation)

        # Case 1: Nested Pydantic model -> Recurse
        if inspect.isclass(base_type) and issubclass(base_type, BaseModel):
            nested_prefix = f"{col_name}_"
            nested_cols = flatten_model_fields(base_type, prefix=nested_prefix)

            if is_nullable:
                # If the whole object can be null, all its sub-fields are nullable
                columns.extend(
                    [(name, sql_type, True) for name, sql_type, _ in nested_cols]
                )
            else:
                columns.extend(nested_cols)

        # Case 2: List or Dict -> Store as JSON (TEXT)
        elif get_origin(base_type) in (list, dict):
            sql_type = map_type_to_sqlite(get_origin(base_type))
            columns.append((col_name, sql_type, is_nullable))

        elif base_type is PydanticObjectId:
            sql_type = "INTEGER"
            columns.append((col_name, sql_type, False))

        # Case 3: Simple scalar type
        else:
            sql_type = map_type_to_sqlite(base_type)
            columns.append((col_name, sql_type, is_nullable))

    return columns


# --- 4. SQL Generation Function (Language Agnostic) ---


def generate_create_table_sql(
    model_class: type[BaseModel], table_name: str | None = None
) -> str:
    """
    Generates the "CREATE TABLE" SQL statement for a Pydantic model.
    """
    if table_name is None:
        # Uses the class name (e.g., "Order")
        table_name = model_class.__name__.lower()

    flat_columns = flatten_model_fields(model_class)

    column_definitions = []
    has_pk = False

    for name, sql_type, is_nullable in flat_columns:
        # 'name' comes directly from your model definition
        definition = f'  "{name}" {sql_type}'

        # Convention: "id" field is PK
        if name == "id" and sql_type == "INTEGER" and not has_pk:
            definition += " PRIMARY KEY"
            has_pk = True
        elif not is_nullable:
            definition += " NOT NULL"

        column_definitions.append(definition)

    columns_sql = ",\n".join(column_definitions)
    return f"CREATE TABLE {table_name} (\n{columns_sql}\n);"


def main():
    print(generate_create_table_sql(SlurmJob, "slurm_job"))


if __name__ == "__main__":
    main()
