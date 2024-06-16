import boto3
import json
import re
from typing import List

from datacontract.model.data_contract_specification import (
    DataContractSpecification,
    Model,
    Field,
    Server,
)


def get_glue_database(datebase_name: str):
    """Get the details Glue database.

    Args:
        database_name (str): glue database to request.

    Returns:
        set: catalogid and locationUri
    """

    glue = boto3.client("glue")
    try:
        response = glue.get_database(Name=datebase_name)
    except glue.exceptions.EntityNotFoundException:
        print(f"Database not found {datebase_name}.")
        return (None, None)
    except Exception as e:
        # todo catch all
        print(f"Error: {e}")
        return (None, None)

    return (response["Database"]["CatalogId"], response["Database"].get("LocationUri", "None"))


def get_glue_tables(database_name: str) -> List[str]:
    """Get the list of tables in a Glue database.

    Args:
        database_name (str): glue database to request.

    Returns:
        List[string]: List of table names
    """

    glue = boto3.client("glue")

    # Set the paginator
    paginator = glue.get_paginator("get_tables")

    # Initialize an empty list to store the table names
    table_names = []
    try:
        # Paginate through the tables
        for page in paginator.paginate(DatabaseName=database_name, PaginationConfig={"PageSize": 100}):
            # Add the tables from the current page to the list
            table_names.extend([table["Name"] for table in page["TableList"] if "Name" in table])
    except glue.exceptions.EntityNotFoundException:
        print(f"Database {database_name} not found.")
        return []
    except Exception as e:
        # todo catch all
        print(f"Error: {e}")
        return []

    return table_names


def get_glue_table_schema(database_name: str, table_name: str):
    """Get the schema of a Glue table.

    Args:
        database_name (str): Glue database name.
        table_name (str): Glue table name.

    Returns:
        dict: Table schema
    """

    glue = boto3.client("glue")

    # Get the table schema
    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
    except glue.exceptions.EntityNotFoundException:
        print(f"Table {table_name} not found in database {database_name}.")
        return {}
    except Exception as e:
        # todo catch all
        print(f"Error: {e}")
        return {}

    table_schema = response["Table"]["StorageDescriptor"]["Columns"]

    # when using hive partition keys, the schema is stored in the PartitionKeys field
    if response["Table"].get("PartitionKeys") is not None:
        for pk in response["Table"]["PartitionKeys"]:
            table_schema.append(
                {
                    "Name": pk["Name"],
                    "Type": pk["Type"],
                    "Hive": True,
                    "Comment": "Partition Key",
                }
            )

    table_schema = format_schema(table_schema)

    return table_schema


def generate_sub_fields(original_type_field):
    iteration_count = original_type_field.count("<")

    for i in range(iteration_count):

        index_opening_sub_element = original_type_field.rfind("<")

        sub_text = original_type_field[index_opening_sub_element:]

        index_closing_sub_element = index_opening_sub_element + sub_text.find(">")

        content = original_type_field[index_opening_sub_element + 1 :index_closing_sub_element]
        prev_content = original_type_field[:index_opening_sub_element]

        index_start_end_content = index_closing_sub_element + 1
        end_content = original_type_field[index_start_end_content:]

        sub_content_type = re.findall(r'\b\w+\b', prev_content)[-1]


        if sub_content_type == "struct":
            new_prev_content = prev_content[:-6]

            fields_from_string = content.split(",")

            fields = "["

            last_item_index = len(fields_from_string) - 1

            for x, field in enumerate(fields_from_string):
                count_caracter = field.count(':')
                if count_caracter == 1:
                    sub_filed = field.split(':')
                    fileds_to_add = f"{{'Name': '{sub_filed[0]}'|'Type': '{sub_filed[1]}'}}"
                else:
                    name_position = field.find(':')
                    name = field[:name_position]
                    new_field = field[name_position + 1:]

                    fileds_to_add = f"{{'Name':'{name}'|{new_field}}}"
                
                fields += fileds_to_add + "|" if x < last_item_index else fileds_to_add
            
            fields += "]"


            original_type_field = f"{new_prev_content}'Type':'struct'|'Fields':{fields}{end_content}"

        if sub_content_type == "array":
            new_prev_content = prev_content[:-5]
            if content.startswith("'Type'"):
                original_type_field = f"{new_prev_content}'Type':'array'|'Items':{{{content}}}{end_content}"
            else:
                original_type_field = f"{new_prev_content}'Type':'array'|'Items':{{'Type':'{content}'}}{end_content}"

    original_type_field = original_type_field.replace('|', ',').replace("'",'"')

    original_type_field = f"{{{original_type_field}}}"

    return json.loads(original_type_field)


def format_schema(schema):
    for column in schema:
        if column['Type'].startswith("array<"):
            items = generate_sub_fields(column['Type'])['Items']
            column['Items'] = items
            column['Type'] = "array"
        elif column['Type'].startswith("struct<"):
            fields = generate_sub_fields(column['Type'])['Fields']
            column['Fields'] = fields
            column['Type'] = "struct"
    
    return schema


def import_record_fields(record_fields):
    imported_fields = {}
    for field in record_fields:

        imported_field = Field()

        field_type = map_type_from_sql(field.get("Type"))

        if field.get("Hive"):
            imported_field.required = True
        else:
            imported_field.required = False
        
        imported_field.description = field.get("Comment")

        if field_type == "struct":
            imported_field.type = field_type
            imported_field.fields = import_record_fields(field.get("Fields"))
        elif field_type == "array":
            imported_field.type = field_type
            imported_field.items = import_array_field(field.get("Items"))
        else:
            imported_field.type = field_type

        imported_fields[field.get("Name")] = imported_field

    return imported_fields


def import_array_field(array_field):
    items = Field()
    field_type = map_type_from_sql(array_field.get("Type"))

    if field_type == "struct":
        items.type = field_type
        items.fields = import_record_fields(array_field.get("Fields"))
    elif field_type == "array":
        items.type = field_type
        items.fields = import_array_field(array_field.get("Items"))
    else:
        items.type = field_type

    return items


def import_glue(data_contract_specification: DataContractSpecification, source: str, table_names: List[str]):
    """Import the schema of a Glue database."""

    catalogid, location_uri = get_glue_database(source)

    # something went wrong
    if catalogid is None:
        return data_contract_specification

    if table_names is None:
        table_names = get_glue_tables(source)

    data_contract_specification.servers = {
        "production": Server(type="glue", account=catalogid, database=source, location=location_uri),
    }

    for table_name in table_names:
        if data_contract_specification.models is None:
            data_contract_specification.models = {}

        table_schema = get_glue_table_schema(source, table_name)

        fields = import_record_fields(table_schema)

        data_contract_specification.models[table_name] = Model(
            type="table",
            fields=fields,
        )

    return data_contract_specification


def map_type_from_sql(sql_type: str):
    if sql_type is None:
        return None

    if sql_type.lower().startswith("varchar"):
        return "varchar"
    if sql_type.lower().startswith("string"):
        return "string"
    if sql_type.lower().startswith("text"):
        return "text"
    elif sql_type.lower().startswith("byte"):
        return "byte"
    elif sql_type.lower().startswith("short"):
        return "short"
    elif sql_type.lower().startswith("integer"):
        return "integer"
    elif sql_type.lower().startswith("long"):
        return "long"
    elif sql_type.lower().startswith("bigint"):
        return "long"
    elif sql_type.lower().startswith("float"):
        return "float"
    elif sql_type.lower().startswith("double"):
        return "double"
    elif sql_type.lower().startswith("boolean"):
        return "boolean"
    elif sql_type.lower().startswith("timestamp"):
        return "timestamp"
    elif sql_type.lower().startswith("date"):
        return "date"
    elif sql_type.lower().startswith("array"):
        return "array"
    elif sql_type.lower().startswith("struct"):
        return "struct"
    else:
        return "variant"
