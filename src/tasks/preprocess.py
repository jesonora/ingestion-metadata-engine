import sys
import logging

logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from src.support import get_spark_session


def validate_fields_cols(df: DataFrame, field_validations: list) -> tuple:
    """
    Validate fields in DataFrame based on specified validations.

    Args:
        df (DataFrame): Input DataFrame.
        field_validations (list): List of dictionaries containing field validations.

    Returns:
        tuple: A tuple containing the validated DataFrame and a list of error columns.
    """
    error_columns = []
    for validation in field_validations:
        field = validation['field']
        validations = validation['validations']
        for v in validations:
            error_col_name = f"{field}_{v}"  # New column name for error
            error_columns.append(error_col_name)
            if 'notEmpty' in v:
                df = df.withColumn(error_col_name, when(col(field) == "", lit("KO")).otherwise(lit("OK")))
            if 'notNull' in v:
                df = df.withColumn(error_col_name, when(col(field).isNull(), lit("KO")).otherwise(lit("OK")))
    return df, error_columns


def filter_by_column_values(df: DataFrame, columns: list) -> tuple:
    """
    Filter DataFrame based on column values.

    Args:
        df (DataFrame): Input DataFrame.
        columns (list): List of column names to filter by.

    Returns:
        tuple: A tuple containing the filtered DataFrame and a DataFrame containing rows not matching the filter.
    """
    condition = " AND ".join(f"{col} = 'OK'" for col in columns)
    filtered_df = df.filter(condition)
    not_ok = df.subtract(filtered_df)
    return filtered_df.drop(*columns), not_ok


def struct_columns_to_single_column(df: DataFrame, column_names: list) -> DataFrame:
    """
    Combine struct columns into a single column.

    Args:
        df (DataFrame): Input DataFrame.
        column_names (list): List of column names to combine.

    Returns:
        DataFrame: DataFrame with combined struct columns.
    """
    struct_col = F.struct(*[F.col(col_name) for col_name in column_names])
    return df.withColumn("validations", struct_col).drop(*column_names)


def load_input(spark: SparkSession, **kwargs) -> DataFrame:
    """
    Load input data into DataFrame.

    Args:
        spark (SparkSession): Spark session object.
        kwargs: Additional arguments for loading data.

    Returns:
        DataFrame: Loaded DataFrame.
    """
    df = spark.read.load(**kwargs)
    return df


def save_output(df: DataFrame, **kwargs) -> None:
    """
    Save DataFrame to output location.

    Args:
        df (DataFrame): DataFrame to save.
        kwargs: Additional arguments for saving data.
    """
    df.write.save(**kwargs)


def transform(input_df: DataFrame, transformation_type: str, steps: list) -> DataFrame:
    """
    Perform transformation on DataFrame.

    Args:
        input_df (DataFrame): Input DataFrame.
        transformation_type (str): Type of transformation to perform.
        steps (list): List of transformation steps.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    return transform_funcs_dict[transformation_type](input_df, steps)


def validate_fields(input_df: DataFrame, steps: list) -> tuple:
    """
    Validate fields in DataFrame.

    Args:
        input_df (DataFrame): Input DataFrame.
        steps (list): List of validation steps.

    Returns:
        tuple: A tuple containing the DataFrame with valid rows and DataFrame with invalid rows.
    """
    validated_df, val_cols = validate_fields_cols(input_df, steps)
    validated_df_OK, validated_df_NOTOK = filter_by_column_values(validated_df, val_cols)
    validated_df_NOTOK = struct_columns_to_single_column(validated_df_NOTOK, val_cols)
    validated_df_NOTOK = add_current_timestamp(validated_df_NOTOK)
    return validated_df_OK, validated_df_NOTOK


def add_fields(input_df: DataFrame, params: list) -> list:
    """
    Add new fields to DataFrame based on specified parameters.

    Args:
        input_df (DataFrame): Input DataFrame.
        params (list): List of dictionaries containing parameters for adding fields.

    Returns:
        list: List containing the modified DataFrame.
    """
    for item in params:
        column_name = item['name']
        function_name = item['function']

        # Get the function object dynamically
        function = globals()[function_name]

        # Apply function and add column to DataFrame
        input_df = input_df.withColumn(column_name, function())
    return [input_df]


def add_current_timestamp(df: DataFrame) -> DataFrame:
    """
    Add current timestamp column to DataFrame.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: DataFrame with current timestamp column added.
    """
    return df.withColumn("dt", current_timestamp())


# Dictionary to map transformation types to corresponding functions
transform_funcs_dict = dict()
transform_funcs_dict["validate_fields"] = validate_fields
transform_funcs_dict["add_fields"] = add_fields


def execute_tasks(spark: SparkSession, sc, metadata_input: dict) -> None:
    """
    Execute tasks specified in the metadata input.

    Args:
        spark (SparkSession): Spark session object.
        sc: Spark context.
        metadata_input (dict): Metadata input containing sources, transformations, and outputs.
    """
    outputs_dict = {}

    # Load input data from sources
    for source in metadata_input["sources"]:
        outputs_dict[source["name"]] = load_input(spark, **source["params"])

    # Perform transformations
    for transformation in metadata_input["transformations"]:
        transformation_name = transformation["name"]
        transformation_type = transformation["type"]
        params = transformation["params"]
        input_df = params["input"]
        steps = params["steps"]
        outputs = params["outputs"]

        # Transform data
        outs = transform(outputs_dict[input_df], transformation_type, steps)
        temp = {value: outs[idx] for idx, value in enumerate(outputs.values())}
        outputs_dict = {**temp, **outputs_dict}

    # Save output data
    for output in metadata_input["outputs"]:
        save_output(outputs_dict[output["input"]], **output["params"])


if __name__ == "__main__":
    sc, spark = get_spark_session()

    metadata = str(sys.argv[1])
    execute_tasks(spark, sc, metadata)
    sc.stop()
