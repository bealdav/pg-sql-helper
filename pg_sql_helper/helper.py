import connectorx as cx
import polars as pl
import logging
import re

_logger = logging.getLogger(__name__)


class PgSqlHelper:
    """
    - remove name string
    - JSON to string
    - friendly names for renames
    """

    # dsn
    conn: str = False
    # convert datetime to date
    datetime_to_date: bool = True
    # convert JSON to string according to local string
    lang: str = None
    # rename as friendly names
    renames: dict[str, str] = None
    # drop some columns
    drop: list[str] = None
    drop_with_regex: list[str] = None

    def __init__(
        self,
        db: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
        lang: str = "en_US",
    ) -> None:
        self.conn = f"postgres://{user}:{password}@{host}:{port}/{db}"

    def process(self, table):
        try:
            df = cx.read_sql(
                self.conn, self._get_columns_query(table), return_type="polars"
            )
        except Exception as e:
            raise e
        df = self._drop_columns(df, table)
        if self.datetime_to_date:
            df = self._convert_to_date(df)
        if self.renames:
            df = self._rename_columns(df)
        if self.lang:
            df = self._select_key_according_to_lang(df)
        return df.get_column("column").to_list()

    def set_renames_to_rename(self, renames: dict[str, str]):
        self.renames = renames

    def set_columns_to_drop(self, columns: list[str]):
        self.drop = columns

    def set_columns_to_drop_according_to_regex(self, columns: list[str]):
        self.drop_with_regex = columns

    def _rename_columns(self, df):
        # TODO implements
        return df

    def _drop_columns(self, df, table):
        if self.drop:
            # drop columns according to the list
            to_drop = self.drop
            # check columns to drop are there
            not_found_cols = [
                x for x in self.drop if x not in df.get_column("column").to_list()
            ]
            if not_found_cols:
                _logger.warning(
                    f"Columns {not_found_cols} to drop not found in the table '{table}'"
                )
                to_drop = [x for x in to_drop if x not in not_found_cols]
            if to_drop:
                # columns names are in the rows of the column 'column'
                df = df.filter(~pl.col("column").is_in(to_drop))
        if self.drop_with_regex:
            for regex in self.drop_with_regex:
                # drop columns according to the regex
                to_drop = [
                    item
                    for item in df.get_column("column").to_list()
                    if re.search(regex, item)
                ]
                df = df.filter(~pl.col("column").is_in(to_drop))
        return df

    def _select_key_according_to_lang(self, df):
        # TODO implements
        return df

    def _convert_to_date(self, df):
        renames = {
            x: f"{x}::date"
            for x in df.filter(pl.col("type") == "timestamp")
            .get_column("column")
            .to_list()
        }

        def replace_values(values):
            return values.replace(renames)

        if renames:
            df = df.with_columns(
                pl.col("column").map_batches(
                    lambda x: replace_values(x), return_dtype=pl.String
                )
            )
        return df

    def _get_columns_query(self, table):
        """udt_name values are:
        int4, numeric, float8, bool, varchar, date, timestamp, jsonb
        """
        return f"""
        SELECT column_name AS column, udt_name AS type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = '{table}'
        """
