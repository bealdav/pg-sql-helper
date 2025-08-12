import connectorx as cx
import polars as pl


class PgSqlHelper:
    """
    - datetime to date OK
    - JSON to string
    - friendly names for columns
    """

    # dsn
    conn: str = False
    # convert datetime to date
    datetime_to_date: bool = True
    # convert JSON to string according to local string
    lang: str = None
    # rename columns as friendly names
    columns: dict[str, str] = None

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
        if self.datetime_to_date:
            df = self._convert_to_date(df)
        if self.columns:
            df = self._rename_columns(df)
        if self.lang:
            df = self._select_key_according_to_lang(df)
        return df.get_column("column").to_list()

    def set_columns_to_rename(self, columns: dict[str, str]):
        self.columns = columns

    def _rename_columns(self, df):
        return df

    def _select_key_according_to_lang(self, df):
        return df

    def _convert_to_date(self, df):
        renames = {
            x: f"{x}::date"
            for x in df.filter(pl.col("type") == "timestamp")
            .get_column("column")
            .to_list()
        }

        def replace_values(value):
            return renames.get(value, value)

        if renames:
            df = df.with_columns(pl.col("column").map_elements(replace_values))
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
