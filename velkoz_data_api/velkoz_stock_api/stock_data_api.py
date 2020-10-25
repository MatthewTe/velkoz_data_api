# Importing 3rd Party Packages:
import pandas as pd
from sqlalchemy import create_engine, MetaData, Column, String, DateTime, Integer, inspect
from sqlalchemy.orm import sessionmaker, Session, scoped_session


class StockDataAPI(object):
    """
    # TODO: Add Documentation
    """
    def __init__(self, database_uri, ticker):

        # Creating a connection to the database:
        self._sqlaengine = create_engine(self._db_uri, pool_pre_ping=True, echo=True)

        # Declaring internal database parameters:
        self._db_uri = database_uri
        self._ticker = ticker

        # TODO: Add method that determines the status of a ticker and dynamically
        # generates a list of avalible database schema (table names).

        # Creating instance parameters representing stock database schema:
        self._price_history_tbl = f'{self._ticker}_price_history'

    def get_price_history(self):
        """
        The method that makes use of the pandas.read_sql_table() to extract the
        {ticker}_price_history database table as a pandas Dataframe

        Returns:
            Dataframe: The dataframe representing time-series price data of the
                ticker symbol associated with the StockDataAPI Instance. The
                DataFrame is in the format of:

                +-----------------+-------+-------+-------+-------+--------+-----------+--------------+
                |   date (index)  | open  |  high |  low  | close | volume | dividends | stock_splits |
                +=================+=======+=======+=======+=======+========+===========+==============+
                |    DateTime     | Float | Float | Float | Float |   Int  |   Float   |    Float     |
                +-----------------+-------+-------+-------+-------+--------+-----------+--------------+

        """
        # Extracting data from the database via .read_sql_table():
        price_df = pd.read_sql_table(
            table_name = self._price_history_tbl,
            con = self._sqlaengine,
            index_col = "date")

        return price_df
