# Importing 3rd Party Packages:
import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Column, String, DateTime, Integer, inspect
from sqlalchemy.orm import sessionmaker, Session, scoped_session


class StockDataAPI(object):
    """
    # TODO: Add Documentation
    """
    def __init__(self):

        # Declaring internal database parameters, database uri is extracted from
        # environment variables:
        try:
            self._db_uri = os.environ["DATABASE_URI"]

        except:
            raise ValueError(f"Error With Database URI Environment Variable, Please Check {os.getenv('DATABASE_URI')}")

        # Creating a connection to the database:
        self._sqlaengine = create_engine(self._db_uri, pool_pre_ping=True, echo=True)

    def get_price_history(self, ticker):
        """
        The method that makes use of the pandas.read_sql_table() to extract the
        {ticker}_price_history database table as a pandas Dataframe.

        A ticker symbol is passed into the method which is used to construct the
        db table name of the table containing time-series price history data.

        Args:
            ticker (str): The ticker symbol string representing the stock whose
                data is stored in the database Eg: 'AAPL'.

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
        # Building the database table for the price history data table of a specific ticker:
        price_history_tbl_name = f"{ticker}_price_history"

        # Extracting data from the database via .read_sql_table():
        price_df = pd.read_sql_table(
            table_name = price_history_tbl_name,
            con = self._sqlaengine,
            index_col = "date")

        return price_df
