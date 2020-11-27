# Importing 3rd Party Packages:
import pandas as pd
import os
from sqlalchemy import create_engine, MetaData, Column, String, DateTime, Integer, inspect
from sqlalchemy.orm import sessionmaker, Session, scoped_session


class StockDataAPI(object):
    """This is the API that is used to query a database that has 
    velkoz formatted data written to it.

    The object is initialized which opens a connection to an external database via the 
    use of the environment variable “DATABASE_URI”. The URI is not passed into the object 
    as a parameter, it is read from an env variable for security concerned. The database 
    connection is also created through a SQLAlchemy engine.

    Once the object is initialized, various methods can be called that make database queries 
    and returns formatted data from the database. The queries that are currently available are:

    - get_price_history --> The Price Time-Series of a particular ticker.
    - get_stock_database_summary --> Summary data of available stocks contained in the database.

    """
    def __init__(self):

        # Declaring internal database parameters, database uri is extracted from
        # environment variables:
        try:
            self._db_uri = os.environ["DATABASE_URI"]
            
             # Creating a connection to the database:
            self._sqlaengine = create_engine(self._db_uri, pool_pre_ping=True, echo=True)
        
        except:
            raise ValueError(f"Error With Database URI Environment Variable, Please Check {os.getenv('DATABASE_URI')}")


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
            index_col = "Date")

        return price_df


    def get_stock_database_summary(self):
        """
        The method that makes use of the pandas.read_sql_table() to extract the 
        “nasdaq_stock_data_summary_tbl” database table as a pandas Dataframe. 
        
        This contains summary data on the stocks available in the velkoz database.
        
        Returns:
            Dataframe: The dataframe representing the summary data on all stock data 
                available in the database. The DataFrame is in the format of:
                
                +----------------+-----------+---------------+----------------+
                | ticker (index) | price_tbl |  holdings_tbl | last_updated   |
                +================+===========+===============+================+
                |      String    |   String  |    String     | DateTime (unix)|   
                +----------------+-----------+---------------+----------------+

        """
        # Extracting database table from the database via .read_sql_table():
        stock_summary_df = pd.read_sql_table(
            table_name = "nasdaq_stock_data_summary_tbl",
            con = self._sqlaengine,
            index_col = "ticker")

        return stock_summary_df
