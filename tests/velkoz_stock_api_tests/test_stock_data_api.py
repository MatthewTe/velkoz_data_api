# Import 3rd party test methods:
import unittest
import sqlite3
import sqlalchemy
import pandas as pd
import numpy as np
import os

# Importing Data API methods for testing:
from velkoz_data_api.velkoz_stock_api.stock_data_api import StockDataAPI

class StockDataAPITest(unittest.TestCase):

    def test_stock_data_api_price_history_test(self):
        """
        A method that performs a unit test on the StockDataAPI object.

        This method performs a unit test on the StockDataAPI’s ability to extract
        the price history data from a database that is maintained by the
        velkoz_airflow_pipeline. The method:

        * Initializes a StockDataAPI instance and tests the connection to the database
        * Calls the “get_price_history” method and tests the data returned to ensure it is extracted in the correct format.

        """
        # Initalizing the Data API instance:
        test_api_instance = StockDataAPI()

        # Asserting that the api has been initalized with database correctly:
        self.assertEqual(test_api_instance._db_uri, os.environ["DATABASE_URI"])
        self.assertEqual(type(test_api_instance._sqlaengine), sqlalchemy.engine.Engine)

        # Performing a database call for price data for AAPL:
        aapl_price_data = test_api_instance.get_price_history('AAPL')

        print(aapl_price_data.head())

        # Assertion testing the column names of the extracted dataframe:
        aapl_price_data_column_names = sorted(aapl_price_data.columns)
        test_column_names_lst = sorted(['open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits'])

        self.assertEqual(aapl_price_data_column_names, test_column_names_lst)

        # Type testing the data from each column in the 'aapl_price_data':
        self.assertIs(aapl_price_data.open.dtype, np.dtype('float64'))
        self.assertIs(aapl_price_data.high.dtype, np.dtype('float64'))
        self.assertIs(aapl_price_data.low.dtype, np.dtype('float64'))
        self.assertIs(aapl_price_data.close.dtype, np.dtype('float64'))
        self.assertIs(aapl_price_data.volume.dtype, np.dtype('int64'))
        self.assertIs(aapl_price_data.dividends.dtype, np.dtype('float64'))
        self.assertIs(aapl_price_data.stock_splits.dtype, np.dtype('float64'))

        # Type Testing the Data in the price_history dataframe index:
        self.assertIsInstance(aapl_price_data.index, pd.DatetimeIndex)
