# Importing Airflow Packages:
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Importing the velkoz data extraction library packages:
from velkoz_web_packages.objects_stock_data.stock_data_compiler import compile_ticker_list

# Stock Price Velkoz Packages:
from velkoz_web_packages.objects_stock_data.objects_stock_price.web_objects_stock_price import NASDAQStockPriceResponseObject
from velkoz_web_packages.objects_stock_data.objects_stock_price.ingestion_engines_stock_price import StockPriceDataIngestionEngine

# Stock Summary Data Velkoz Packages: 
from velkoz_web_packages.objects_stock_data.objects_stock_db_summary.ingestion_engines_stock_data_summary import StockDataSummaryIngestionEngine

# Importing 3rd party packages:
from datetime import datetime
from datetime import timedelta
import time
import os


class VelkozStockPipeline(object):
    """
    The VelkozStockPipeline Class contains all of the necessary data 
    and methods that are used to construct the pipelines that write 
    stock price data to a database.

    The class, upon initialization (in addition to declaring the 
    instance variables) generates dictionaries that are used to 
    configure the various pipeline DAGs that are then generated 
    by the internal methods. 

    The scheduling methods then need to be called in order to 
    add these DAGs to the airflow server/scheduler. The current 
    DAGs and methods that the VelkozStockPipeline supports are:

    * Stock Price Data Pipeline --> schedule_stock_price_data_ingestion() --> 
        write_price_data_operator
   
    Attributes:
        db_uri (str): The string that is used to connect to the
            database.
        
        email (str): The string that represents the email address 
            used to configure the DAGs in the default args dicts.
        
        dag_start_date (datetime.datetime): The datetime object
            that is used to set the start date configuration of
            the DAGs via the default args dicts.
        
        default_stock_price_args (dict): The default DAG argument
            dict for configuring the stock price DAG.
        
    Todo:
        * Extend the VelkozStockPipeline object for the fund holdings
            data. 
    """
    def __init__(self, db_uri, start_date, email="NaN"):
        
        # Declaring the instance parameters:
        self.db_uri = db_uri
        self.email = email
        
        # Converting input start date from string --> datetime obj:
        date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        self.dag_start_date = date_obj
        
        # Declaring default arguments for DAG:
        self.default_stock_price_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': self.dag_start_date,
            'email' : [self.email],
            'email_on_failure' : False,
            'email_on_retry' : False,
            'retries' : 1,
            'retry_delay' : timedelta(minutes=1)
            }        
    
    # Method that schedules writing stock price data to the database:
    def schedule_stock_price_data_ingestion(self, ticker_lst):
        """This method generates the DAG for scheduling stock price 
        time-series data ingestion to the database and returns 
        a PythonOperator that executes the “_perform_stock_data_ingestion”
        method. 

        It then uses the stock price DAG to create the PythonOperator which calls 
        the “_perform_stock_data_ingestion” method with the input ticker 
        list.

        This method is meant to be called after the parent object has 
        been initialized as both the stock price DAG and the
        python operator need to be exposed to the global execution conetext. 

        It does this so when this method is called, the DAG and Operator 
        can be declared as variables within the global execution context 
        to be detected by the airflow scheduler.


        Args:
            ticker_lst (list): The list of ticker strings to be passed
                into the "_perform_stock_data_ingestion" method via
                the PythonOperator.
        
        Returns:
            airflow.operators.python_operator.PythonOperator: The 
                scheduled Airflow Operator that is to be detected 
                by the Airflow Scheduler.
        """
        # Building the stock price DAG as an instance parameter:
        self.stock_price_dag = DAG(
            dag_id = 'stock_price_data_pipeline',
            description = "PlaceHolder",
            schedule_interval = '@daily',
            default_args = self.default_stock_price_args)

        # Creating the PythonOperator that calls the 
        # _stock_data_ingestion method:
        write_price_data_operator = PythonOperator(
            task_id = "write_price_data_to_velkoz_database",
            python_callable = self._perform_stock_data_ingestion,
            op_kwargs = {"ticker_lst":ticker_lst},
            dag = self.stock_price_dag)

        return write_price_data_operator
 
    # Method that schedules writing stock summary data to the database:
    def schedule_stock_summary_data_ingestion(self, ticker_lst):
        """
        This method generates the DAG for scheduling data ingestion of the stock 
        summary data to the database.

        It allows for said scheduling by declaring and returning a PythonOperator 
        that calls the associated internal scheduling method “_perform_stock_summary_data_ingestion”
        with the input ticker symbol list as a parameter. 
        
        The method also creates the DAG that the returned PythonOperator is associated with 
        and declares it as an instance variable, allowing it to be exposed to the global execution 
        context to be detected by the airflow scheduler.
        
        Args:
            ticker_lst (list): The list of ticker strings to be passed
                into the "_perform_stock_data_ingestion" method via
                the PythonOperator.
        
        Returns:
            airflow.operators.python_operator.PythonOperator: The 
                scheduled Airflow Operator that is to be detected 
                by the Airflow Scheduler.
        
        """
        # Building the stock data DAG as an instance parameter:
        self.stock_summary_dag = DAG(
            dag_id = 'stock_summary_data_pipeline',
            description = "PlaceHolder",
            schedule_interval = '@daily',
            default_args = self.default_stock_price_args)
        
        # Creating the PythonOperator that calls the
        # _stock_summary_data_ingestion method:
        write_stock_summary_data_operator = PythonOperator(
            task_id = "write_stock_db_summary_data_to_velkoz_database",
            python_callable = self._perform_stock_summary_data_ingestion,
            op_kwargs = {"ticker_lst":ticker_lst},
            dag = self.stock_summary_dag)

        return write_stock_summary_data_operator
   
    # Nested method to be called via the DAGs generated in the 
    # 'schedule_stock_price_data_ingestion' method:
    def _perform_stock_data_ingestion(self, ticker_lst):
        """
        This is the method that performs the actual data ingestion 
        of stock price data to a database according to the logic 
        / processes described by the velkoz web data extraction library. 

        The method creates an ingestion engine that connects to the database 
        indicated by the URI.

        It then iterates over the input ticker list and creates & adds 
        StockPriceResponseObjs to the ingestion engine’s que. The price data 
        contained within all of these web response objects are then written 
        to the database via the ingestion engine’s internal writing methods.
           
        This method is intended to be purely internal and is meant to only
        be called via the PythonOperator declared in the parent method:
        "schedule_stock_price_data_ingestion".

        Args:
            ticker_lst (list): A list of ticker strings that are used to initalize
                StockPriceResponseObjects. 
        
        """
        # Creating an instance of a database ingestion engine:
        stock_price_ingestion_engine = StockPriceDataIngestionEngine(self.db_uri)  
        
        # Creating empty list to be populated with NASDAQStockPriceResponseObjects:
        price_obj_lst = []

        # Creating the NASDAQStockPriceResponseObject List based on ticker lst:
        for ticker in ticker_lst:
            
            # Appending the price objects to the list:
            price_obj_lst.append(NASDAQStockPriceResponseObject(ticker))

            # Sleeping to prevent timeout from Yahoo Servers:
            time.sleep(20)
            
            # Mannual Debug Print:
            print("Ticker Added:", ticker)
   
        # Adding NASDAQStockPriceResponseObjects into Ingestion Engine Que:
        for price_obj in price_obj_lst:
            stock_price_ingestion_engine._insert_web_obj(price_obj)

        # Writing ingested data to the database:
        stock_price_ingestion_engine._write_web_objects() 

    # Nested Method to be called by the DAG generated in the 
    # 'schedule_stock_summary_data_ingestion' method: 
    def _perform_stock_summary_data_ingestion(self, ticker_lst):
        """
        This is the method that makes use of the Web Data Extraction Library to
         perform the actual data ingestion of price summary data to the database.

        The method creates an ingestion engine that connects to the database indicated 
        by the URI. It then iterates over the ingested list of ticker symbol strings, 
        adding them to the created ingestion engine’s que. The ingestion engine’s que 
        is then written to the database based on the engine’s internal database writing methods.

        This method is intended to be purely internal and is meant to only be called 
        via the PythonOperator declared in its parent method: 
        “schedule_stock_summary_data_ingestion”.

        Args:
            ticker_lst (list): A list of ticker strings that are used to initalize
                StockPriceResponseObjects. 

        """
        # Creating an instance of the database ingestion engine:
        stock_summary_ingestion_engine = StockDataSummaryIngestionEngine(self.db_uri)

        # Iterating through the Ingestion Engine adding all ticker symbols to the que:
        for ticker in ticker_lst:
            stock_summary_ingestion_engine._insert_web_obj(ticker)
    
        # Writing all the data from the Ingestion Engine que to the database:
        stock_summary_ingestion_engine._write_web_objects()
                  

# Basic Testing:
# test_ticker_lst = ["AAPL", "TSLA"]
# test_pipeline = VelkozStockPipeline(":memory:", "2020-10-10")
# test_pipeline.schedule_stock_price_data_ingestion(test_ticker_lst)
