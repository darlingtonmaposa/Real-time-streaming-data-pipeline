"""

    Description:

    This script provides functionality to generate a stream of synthetic 
    ticker data, representing real-time stock market data for various 
    ticker indexes. 

    For guidance on how to use this script, please see the provided
    project instructions.  
"""  

import logging
from datetime import datetime, timedelta
import random
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from decimal import Decimal
import json


def get_current_time(second_delta):
    """Function to get and return the current time changed by a specific 
    delta in seconds. 

    Args:
        second_delta (int): Delta in seconds to be added from the time.

    Returns:
        datetime.datetime: Current time with delta time applied.
    """
    now = datetime.now()
    current_delta_time = now + timedelta(seconds=second_delta)

    return current_delta_time

def price_generator(old_price, volatility_perc):
    """A function to generate a price of a ticker, given an intialisation
    value and volatility percentage.

    Args:
        old_price (int|float): Initial value for the price to be generated.
        volatility_perc (float): Percentage volatility as a fraction of 1.

    Returns:
        float: New price for ticker.
    """

    rnd = random.random()
    change_percent = 2 * volatility_perc * rnd
    if (change_percent > volatility_perc):
        change_percent -= (2 * volatility_perc)
    change_amount = old_price * change_percent
    new_price = old_price + change_amount
    return new_price

# ===================================================================================
# Insert your SNS topic ARN below
sns_arn = "arn:aws:sns:eu-west-1:529077217687:DAR-streaming-sns-topic" 
# Insert your full name and surname for marking purposes
student_name = "DARLINGTON MAPOSA" 
# ===================================================================================
def lambda_handler(event, context):
    """Handler function connects to both AWS DynamoDB, and Firehose.
    It genrates ticker data for every minute (5 values for every 15 seconds).
    It the deposits the data in DynamoDB, as well as depositing the data
    into the delivery stream. 
    """

    # Index prefix to extract from the DynamoDB table
    TICKER = "Index_" 

    # ==================================================================================
    # Configure DynamoDB as part of Step 3
    __TableName__ = 'DAR-streaming-dynamodb' # <-- Insert your table name
    # Configure Firehose service as part of Step 6
    firehose_name = 'DAR-delivery-stream-firehose' # <-- Insert your Firehose name
    # ===================================================================================

     # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # ===================================================================================
    # Write code here as part of Step 3 to connect to DynamoDB to retrieve the database and 
    # table object you've created. Use the boto3 client to perform this task. 
    # Store the resulting object in a variable called `table`:
    table = boto3.resource('dynamodb').Table(__TableName__) # <-- Write code to update the value of this variable.
    # ===================================================================================


    # Retrieve ticker data from the DynamoDB table
    fe = Attr('TickerName').contains(TICKER);
    response = table.scan(FilterExpression=fe)

    dictlist = []
    for value in response["Items"]:
        temp = [value["TickerName"], value["Price"]]
        dictlist.append(temp)

    Tickers = []
    Prices = []

    for a in dictlist:
        Tickers.append(a[0])
        Prices.append(float(a[1]))

    # Generate new ticker values
    batch = []
    for data_gen in range(0, 4):
    # Ticker1
        current_price1 = price_generator(float(Prices[0]), 0.01)
        new_price_time1 = get_current_time(data_gen * 15)
        temp_dict1 = {"Data": "{ticker : " + str(Tickers[0]) + ", timestamp : " + str(new_price_time1) + ", price : " + str(current_price1) + "}"}
        batch.append(temp_dict1)
    
        # Update ticker value in DynamoDB table
        response = table.put_item(
            Item={
                'TickerName': Tickers[0],
                'Price': Decimal(str(current_price1))
            }
        )
    
        # Ticker2
        current_price2 = price_generator(float(Prices[1]), 0.01)
        new_price_time2 = get_current_time(data_gen * 15)
        temp_dict2 = {"Data": "{ticker : " + str(Tickers[1]) + ", timestamp : " + str(new_price_time2) + ", price : " + str(current_price2) + "}"}
        batch.append(temp_dict2)
    
        # Update ticker value in DynamoDB table
        response = table.put_item(
            Item={
                'TickerName': Tickers[1],
                'Price': Decimal(str(current_price2))
            }
        )
    
        # Ticker3
        current_price3 = price_generator(float(Prices[2]), 0.01)
        new_price_time3 = get_current_time(data_gen * 15)
        temp_dict3 = {"Data": "{ticker : " + str(Tickers[2]) + ", timestamp : " + str(new_price_time3) + ", price : " + str(current_price3) + "}"}
        batch.append(temp_dict3)
    
        # Update ticker value in DynamoDB table
        response = table.put_item(
            Item={
                'TickerName': Tickers[2],
                'Price': Decimal(str(current_price3))
            }
        )
    
        # Ticker4
        current_price4 = price_generator(float(Prices[3]), 0.01)
        new_price_time4 = get_current_time(data_gen * 15)
        temp_dict4 = {"Data": "{ticker : " + str(Tickers[3]) + ", timestamp : " + str(new_price_time4) + ", price : " + str(current_price4) + "}"}
        batch.append(temp_dict4)
    
        # Update ticker value in DynamoDB table
        response = table.put_item(
            Item={
                'TickerName': Tickers[3],
                'Price': Decimal(str(current_price4))
            }
        )
    
        # Ticker5
        current_price5 = price_generator(float(Prices[4]), 0.01)
        new_price_time5 = get_current_time(data_gen * 15)
        temp_dict5 = {"Data": "{ticker : " + str(Tickers[4]) + ", timestamp : " + str(new_price_time5) + ", price : " + str(current_price5) + "}"}
        batch.append(temp_dict5)
    
        response = table.put_item(
            Item={
                'TickerName': Tickers[4],
                'Price': Decimal(str(current_price5))
            }
        )



    # Put records into the Firehose stream
    firehose_client = boto3.client('firehose')
    try:

        # ===================================================================================
        # Push the generated `batch` list above to the firehose stream
        # ===================================================================================

        #pass
        firehose_client.put_record_batch(
            DeliveryStreamName='DAR-delivery-stream-firehose',
            Records=batch,
            )

    except ClientError as e:
        logging.error(e)
        client = boto3.client("sns")
        resp = client.publish(TargetArn=sns_arn, 
                              Message=json.dumps(event),
                              Subject=f"Failure: {student_name}, could not generate stream")
        raise e
        

    

    logging.info('Test data sent to Firehose stream')
