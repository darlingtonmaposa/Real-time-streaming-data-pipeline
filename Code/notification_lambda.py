"""

    This script provides functionality to generate custom AWS SNS
    email messages when triggered. 

    For guidance on how to use this script, please see the provided
    predict part-2 instructions.  
"""  

import boto3
import json

# ===================================================================================
# Insert your SNS topic ARN below
sns_arn = "arn:aws:sns:your:topic:here" 
# Insert your full name and surname
student_name = "Dora Explorer" 
# ===================================================================================

def lambda_handler(event, context):
    client = boto3.client("sns")
    resp = client.publish(TargetArn=sns_arn, 
                          Message=json.dumps(event),
                          Subject=f"Failure: {student_name}, could not generate stream")
