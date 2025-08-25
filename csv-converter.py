import os
import json
import boto3
from urllib.parse import unquote_plus
from collections import defaultdict

def lambda_handler(event, context):
    
    
    
    
    OUTPUT_BUCKET_NAME = os.environ["OUTPUT_BUCKET_NAME"]
    OUTPUT_S3_PREFIX = os.environ["OUTPUT_S3_PREFIX"]
    

    message = json.loads(event["Records"][0]["Sns"]["Message"])
    job_id = message["JobId"]

    print(f"Processing Textract JobId: {job_id}")

   
    key_value_pairs = process_document_analysis_response(job_id)


    output_key_name = f"{job_id}.json"
    

    upload_to_s3(key_value_pairs, OUTPUT_BUCKET_NAME, f"{OUTPUT_S3_PREFIX}/{output_key_name}")

    return {
        "statusCode": 200,
        "body": json.dumps("Key-value pairs extracted and uploaded successfully!")
    }


def upload_to_s3(data, bucket, key):
  
    s3 = boto3.client("s3")
    json_data = json.dumps(data, indent=2)
    s3.put_object(Body=json_data, Bucket=bucket, Key=key)
    print(f"Successfully uploaded JSON to s3://{bucket}/{key}")


def get_text_from_blocks(block_id, block_map):
    
    text = ""
    block = block_map.get(block_id)
    if block and "Relationships" in block:
        for relationship in block["Relationships"]:
            if relationship["Type"] == "CHILD":
                for child_id in relationship["Ids"]:
                    child_block = block_map.get(child_id)
                    if child_block and child_block["BlockType"] == "WORD":
                        text += child_block["Text"] + " "
    return text.strip()


def process_document_analysis_response(job_id):
    """
    Retrieves the full asynchronous Textract analysis result and
    parses it to extract key-value pairs.
    """
    textract = boto3.client("textract")
    all_pages = []
    

    response = textract.get_document_analysis(JobId=job_id)
    all_pages.append(response)

   
    nextToken = response.get("NextToken")
    while nextToken:
        response = textract.get_document_analysis(JobId=job_id, NextToken=nextToken)
        all_pages.append(response)
        nextToken = response.get("NextToken")

    key_value_pairs = {}
    
    for page in all_pages:
        block_map = {block["Id"]: block for block in page["Blocks"]}
        
        for block in page["Blocks"]:
            if block["BlockType"] == "KEY_VALUE_SET" and "KEY" in block["EntityTypes"]:
                key_text = get_text_from_blocks(block["Id"], block_map)
                
               
                for relationship in block.get("Relationships", []):
                    if relationship["Type"] == "VALUE":
                        for value_id in relationship["Ids"]:
                            value_text = get_text_from_blocks(value_id, block_map)
                            key_value_pairs[key_text] = value_text
                            break 
                    if key_text in key_value_pairs:
                        break 
            
    return key_value_pairs