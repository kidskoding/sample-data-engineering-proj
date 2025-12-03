from helper_functions import extract_pokemon_data, transform_data, load_data_to_s3
import json
import aiohttp
import asyncio

# Asynchronously run ETL pipeline
async def async_etl():
    async with aiohttp.ClientSession() as session:
        gen_6_pokemon = range(1, 722)
        tasks = [extract_pokemon_data(session, p_id) for p_id in gen_6_pokemon]
        raw_results = await asyncio.gather(*tasks)
        
        transformed_records = [transform_data(r) for r in raw_results if r]
        return load_data_to_s3(transformed_records)

# Lambda Function Handler: this is the main entry point for AWS Lambda execution
def lambda_handler(event, context):
    rows_inserted = asyncio.run(async_etl())
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'completed etl successfully. total records loaded to S3 is {rows_inserted}')
    }