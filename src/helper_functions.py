import boto3
import os
from datetime import datetime
import json

# Load environment variables
BUCKET_NAME = os.getenv('BUCKET_NAME')
POKE_API_URL = os.getenv('POKE_API_URL')

# AWS S3 client instance for accessing in program
s3 = boto3.client('s3')

# ETL Functions

# Extract: Extracts raw data for a single Pokemon ID from the PokeAPI
async def extract_pokemon_data(session, pokemon_id):
    url = f"{POKE_API_URL}{pokemon_id}"
    
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                print(f"HTTP {resp.status}: failed {pokemon_id}")
                return None
    
    except Exception as e:
        print(f"error fetching {pokemon_id}: {e}")
    
# Transform: Transforms the raw JSON into a flattened record
def transform_data(raw_data):
    if not raw_data:
        return None
    
    try:
        transformed_record = {
            'pokemon_id': raw_data['id'],
            'pokemon_name': raw_data['name'],
            'primary_type': raw_data['types'][0]['type']['name'],
            'base_hp': raw_data['stats'][0]['base_stat'],
            'base_attack': raw_data['stats'][1]['base_stat'],
            'base_defense': raw_data['stats'][2]['base_stat'],
            'height_dm': raw_data['height'],
            'weight_hg': raw_data['weight'],
            'first_ability': raw_data['abilities'][0]['ability']['name'],
            'ingest_timestamp': datetime.now().isoformat(),
        }
        
        return transformed_record
    
    except (KeyError, IndexError) as e:
        print(f'data transformation failed for ID {raw_data.get('id', 'Unknown')}: missing key {e}')
        return None
    
# Load: Loads and writes the transformed records to a transformed S3 path
def load_data_to_s3(records):
    if not records:
        print('no records available to load!')
        return 0
    
    # convert records to JSON lines format for Glue and Athena efficiency via. List Comprehension
    data_payload = '\n'.join([json.dumps(r) for r in records])
    
    # define a partitioned S3 key based on the current date for data organization
    current_date = datetime.now().strftime('%Y-%m-%d')
    s3_key = f'raw/pokemon/ingest_date={current_date}/{datetime.now().strftime("%H%M%S%f")}.json'
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=data_payload.encode('utf-8')
        )
        
        print(f'successfully loaded {len(records)} records to s3://{BUCKET_NAME}/{s3_key}')
        return len(records)
    
    except Exception as e:
        print(f'failed to write data to S3: {e}')
        return 0