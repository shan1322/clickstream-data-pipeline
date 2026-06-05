import pandas as pd
import boto3
import logging
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = "clickstream-pipeline"
KEY = "raw/events.csv"

def load_events():
    logger.info(f"Loading events from s3://{BUCKET}/{KEY}")
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={
        'visitorid': 'user_id',
        'itemid': 'item_id',
        'event': 'event_type'
    })
    df = df[['user_id', 'item_id', 'event_type', 'timestamp']]
    df['event_type'] = df['event_type'].str.lower()
    df = df[df['event_type'].isin(['view', 'addtocart', 'transaction'])]
    logger.info(f"Loaded {len(df)} events")
    return df.to_dict('records')
