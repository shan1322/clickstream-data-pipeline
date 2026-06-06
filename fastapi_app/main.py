from fastapi import FastAPI, BackgroundTasks
from models import ClickEvent, BulkClickEvents
from producer import send_event, send_bulk_events
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Clickstream Ingest API")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest")
def ingest_event(event: ClickEvent, background_tasks: BackgroundTasks):
    background_tasks.add_task(send_event, event.dict())
    return {"status": "ok", "event": event.dict()}

@app.post("/ingest/bulk")
def ingest_bulk(payload: BulkClickEvents, background_tasks: BackgroundTasks):
    background_tasks.add_task(send_bulk_events, [e.dict() for e in payload.events])
    return {"status": "ok", "count": len(payload.events)}
