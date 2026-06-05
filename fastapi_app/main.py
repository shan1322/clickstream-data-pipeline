from fastapi import FastAPI, HTTPException
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
def ingest_event(event: ClickEvent):
    try:
        send_event(event.dict())
        logger.info(f"Event sent: user={event.user_id} item={event.item_id} type={event.event_type}")
        return {"status": "ok", "event": event.dict()}
    except Exception as e:
        logger.error(f"Failed to send event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/bulk")
def ingest_bulk(payload: BulkClickEvents):
    try:
        send_bulk_events([e.dict() for e in payload.events])
        logger.info(f"Bulk sent: {len(payload.events)} events")
        return {"status": "ok", "count": len(payload.events)}
    except Exception as e:
        logger.error(f"Failed to send bulk events: {e}")
        raise HTTPException(status_code=500, detail=str(e))
