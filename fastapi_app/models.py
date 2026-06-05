from pydantic import BaseModel, validator
from typing import Optional
import time

class ClickEvent(BaseModel):
    user_id: int
    item_id: int
    event_type: str
    timestamp: Optional[int] = None
    session_id: Optional[str] = None

    @validator('event_type')
    def validate_event_type(cls, v):
        allowed = ['view', 'addtocart', 'transaction']
        if v not in allowed:
            raise ValueError(f'event_type must be one of {allowed}')
        return v

    @validator('timestamp', always=True, pre=True)
    def set_timestamp(cls, v):
        return v or int(time.time() * 1000)

class BulkClickEvents(BaseModel):
    events: list[ClickEvent]
