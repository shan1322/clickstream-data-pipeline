import random
from locust import HttpUser, task, between
from data_loader import load_events

events_data = load_events()

class ClickstreamUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def send_event(self):
        event = random.choice(events_data)
        self.client.post("/ingest", json=event)
