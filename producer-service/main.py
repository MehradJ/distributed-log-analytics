from fastapi import FastAPI
import time
import redis
import json

app = FastAPI()
r = redis.Redis(host="redis", port=6379, decode_responses=True)

@app.get("/")
def health():
    return {"status": "ok"}

@app.get("/send")
def send_event():
    event = {"message": "log event", "timestamp": time.time()}
    r.publish("log_channel", json.dumps(event))
    return {"status": "sent", "event": event}
