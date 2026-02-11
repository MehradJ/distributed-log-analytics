from fastapi import FastAPI
import time
import redis
import os
import json
import threading

app = FastAPI()
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    decode_responses=True
)
@app.get("/")
def health():
    return {"status": "ok"}

@app.get("/send")
def send_event():
    data = {"message": "log event", "timestamp": time.time()}
    r.xadd("log_stream", data)
    return {"status": "sent", "event": data}
