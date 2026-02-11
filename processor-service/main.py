from fastapi import FastAPI
import redis
import json
import psycopg2
import threading
import time

app = FastAPI()

r = redis.Redis(host="redis", port=6379, decode_responses=True)

conn = psycopg2.connect(
    dbname="logs", user="user", password="pass", host="postgres", port=5432
)
cur = conn.cursor()
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS logs (
        id SERIAL PRIMARY KEY,
        message TEXT,
        timestamp FLOAT
    )
"""
)
conn.commit()
def listen():
    pubsub = r.pubsub()
    pubsub.subscribe("log_channel")
    for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"])
            cur.execute(
                "INSERT INTO logs (message, timestamp) VALUES (%s, %s)",
                (data["message"], data["timestamp"]),
            )
            conn.commit()
            print("Inserted:", data)

threading.Thread(target=listen, daemon=True).start()


@app.get("/")
def health():
    return {"status": "ok"}
