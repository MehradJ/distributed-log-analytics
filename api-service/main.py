from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
)

cur = conn.cursor()

@app.get("/")
def health():
    return {"status": "ok"}
@app.get("/logs")

def get_logs():
    cur.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 10")
    rows = cur.fetchall()
    return {"logs": rows}