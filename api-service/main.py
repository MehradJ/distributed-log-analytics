from fastapi import FastAPI
import psycopg2

app = FastAPI()

conn = psycopg2.connect(
    dbname="logs", user="user", password="pass", host="postgres", port=5432
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