from fastapi import FastAPI, Query
from db import get_connection

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}
@app.get("/logs")
def get_logs(limit: int = 100, from_timestamp: float = 0):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, message, timestamp
        FROM logs
        WHERE timestamp >= %s
        ORDER BY timestamp DESC
        LIMIT %s
        """,
        (from_timestamp, limit),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    logs = [{"id": row[0], "message": row[1], "timestamp": row[2]} for row in rows]
    return {"logs": logs}
