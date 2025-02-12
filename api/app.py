# api/app.py
from fastapi import FastAPI
import duckdb

app = FastAPI()

@app.get("/grants")
def get_grants():
    conn = duckdb.connect("openjordi.duckdb")
    results = conn.execute("SELECT * FROM grants").fetchall()
    return {"grants": results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)