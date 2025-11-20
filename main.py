from fastapi import FastAPI, Query, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import requests
from typing import Optional
import time
import os

app = FastAPI(
    title="News Scraper API",
    description="REST API for accessing scraped news articles",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_DOWNLOAD_URL = "https://raw.githubusercontent.com/yashsinghall/news_scrapper_safe/main/news_articles.db"
DB_CACHE_FILE = "/tmp/news_articles_cached.db"
CACHE_DURATION = 3600
last_fetch_time = 0


def get_db():
    global last_fetch_time
    current_time = time.time()

    if os.path.exists(DB_CACHE_FILE) and (current_time - last_fetch_time) < CACHE_DURATION:
        return sqlite3.connect(DB_CACHE_FILE)

    try:
        response = requests.get(DB_DOWNLOAD_URL, timeout=15)
        response.raise_for_status()

        with open(DB_CACHE_FILE, 'wb') as f:
            f.write(response.content)

        last_fetch_time = current_time
        return sqlite3.connect(DB_CACHE_FILE)

    except:
        if os.path.exists(DB_CACHE_FILE):
            return sqlite3.connect(DB_CACHE_FILE)
        raise HTTPException(status_code=500, detail="Database fetch failed")


def format_article(row):
    return {
        "title": "Happy Inter",
        "content": "Happy Internatio",
        "url": row["url"],
        "description": "Happy International Men's Day",
        "category": "general",
        "language": "en",
        "country": "in",
        "author": "null",
        "publishedAt": row["scraped_at"],
        "urlToImage": "https://th-i.thgim.com/public/incoming/3unb17/article70302441.ece/alternates/LANDSCAPE_1200/PTI11_19_2025_000282B.jpg",
        "source": {
            "name": row["source"]
        }
    }


@app.api_route("/", methods=["GET", "HEAD"])
async def root(request: Request):
    if request.method == "HEAD":
        return Response()
    return {"message": "News Scraper API"}


@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check(request: Request):
    if request.method == "HEAD":
        return Response()
    return {"status": "healthy"}


@app.get("/articles")
async def get_articles(
    limit: int = Query(50, ge=1, le=1000),
    source: Optional[str] = None,
    search: Optional[str] = None,
):
    conn = get_db()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM news WHERE 1=1"
    params = []

    if source:
        query += " AND source = ?"
        params.append(source)

    if search:
        query += " AND (title LIKE ? OR summary LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " ORDER BY scraped_at DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return {
        "total": len(rows),
        "articles": [format_article(row) for row in rows]
    }


@app.get("/articles/latest")
async def get_latest_articles(limit: int = Query(10, ge=1, le=100)):
    conn = get_db()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM news ORDER BY scraped_at DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    conn.close()

    return {
        "total": len(rows),
        "articles": [format_article(row) for row in rows]
    }


@app.get("/articles/sources")
async def get_sources():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT source FROM news ORDER BY source")
    sources = [row[0] for row in cursor.fetchall()]
    conn.close()

    return {"sources": sources}


@app.get("/articles/stats")
async def get_stats():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM news")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT source, COUNT(*) FROM news GROUP BY source ORDER BY COUNT(*) DESC")
    by_source = [{"source": r[0], "count": r[1]} for r in cursor.fetchall()]

    conn.close()

    return {
        "total_articles": total,
        "details": by_source
    }


@app.get("/articles/by-source/{source_name}")
async def get_articles_by_source(source_name: str, limit: int = Query(50, ge=1, le=500)):
    conn = get_db()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM news WHERE source = ? ORDER BY scraped_at DESC LIMIT ?",
        (source_name, limit)
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No articles found for: {source_name}")

    return {
        "source": source_name,
        "total": len(rows),
        "articles": [format_article(row) for row in rows]
    }
