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

# Use direct download URL instead of API (no rate limits!)
DB_DOWNLOAD_URL = "https://raw.githubusercontent.com/yashsinghall/news_scrapper_safe/main/news_articles.db"

# Cache settings
DB_CACHE_FILE = "/tmp/news_articles_cached.db"
CACHE_DURATION = 3600  #15  # 1/4 minutes
last_fetch_time = 0

def get_db():
    """Download database from GitHub with caching (uses raw URL, no API limits)"""
    global last_fetch_time

    current_time = time.time()

    # Use cached database if recent
    if os.path.exists(DB_CACHE_FILE) and (current_time - last_fetch_time) < CACHE_DURATION:
        return sqlite3.connect(DB_CACHE_FILE)

    # Fetch fresh database from GitHub (raw URL - no rate limits!)
    try:
        response = requests.get(DB_DOWNLOAD_URL, timeout=15)
        response.raise_for_status()

        # Save binary content directly
        with open(DB_CACHE_FILE, 'wb') as f:
            f.write(response.content)

        last_fetch_time = current_time
        return sqlite3.connect(DB_CACHE_FILE)

    except requests.exceptions.RequestException as e:
        # If download fails but cache exists, use cache
        if os.path.exists(DB_CACHE_FILE):
            return sqlite3.connect(DB_CACHE_FILE)
        raise HTTPException(status_code=500, detail=f"Failed to fetch database: {str(e)}")

@app.api_route("/", methods=["GET", "HEAD"])
async def root(request: Request):
    if request.method == "HEAD":
        return Response()
    return {
        "message": "News Scraper API - Live news from multiple sources",
        "version": "1.0.0",
        "cache_duration": f"{CACHE_DURATION} seconds",
        "endpoints": {
            "/articles": "Get all articles with filters",
            "/articles/latest": "Get latest articles",
            "/articles/sources": "Get list of sources",
            "/articles/stats": "Get statistics",
            "/health": "Health check",
            "/docs": "Interactive documentation"
        }
    }

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check(request: Request):
    if request.method == "HEAD":
        return Response()
    return {"status": "healthy"}

@app.api_route("/articles", methods=["GET", "HEAD"])
async def get_articles(
    request: Request,
    limit: int = Query(50, ge=1, le=1000),
    source: Optional[str] = Query(None),
    search: Optional[str] = Query(None)
):
    if request.method == "HEAD":
        return Response()

    conn = get_db()
    cursor = conn.cursor()

    query = "SELECT id, source, title, url, summary, image_url, scraped_at FROM news WHERE 1=1"
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
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    conn.close()

    return {
        "total": len(results),
        "filters": {"limit": limit, "source": source, "search": search},
        "articles": results
    }

@app.api_route("/articles/latest", methods=["GET", "HEAD"])
async def get_latest_articles(request: Request, limit: int = Query(10, ge=1, le=100)):
    if request.method == "HEAD":
        return Response()

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, source, title, url, summary, image_url, scraped_at FROM news ORDER BY scraped_at DESC LIMIT ?",
        (limit,)
    )

    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()

    return {"total": len(results), "articles": results}

@app.api_route("/articles/sources", methods=["GET", "HEAD"])
async def get_sources(request: Request):
    if request.method == "HEAD":
        return Response()

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT source FROM news ORDER BY source")
    sources = [row[0] for row in cursor.fetchall()]
    conn.close()

    return {"total_sources": len(sources), "sources": sources}

@app.api_route("/articles/stats", methods=["GET", "HEAD"])
async def get_stats(request: Request):
    if request.method == "HEAD":
        return Response()

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM news")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT source, COUNT(*) as count FROM news GROUP BY source ORDER BY count DESC")
    by_source = [{"source": row[0], "count": row[1]} for row in cursor.fetchall()]

    cursor.execute("SELECT MAX(scraped_at) FROM news")
    last_updated = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(scraped_at) FROM news")
    first_article = cursor.fetchone()[0]

    conn.close()

    return {
        "total_articles": total,
        "articles_by_source": by_source,
        "last_updated": last_updated,
        "first_article": first_article
    }

@app.api_route("/articles/by-source/{source_name}", methods=["GET", "HEAD"])
async def get_articles_by_source(request: Request, source_name: str, limit: int = Query(50, ge=1, le=500)):
    if request.method == "HEAD":
        return Response()

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, source, title, url, summary, image_url, scraped_at FROM news WHERE source = ? ORDER BY scraped_at DESC LIMIT ?",
        (source_name, limit)
    )

    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()

    if not results:
        raise HTTPException(status_code=404, detail=f"No articles found for source: {source_name}")

    return {"source": source_name, "total": len(results), "articles": results}
