"""
Sample plugin that fetches recent documents from the Federal Register API
and writes them to a table, with backfill for the past 24 hours.

Install: copy this file into your plugins directory

Usage:
    datasette tmp.db --plugins-dir=samples/
"""

from datasette import hookimpl
from datetime import datetime, timedelta, timezone
import httpx


API_URL = "https://www.federalregister.gov/api/v1/documents.json"


async def fetch_federal_register(datasette, config):
    """Fetch recent documents from the Federal Register API and upsert them."""
    db_name = config.get("database", "_memory")
    db = datasette.get_database(db_name)
    per_page = config.get("per_page", 20)
    backfill = config.get("backfill", False)

    params = {"per_page": per_page, "order": "newest"}

    if backfill:
        # Fetch documents from the past 24 hours
        yesterday = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
            "%m/%d/%Y"
        )
        params["conditions[publication_date][gte]"] = yesterday

    async with httpx.AsyncClient() as client:
        page = 1
        while True:
            params["page"] = page
            resp = await client.get(API_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not results:
                break

            for doc in results:
                agencies = ", ".join(
                    a.get("name") or a.get("raw_name", "")
                    for a in (doc.get("agencies") or [])
                )
                await db.execute_write(
                    """
                    INSERT OR REPLACE INTO federal_register_documents
                        (document_number, title, type, abstract, agencies,
                         publication_date, html_url, pdf_url, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        doc.get("document_number"),
                        doc.get("title"),
                        doc.get("type"),
                        doc.get("abstract"),
                        agencies,
                        doc.get("publication_date"),
                        doc.get("html_url"),
                        doc.get("pdf_url"),
                        datetime.now(timezone.utc).isoformat(),
                    ],
                )

            # Only paginate during backfill
            if not backfill or page >= data.get("total_pages", 1):
                break
            page += 1


@hookimpl
def cron_register_handlers(datasette):
    return {
        "federal-register-fetch": fetch_federal_register,
    }


@hookimpl
def startup(datasette):
    async def inner():
        # Find a mutable database
        db_name = None
        for name, db in datasette.databases.items():
            if name != "_internal" and db.is_mutable:
                db_name = name
                break

        if db_name is None:
            return

        db = datasette.get_database(db_name)
        await db.execute_write(
            """
            CREATE TABLE IF NOT EXISTS federal_register_documents (
                document_number TEXT PRIMARY KEY,
                title TEXT,
                type TEXT,
                abstract TEXT,
                agencies TEXT,
                publication_date TEXT,
                html_url TEXT,
                pdf_url TEXT,
                fetched_at TEXT NOT NULL
            )
            """
        )

        scheduler = datasette._cron_scheduler
        now = datetime.now(tz=None).isoformat()

        # Backfill: one-time fetch of past 24 hours
        await scheduler.add_task(
            name="federal-register-backfill",
            handler="federal-register-fetch",
            schedule={"interval": 999999},
            config={"database": db_name, "backfill": True, "per_page": 100},
            overlap="skip",
        )
        # Run immediately on startup
        await scheduler.internal_db.update_next_run("federal-register-backfill", now)

        # Regular polling: every 5 minutes, fetch the 20 newest
        await scheduler.add_task(
            name="federal-register-poll",
            handler="federal-register-fetch",
            schedule={"interval": 300},
            config={"database": db_name, "per_page": 20},
            overlap="skip",
        )
        # Run immediately on startup
        await scheduler.internal_db.update_next_run("federal-register-poll", now)

    return inner
