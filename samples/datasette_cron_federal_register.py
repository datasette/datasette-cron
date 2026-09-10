"""
Sample plugin that fetches recent documents from the Federal Register API
and writes them to a table, with backfill for the past 24 hours.

Install: copy this file into your plugins directory

Usage:
    datasette tmp.db --plugins-dir=samples/

Also demonstrates a handler plugin adding its own telemetry:
`opentelemetry-api` only (no provider, no SDK - everything below is a
no-op until whoever runs Datasette turns tracing on). Context propagation
is via contextvars, so the spans opened here nest inside datasette-cron's
`datasette_cron.attempt` span automatically:

    datasette_cron.attempt
    └── cron_federal_register.page        one per API page
        ├── cron_federal_register.fetch   the HTTP request
        └── cron_federal_register.write   the single upsert query
            └── db.query                  core's span, nested here in turn
"""

from datasette import hookimpl
from datetime import datetime, timedelta, timezone
import httpx2
import json

from opentelemetry import metrics, trace

# Own instrumentation scope, named after the plugin (the `plugin` half of
# the handler reference).
tracer = trace.get_tracer("cron_federal_register")
meter = metrics.get_meter("cron_federal_register")

documents_upserted = meter.create_counter(
    "cron_federal_register.documents",
    unit="{document}",
    description="Federal Register documents upserted into the table",
)


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

    async with httpx2.AsyncClient() as client:
        page = 1
        while True:
            params["page"] = page
            # One span per API page. An exception raised inside a span's
            # context manager is recorded on it and marks it ERROR - both
            # are default behavior.
            with tracer.start_as_current_span(
                "cron_federal_register.page",
                attributes={
                    "cron_federal_register.page": page,
                    "cron_federal_register.backfill": backfill,
                },
            ) as span:
                # The HTTP request gets its own span, so network time and
                # write time are separable in the trace waterfall.
                with tracer.start_as_current_span(
                    "cron_federal_register.fetch"
                ) as fetch_span:
                    resp = await client.get(API_URL, params=params)
                    fetch_span.set_attribute(
                        "http.response.status_code", resp.status_code
                    )
                    resp.raise_for_status()
                data = resp.json()

                results = data.get("results", [])
                span.set_attribute("cron_federal_register.documents", len(results))
                if not results:
                    break

                fetched_at = datetime.now(timezone.utc).isoformat()
                rows = [
                    {
                        "document_number": doc.get("document_number"),
                        "title": doc.get("title"),
                        "type": doc.get("type"),
                        "abstract": doc.get("abstract"),
                        "agencies": ", ".join(
                            a.get("name") or a.get("raw_name", "")
                            for a in (doc.get("agencies") or [])
                        ),
                        "publication_date": doc.get("publication_date"),
                        "html_url": doc.get("html_url"),
                        "pdf_url": doc.get("pdf_url"),
                        "fetched_at": fetched_at,
                    }
                    for doc in results
                ]

                # One write per page: json_each() unrolls the JSON array
                # so the whole page upserts in a single query, instead of
                # one INSERT (and one trip through the write queue) per
                # document.
                with tracer.start_as_current_span(
                    "cron_federal_register.write",
                    attributes={"cron_federal_register.rows": len(rows)},
                ):
                    await db.execute_write(
                        """
                        INSERT OR REPLACE INTO federal_register_documents
                            (document_number, title, type, abstract, agencies,
                             publication_date, html_url, pdf_url, fetched_at)
                        SELECT
                            value ->> 'document_number', value ->> 'title',
                            value ->> 'type', value ->> 'abstract',
                            value ->> 'agencies', value ->> 'publication_date',
                            value ->> 'html_url', value ->> 'pdf_url',
                            value ->> 'fetched_at'
                        FROM json_each(?)
                        """,
                        [json.dumps(rows)],
                    )

                # Backfill vs poll is the dimension an operator would
                # actually chart these by, and a bounded one.
                documents_upserted.add(
                    len(rows), {"cron_federal_register.backfill": backfill}
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
            handler="cron_federal_register:federal-register-fetch",
            schedule={"interval": 999999},
            config={"database": db_name, "backfill": True, "per_page": 100},
            overlap="skip",
        )
        # Run immediately on startup
        await scheduler.internal_db.update_next_run("federal-register-backfill", now)

        # Regular polling: every 5 minutes, fetch the 20 newest
        await scheduler.add_task(
            name="federal-register-poll",
            handler="cron_federal_register:federal-register-fetch",
            schedule={"interval": 300},
            config={"database": db_name, "per_page": 20},
            overlap="skip",
        )
        # Run immediately on startup
        await scheduler.internal_db.update_next_run("federal-register-poll", now)

    return inner
