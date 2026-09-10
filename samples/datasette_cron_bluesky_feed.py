"""
Sample plugin that polls a Bluesky feed (Congress Members) for new posts
and writes them to a table.

Install: copy this file into your plugins directory

Usage:
    datasette tmp.db --plugins-dir=samples/

Also demonstrates a handler plugin adding its own telemetry:
`opentelemetry-api` only (no provider, no SDK - everything below is a
no-op until whoever runs Datasette turns tracing on). Context propagation
is via contextvars, so the spans opened here nest inside datasette-cron's
`datasette_cron.attempt` span automatically:

    datasette_cron.attempt
    └── cron_bluesky_feed.page        one per feed page
        ├── cron_bluesky_feed.fetch   the HTTP request
        └── cron_bluesky_feed.write   the single upsert query
            └── db.query              core's span, nested here in turn
"""

import json
from datetime import datetime

import httpx2
from datasette import hookimpl
from opentelemetry import metrics, trace

# Own instrumentation scope, named after the plugin (the `plugin` half of
# the handler reference), so this plugin's signals can be filtered
# independently of datasette-cron's and core's.
tracer = trace.get_tracer("cron_bluesky_feed")
meter = metrics.get_meter("cron_bluesky_feed")

posts_upserted = meter.create_counter(
    "cron_bluesky_feed.posts",
    unit="{post}",
    description="Bluesky posts upserted into the table",
)


BSKY_API = "https://public.api.bsky.app/xrpc"
FEED_URI = "at://did:plc:cr26c7oguulx6ipxdy6bf2it/app.bsky.feed.generator/aaageh42iayoa"


async def fetch_bluesky_feed(datasette, config):
    """Fetch posts from a Bluesky feed and upsert them."""
    db_name = config.get("database", "_memory")
    db = datasette.get_database(db_name)
    feed_uri = config.get("feed_uri", FEED_URI)
    limit = config.get("limit", 30)
    max_pages = config.get("max_pages", 1)

    async with httpx2.AsyncClient() as client:
        cursor = None
        for page in range(1, max_pages + 1):
            params = {"feed": feed_uri, "limit": limit}
            if cursor:
                params["cursor"] = cursor

            # One span per feed page. An exception raised inside a span's
            # context manager is recorded on it and marks it ERROR - both
            # are default behavior.
            with tracer.start_as_current_span(
                "cron_bluesky_feed.page",
                attributes={"cron_bluesky_feed.page": page},
            ) as span:
                # The HTTP request gets its own span, so network time and
                # write time are separable in the trace waterfall.
                with tracer.start_as_current_span(
                    "cron_bluesky_feed.fetch"
                ) as fetch_span:
                    resp = await client.get(
                        f"{BSKY_API}/app.bsky.feed.getFeed", params=params
                    )
                    fetch_span.set_attribute(
                        "http.response.status_code", resp.status_code
                    )
                    resp.raise_for_status()
                data = resp.json()

                feed_items = data.get("feed", [])
                span.set_attribute("cron_bluesky_feed.posts", len(feed_items))
                if not feed_items:
                    break

                fetched_at = datetime.now(tz=None).isoformat()
                rows = []
                for item in feed_items:
                    post = item.get("post", {})
                    author = post.get("author", {})
                    record = post.get("record", {})

                    # Extract embedded link if present
                    embed_url = None
                    embed = record.get("embed", {})
                    if embed.get("$type") == "app.bsky.embed.external":
                        embed_url = embed.get("external", {}).get("uri")

                    rows.append(
                        {
                            "uri": post.get("uri"),
                            "cid": post.get("cid"),
                            "author_did": author.get("did"),
                            "author_handle": author.get("handle"),
                            "author_name": author.get("displayName"),
                            "text": record.get("text"),
                            "embed_url": embed_url,
                            "created_at": record.get("createdAt"),
                            "indexed_at": post.get("indexedAt"),
                            "like_count": post.get("likeCount", 0),
                            "repost_count": post.get("repostCount", 0),
                            "reply_count": post.get("replyCount", 0),
                            "quote_count": post.get("quoteCount", 0),
                            "fetched_at": fetched_at,
                        }
                    )

                # One write per page: json_each() unrolls the JSON array
                # so the whole page upserts in a single query, instead of
                # one INSERT (and one trip through the write queue) per post.
                with tracer.start_as_current_span(
                    "cron_bluesky_feed.write",
                    attributes={"cron_bluesky_feed.rows": len(rows)},
                ):
                    await db.execute_write(
                        """
                        INSERT OR REPLACE INTO bluesky_congress_posts
                            (uri, cid, author_did, author_handle, author_name,
                             text, embed_url, created_at, indexed_at,
                             like_count, repost_count, reply_count, quote_count,
                             fetched_at)
                        SELECT
                            value ->> 'uri', value ->> 'cid',
                            value ->> 'author_did', value ->> 'author_handle',
                            value ->> 'author_name', value ->> 'text',
                            value ->> 'embed_url', value ->> 'created_at',
                            value ->> 'indexed_at', value ->> 'like_count',
                            value ->> 'repost_count', value ->> 'reply_count',
                            value ->> 'quote_count', value ->> 'fetched_at'
                        FROM json_each(?)
                        """,
                        [json.dumps(rows)],
                    )

                # The feed URI comes from task config, so it is a bounded
                # value and safe as a metric dimension.
                posts_upserted.add(len(rows), {"cron_bluesky_feed.feed": feed_uri})

            cursor = data.get("cursor")
            if not cursor:
                break


@hookimpl
def cron_register_handlers(datasette):
    return {
        "bluesky-feed-fetch": fetch_bluesky_feed,
    }


@hookimpl
def startup(datasette):
    async def inner():
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
            CREATE TABLE IF NOT EXISTS bluesky_congress_posts (
                uri TEXT PRIMARY KEY,
                cid TEXT,
                author_did TEXT,
                author_handle TEXT,
                author_name TEXT,
                text TEXT,
                embed_url TEXT,
                created_at TEXT,
                indexed_at TEXT,
                like_count INTEGER DEFAULT 0,
                repost_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                quote_count INTEGER DEFAULT 0,
                fetched_at TEXT NOT NULL
            )
            """
        )

        scheduler = datasette._cron_scheduler
        now = datetime.now(tz=None).isoformat()

        # Backfill: paginate to get recent history
        await scheduler.add_task(
            name="bluesky-congress-backfill",
            handler="cron_bluesky_feed:bluesky-feed-fetch",
            schedule={"interval": 999999},
            config={"database": db_name, "limit": 100, "max_pages": 10},
            overlap="skip",
        )
        await scheduler.internal_db.update_next_run("bluesky-congress-backfill", now)

        # Poll every 5 minutes
        await scheduler.add_task(
            name="bluesky-congress-poll",
            handler="cron_bluesky_feed:bluesky-feed-fetch",
            schedule={"interval": 300},
            config={"database": db_name, "limit": 30},
            overlap="skip",
        )
        await scheduler.internal_db.update_next_run("bluesky-congress-poll", now)

    return inner
