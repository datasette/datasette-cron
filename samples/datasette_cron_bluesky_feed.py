"""
Sample plugin that polls a Bluesky feed (Congress Members) for new posts
and writes them to a table.

Install: copy this file into your plugins directory

Usage:
    datasette tmp.db --plugins-dir=samples/
"""

from datasette import hookimpl
from datetime import datetime
import httpx


BSKY_API = "https://public.api.bsky.app/xrpc"
FEED_URI = "at://did:plc:cr26c7oguulx6ipxdy6bf2it/app.bsky.feed.generator/aaageh42iayoa"


async def fetch_bluesky_feed(datasette, config):
    """Fetch posts from a Bluesky feed and upsert them."""
    db_name = config.get("database", "_memory")
    db = datasette.get_database(db_name)
    feed_uri = config.get("feed_uri", FEED_URI)
    limit = config.get("limit", 30)
    max_pages = config.get("max_pages", 1)

    async with httpx.AsyncClient() as client:
        cursor = None
        for _ in range(max_pages):
            params = {"feed": feed_uri, "limit": limit}
            if cursor:
                params["cursor"] = cursor

            resp = await client.get(f"{BSKY_API}/app.bsky.feed.getFeed", params=params)
            resp.raise_for_status()
            data = resp.json()

            feed_items = data.get("feed", [])
            if not feed_items:
                break

            for item in feed_items:
                post = item.get("post", {})
                author = post.get("author", {})
                record = post.get("record", {})

                # Extract embedded link if present
                embed_url = None
                embed = record.get("embed", {})
                if embed.get("$type") == "app.bsky.embed.external":
                    embed_url = embed.get("external", {}).get("uri")

                await db.execute_write(
                    """
                    INSERT OR REPLACE INTO bluesky_congress_posts
                        (uri, cid, author_did, author_handle, author_name,
                         text, embed_url, created_at, indexed_at,
                         like_count, repost_count, reply_count, quote_count,
                         fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        post.get("uri"),
                        post.get("cid"),
                        author.get("did"),
                        author.get("handle"),
                        author.get("displayName"),
                        record.get("text"),
                        embed_url,
                        record.get("createdAt"),
                        post.get("indexedAt"),
                        post.get("likeCount", 0),
                        post.get("repostCount", 0),
                        post.get("replyCount", 0),
                        post.get("quoteCount", 0),
                        datetime.now(tz=None).isoformat(),
                    ],
                )

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
            handler="bluesky-feed-fetch",
            schedule={"interval": 999999},
            config={"database": db_name, "limit": 100, "max_pages": 10},
            overlap="skip",
        )
        await scheduler.internal_db.update_next_run("bluesky-congress-backfill", now)

        # Poll every 5 minutes
        await scheduler.add_task(
            name="bluesky-congress-poll",
            handler="bluesky-feed-fetch",
            schedule={"interval": 300},
            config={"database": db_name, "limit": 30},
            overlap="skip",
        )
        await scheduler.internal_db.update_next_run("bluesky-congress-poll", now)

    return inner
