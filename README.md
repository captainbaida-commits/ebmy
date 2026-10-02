# US eBay scanner: optional coordinator integration (v6.45)

The Blitz US feed adds an independent, prioritized public proxy pool. Existing
`PROXY_LIST`, ProxyScrape Premium and Webshare remain available with their original
cooldowns, circuits, traffic limits and rescue conditions. A proven working fixed
session is never replaced just to change its source.

Set `COORDINATOR_BASE_URL` to the coordinator HTTPS origin and
`COORDINATOR_US_TOKEN` to its existing **US feed** token in Render Environment.
Never commit credentials, use the admin token, or put a token in a URL. With either
setting absent or invalid, the original providers continue operating.

## Selection and outages

- Refresh metadata about once per minute in the existing reserve worker. No new
  threads, discovery concurrency changes, scan interval changes or extra eBay
  checks are introduced.
- Keep actual recent eBay successes first. Fresh coordinator endpoints get the
  first half of replacement batches, while the existing backup providers can
  occupy the remaining slots immediately under their existing conditions.
- Immediate free standby reserves use at most two coordinator slots within the
  existing free reserve budget. Remote neutral health never clears local eBay
  block, transport failure, host cooldown or outage history.
- Read at most 500 healthy, public, unauthenticated entries. Validate scheme,
  global IP, port, source timestamp and market. Do not treat transport health as
  proof that eBay accepts an address.
- Expire metadata after at most 180 seconds. Failed refreshes retain the last
  valid snapshot only until that original expiry; retries back off from 30 to
  300 seconds. A background refresh never makes discovery wait on its lock.
- Queue compact outcomes of existing real eBay requests. Send at most 64 reports
  per batch; cap the queue at 128 and identity history at 1,000. Do not replay a
  report after an ambiguous failure. No target-site requests are made by this
  adapter, and responses/credentials are not retained in logs.
- Legacy text downloads are capped at 2 MiB, checked for malformed entries and
  closed explicitly. Preserve the original `PROXY_LIST` value.

## Memory and rollback

The existing parser, durable seen-item state, continuity protection, leader locks,
memory guard, session recycling and all worker limits are unchanged. On a small
Linux instance, `MALLOC_ARENA_MAX=2` can reduce allocator fragmentation; monitor
memory and cadence after deployment, since this does not fix every native leak.
It does not change Python worker counts.

Unset the two coordinator settings to disable only the integration. To roll back
all code, deploy the preceding known-good revision through Render. Keep existing
database and provider environment values intact.

## Offline checks

Install `requirements.txt`, pytest and (on Windows) tzdata, then run
`python -m pytest -q test_coordinator_feed.py`. Tests mock network operations and
never contact production eBay, Telegram or the database. They cover outage/expiry,
concurrent refresh, invalid data, bounded reporting, primary/backup selection,
local cooldown precedence and managed provider safeguards.
