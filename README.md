# US eBay scanner: coordinator and fast failover (v6.47)

The Blitz US feed is the preferred public pool. Existing PROXY_LIST, ProxyScrape Premium and Webshare remain independent backups. A working fixed Session stays in use. Store only the existing US feed token and coordinator HTTPS origin in Render Environment; credentials never belong in this repository.

## Selection and failover

- Actual recent eBay successes stay first, including SOCKS5. An immediate standby candidate must have local TLS/TCP readiness or real success; remote neutral health alone does not consume a ready slot.
- Fresh coordinator candidates normally receive about 75% of discovery/free standby slots, with an independent backup slot where available. After at least 12 actual discovery samples from each public group, this falls to 50% if coordinator success per second is clearly worse. Premium unlock, Webshare rescue delay, usage quotas and provider circuits still apply.
- Preserve finite remote ranking, neutral latency and US outcome history as soft ranking signals. Neutral TLS never proves eBay acceptance or clears local block/host cooldowns. Stable source labels include bounded coordinator provenance after a snapshot rotates.
- One metadata worker owns coordinator, legacy and managed API updates. Main failover reads existing snapshots immediately, including while a source is slow. A cold start waits within the existing discovery budget for initial metadata; source failure never deletes an unexpired cache or disables other providers.
- Coordinator snapshots contain at most 500 public endpoints, expire after at most 180 seconds, and retain the original retry backoff. Metadata refresh does not extend an old snapshot's expiry.

## Bounded speed and memory

- Background neutral TLS preparation targets 16 independent ready IPs, or 20 during repeated rotations at RSS below 300 MB. Six TLS workers, batches up to 18 and memory safeguards remain in place. Stagger up to four rechecks within 30 seconds of expiry rather than let the whole reserve expire together. These checks do not request eBay pages.
- Discovery ramps from 4 to 5 after 5 seconds and 6 after 8 seconds. At 18 seconds, eight probes are permitted only with RSS below 300 MB and at least 16 available independent IPs. A single global eight-probe limit includes unfinished earlier waves and handoff scouts, with no concurrent probe of the same IP. Fixed fetching and neutral TLS warming have their separate existing limits.
- Keep at most one additional Session from an already successful concurrent probe, without its HTML. It expires after 35 seconds and is closed on quarantine, pause or memory pressure. Adoption requests and validates a fresh normal search response; old HTML is never returned. Metered Webshare is not retained as an extra spare.
- Fixed-request timeouts shorten only after three measured successes and at least four local ready replacements. Floors are 10 seconds for normal reads and 6 seconds for recovery; unmeasured or slower endpoints keep their existing limits. A ready reserve skips a redundant recovery once the first failed request already took eight seconds.
- Main fetching has priority over new auction reads of the shared Session; the existing Session lock remains in force. Parser, durable item state, continuity protection, database leader locks and notification rules remain intact.

## Measurements and rollback

Every minute, bounded pipeline logs distinguish total intervals between successful main fetches from real network outages; they include p95 preparation/discovery/Session-lock time, actual request outcomes per source, fresh feed/TLS/known-good counts, active probes and spare count. No HTML, credentials or URLs are retained in this telemetry.

Unset coordinator settings to disable only that integration. To revert this release, use Render rollback to v6.46 commit 78b4e025a7645f84c209e82a8860d1a19bb108e0. Keep database and provider secrets intact. The free hosting service can still have platform outages; application tuning is not a hosting availability guarantee.

## Offline checks

Install requirements.txt and pytest (plus tzdata on Windows), then run `python -m pytest -q test_coordinator_feed.py`. All tests mock external services and start no production workers. Coverage includes cache expiry/failure, metadata validation and ranking, feedback order and latency, ready-queue eligibility, source fallback, global concurrency and unique-IP limits, spare ownership/expiry/quarantine, fresh-response adoption, adaptive timeout floors, bounded telemetry and main/auction Session priority.
