"""Offline regression tests: never connect to eBay, Telegram or production DB."""
import json
import threading
import time
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from coordinator_feed import CoordinatorFeed


class Response:
    def __init__(self, data=None, status=200, raw=None):
        self.status_code = status
        self.body = raw if raw is not None else json.dumps(data).encode()
        self.closed = False

    def iter_content(self, chunk_size=8192):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]

    def close(self):
        self.closed = True


def row(ip='8.8.8.8', key='a', **extra):
    return dict(id=key * 64, proxy='http://%s:8080' % ip,
                managed=False, health_state='healthy', strong=True, **extra)


def payload(now, rows=None, market='us'):
    return dict(market=market, generated_at=datetime.fromtimestamp(
        now, timezone.utc).isoformat(), proxies=[row()] if rows is None else rows)


@pytest.fixture
def feed():
    clock = [time.time()]
    transport = Mock()
    transport.get.return_value = Response(payload(clock[0]))
    transport.post.return_value = Response(dict(ok=True, accepted=1))
    client = CoordinatorFeed('https://feed.example', 'test-only',
                             clock=lambda: clock[0], transport=transport)
    return client, clock, transport


def test_snapshot_retry_and_expiry_fail_open(feed, caplog):
    client, clock, transport = feed
    assert client.refresh()[0] == ('http://8.8.8.8:8080',)
    assert transport.get.call_args.kwargs['allow_redirects'] is False
    assert transport.get.return_value.closed
    client.refresh()
    assert transport.get.call_count == 1
    clock[0] += 61
    transport.get.return_value = Response(status=502)
    assert client.refresh()[0]
    client.refresh()
    assert transport.get.call_count == 2
    clock[0] += 120
    assert client.refresh()[0] == ()
    assert 'test-only' not in caplog.text
    assert '502' in caplog.text


def test_background_refresh_never_blocks_discovery(feed):
    client, clock, transport = feed
    client.refresh()
    clock[0] += 61
    entered, release = threading.Event(), threading.Event()
    def slow(*args, **kwargs):
        entered.set()
        release.wait(2)
        return Response(payload(clock[0]))
    transport.get.side_effect = slow
    worker = threading.Thread(target=client.refresh)
    worker.start()
    try:
        assert entered.wait(1)
        start = time.monotonic()
        assert client.refresh()[0]
        assert time.monotonic() - start < 0.2
    finally:
        release.set()
        worker.join(2)


@pytest.mark.parametrize('change', ['market', 'stale', 'future', 'managed', 'degraded',
                                    'auth', 'private', 'invalid_id', 'oversize'])
def test_bad_feed_cannot_replace_valid_snapshot(feed, change):
    client, clock, transport = feed
    before = client.refresh()
    clock[0] += 61
    data = payload(clock[0])
    if change == 'market':
        data['market'] = 'uk'
    elif change == 'stale':
        data = payload(clock[0] - 181)
    elif change == 'future':
        data = payload(clock[0] + 61)
    elif change == 'managed':
        data['proxies'][0]['managed'] = True
    elif change == 'degraded':
        data['proxies'][0]['health_state'] = 'degraded'
    elif change == 'auth':
        data['proxies'][0]['proxy'] = 'http://user:secret@8.8.8.8:8080'
    elif change == 'private':
        data['proxies'][0]['proxy'] = 'http://127.0.0.1:8080'
    elif change == 'invalid_id':
        data['proxies'][0]['id'] = 'bad'
    transport.get.return_value = Response(data, raw=b'x' * (1024 * 1024 + 1)
                                          if change == 'oversize' else None)
    assert client.refresh() == before
    assert transport.get.return_value.closed


def test_empty_valid_snapshot_clears_priority(feed):
    client, clock, transport = feed
    client.refresh()
    clock[0] += 61
    transport.get.return_value = Response(payload(clock[0], []))
    assert client.refresh()[0] == ()


@pytest.mark.parametrize('origin', ['', 'http://feed.example', 'https://x/path',
                                   'https://user:pass@x', 'https://[invalid'])
def test_bad_config_is_optional(origin):
    transport = Mock()
    client = CoordinatorFeed(origin, 'test-only', transport=transport)
    assert not client.enabled
    assert client.refresh()[0] == ()
    transport.get.assert_not_called()


def test_feedback_queues_real_results_without_network_and_flushes(feed):
    client, clock, transport = feed
    client.refresh()
    client.record_result('http://8.8.8.8:8080', 'success')
    client.record_result('http://8.8.8.8:8080', 'blocked')
    client.record_result('http://1.1.1.1:80', 'success')
    transport.post.assert_not_called()
    assert len(client.pending) == 1
    client.flush_feedback()
    sent = transport.post.call_args.kwargs['json']
    assert sent == dict(market='us', reports=[dict(proxy_key='a' * 64, result='success'),
                                            dict(proxy_key='a' * 64, result='blocked')])
    assert client.accepted_feedback == 1  # Count the server's acknowledgement, not guesses.
    assert transport.post.return_value.closed


def test_feedback_retains_success_and_only_latest_negative(feed):
    client, clock, transport = feed
    client.refresh()
    for result in ['success', 'success', 'blocked', 'proxy_timeout', 'proxy_ssl']:
        client.record_result('http://8.8.8.8:8080', result)
    client.flush_feedback()
    assert [r['result'] for r in transport.post.call_args.kwargs['json']['reports']] == ['success', 'ssl']


def test_feedback_later_real_success_supersedes_old_failure(feed):
    client, clock, transport = feed
    client.refresh()
    for result in ['success', 'blocked', 'success']:
        client.record_result('http://8.8.8.8:8080', result)
    client.flush_feedback()
    assert [r['result'] for r in transport.post.call_args.kwargs['json']['reports']] == ['success']


def test_feedback_pairs_stay_ordered_bounded_and_never_split(feed):
    client, clock, transport = feed
    for i in range(200):
        uri = 'http://8.8.%d.%d:80' % (i // 255, i % 255)
        client.ids[uri] = '%064x' % i
        client.record_result(uri, 'success')
        client.record_result(uri, 'blocked')
    assert len(client.pending) == 128
    assert sum(len(events) for events in client.pending.values()) == 256
    client.flush_feedback()
    reports = transport.post.call_args.kwargs['json']['reports']
    assert len(reports) == 64 and len(client.pending) == 96
    for before, after in zip(reports[::2], reports[1::2]):
        assert before['proxy_key'] == after['proxy_key']
        assert before['result'] == 'success' and after['result'] == 'blocked'


def test_feedback_bounded_and_not_replayed_after_ambiguous_failure(feed):
    client, clock, transport = feed
    for i in range(200):
        uri = 'http://8.8.%d.%d:80' % (i // 255, i % 255)
        client.ids[uri] = '%064x' % i
        client.record_result(uri, 'proxy_timeout')
    assert len(client.pending) == 128
    transport.post.side_effect = TimeoutError('contains test-only secret')
    client.flush_feedback()
    assert len(client.pending) == 64
    client.flush_feedback()
    assert transport.post.call_count == 1


@pytest.fixture
def scanner(monkeypatch):
    # No prod env file is present. Import starts neither workers nor DB requests.
    monkeypatch.setenv('COORDINATOR_BASE_URL', '')
    monkeypatch.setenv('COORDINATOR_US_TOKEN', '')
    monkeypatch.setenv('EBAY_SEARCH_URL', 'https://www.ebay.com/sch/i.html')
    monkeypatch.setenv('TELEGRAM_BOT_TOKEN', 'test-only')
    monkeypatch.setenv('TELEGRAM_CHAT_ID', 'test-only')
    monkeypatch.setenv('DATABASE_URL', 'postgresql://test:test@localhost/test')
    monkeypatch.setenv('PROXY_LIST', 'https://legacy.example')
    import app
    monkeypatch.setattr(app.provider_manager, 'candidates', lambda **kwargs: [])
    monkeypatch.setattr(app.provider_manager, 'source_fast', lambda p: 'free')
    return app


def test_coordinator_merge_preserves_legacy_and_local_ebay_cooldown(scanner, feed, monkeypatch):
    client, clock, transport = feed
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager('https://legacy.example')
    legacy = 'http://1.1.1.1:80'
    primary = 'http://8.8.8.8:8080'
    manager.proxies = manager.all_proxies = [legacy]
    manager.host_bad_until['8.8.8.8'] = clock[0] + 300
    manager.refresh_coordinator()
    assert primary in manager.all_proxies
    assert manager.proxies == [legacy]
    assert manager.host_bad_until['8.8.8.8'] > clock[0]
    assert primary not in manager.quality_ok_until
    assert primary not in manager.last_success_at


def test_primary_candidates_keep_known_good_and_parallel_legacy_slots(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:8080' % i for i in range(1, 5)]
    legacy = ['http://1.1.1.%d:80' % i for i in range(1, 5)]
    manager.proxies = manager.all_proxies = primary + legacy
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    manager.last_refresh = time.time()
    monkeypatch.setattr(manager, 'refresh_proxies', lambda *args, **kwargs: False)
    # Simulate legacy endpoints with an already successful neutral preflight.
    manager.quality_ok_until = {p: time.time() + 100 for p in legacy}
    batch = manager.get_candidate_batch(4, allow_premium=False)
    assert len(batch) == 4 and len(set(batch)) == 4
    assert all(p in primary for p in batch[:2])
    assert any(p in legacy for p in batch[2:])
    manager.last_success_at[legacy[0]] = time.time()
    assert manager.get_candidate_batch(4, allow_premium=False)[0] == legacy[0]
    manager.host_bad_until['8.8.8.1'] = time.time() + 100
    assert primary[0] not in manager.get_candidate_batch(4, allow_premium=False)
    manager.coordinator_valid_until = 0
    assert all(p in legacy for p in manager.get_candidate_batch(4, allow_premium=False))


def test_immediate_reserve_uses_primary_within_existing_budget(scanner):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:8080' % i for i in range(1, 5)]
    legacy = ['http://1.1.1.%d:80' % i for i in range(1, 8)]
    manager.proxies = primary + legacy
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    manager.quality_ok_until = {p: time.time() + 100 for p in legacy}
    reserve = manager.get_warm_standby_candidates(8)
    assert all(p in primary for p in reserve[:2])
    assert len(reserve) <= scanner.WARM_STANDBY_FREE_QUALITY_LIMIT
    manager.host_bad_until['8.8.8.1'] = time.time() + 100
    assert primary[0] not in manager.get_warm_standby_candidates(8)


def test_legacy_parser_retains_http_and_socks_rejects_malformed(scanner, monkeypatch):
    response = Response(raw=(b'8.8.8.8:80\nsocks5://1.1.1.1:1080\n'
                             b'https://9.9.9.9:443\nhttp://127.0.0.1:80\n'
                             b'http://user:secret@8.8.8.8:80\n<html>\n'
                             b'8.8.8.8:80\nhttp://8.8.4.4:99999\n'))
    monkeypatch.setattr(scanner.requests, 'get', Mock(return_value=response))
    manager = scanner.ProxyManager('https://legacy.example')
    assert set(manager.fetch_proxies_from_api()) == {
        'http://8.8.8.8:80', 'socks5://1.1.1.1:1080', 'https://9.9.9.9:443'}
    assert response.closed


def test_managed_backup_slots_and_webshare_guard_remain(scanner, monkeypatch):
    manager = scanner.ProxyManager()
    primary = ['http://8.8.8.%d:8080' % i for i in range(1, 5)]
    premium = ['http://9.9.9.%d:80' % i for i in range(1, 5)]
    webshare = ['http://1.1.1.%d:80' % i for i in range(1, 5)]
    def source(p):
        return ('proxyscrape_premium' if p in premium else
                'webshare' if p in webshare else 'free')
    monkeypatch.setattr(scanner.provider_manager, 'source_fast', source)
    monkeypatch.setattr(scanner.provider_manager, 'candidates',
                        lambda include_premium=True, include_webshare=False:
                        (premium if include_premium else []) +
                        (webshare if include_webshare else []))
    monkeypatch.setattr(scanner.provider_manager, 'usable_webshare_account_count', lambda: 4)
    monkeypatch.setattr(scanner.provider_manager, 'webshare_account_index_fast',
                        lambda p: webshare.index(p) if p in webshare else None)
    monkeypatch.setattr(scanner.provider_manager, 'managed_proxy_available', lambda p: True)
    monkeypatch.setattr(manager, 'refresh_proxies', lambda *args, **kwargs: False)
    manager.proxies = primary
    manager.coordinator_current = frozenset(primary)
    manager.coordinator_valid_until = time.time() + 180
    batch = manager.get_candidate_batch(4, allow_webshare=True)
    assert all(p in primary for p in batch[:2])
    assert sum(p in webshare for p in batch) <= 1
    assert any(p in premium for p in batch)
    locked = manager.get_candidate_batch(4, allow_webshare=False)
    assert not any(p in webshare for p in locked)


def test_feed_refill_does_not_discard_recent_working_proxy(scanner, feed, monkeypatch):
    client, clock, transport = feed
    monkeypatch.setattr(scanner, 'coordinator_feed', client)
    manager = scanner.ProxyManager('https://legacy.example')
    proven = 'http://9.9.9.9:80'
    manager.proxies = manager.all_proxies = [proven]
    manager.last_success_at[proven] = time.time()
    manager.refresh_coordinator()
    monkeypatch.setattr(manager, 'fetch_proxies_from_api', lambda **kwargs: ['http://1.1.1.1:80'])
    manager.refresh_proxies(force=True)
    assert proven in manager.proxies
    assert 'http://1.1.1.1:80' in manager.proxies
    assert 'http://8.8.8.8:8080' in manager.proxies
