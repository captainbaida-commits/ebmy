"""Offline search continuity recovery; real DB, eBay and Telegram are forbidden."""
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from test_coordinator_feed import scanner, forbid_unmocked_network


def item_ids(start, count=60):
    return {str(start + i) for i in range(count)}


@pytest.fixture
def continuity(scanner, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(scanner, 'time', SimpleNamespace(monotonic=lambda: clock[0], sleep=Mock()))
    monkeypatch.setattr(scanner, 'search_continuity_lock', threading.Lock())
    for name in ('search_continuity_trusted_ids', 'search_continuity_cold_candidate_ids',
                 'search_continuity_reconfirm_ids'):
        monkeypatch.setattr(scanner, name, set())
    for name in ('search_continuity_cold_candidate_count', 'search_continuity_cold_candidate_started',
                 'search_continuity_reconfirm_count', 'search_continuity_last_warn'):
        monkeypatch.setattr(scanner, name, 0)
    monkeypatch.setattr(scanner, 'search_continuity_reconfirm_started', None)
    monkeypatch.setattr(scanner, 'SEARCH_CONTINUITY_GUARD_ENABLED', True)
    monkeypatch.setattr(scanner, 'SEARCH_CONTINUITY_MIN_ITEMS', 40)
    monkeypatch.setattr(scanner, 'SEARCH_CONTINUITY_MIN_OVERLAP', .35)
    monkeypatch.setattr(scanner, 'SEARCH_CONTINUITY_COLD_MASS_NEW', 50)
    monkeypatch.setattr(scanner, 'SEARCH_CONTINUITY_COLD_CONFIRMATIONS', 2)
    monkeypatch.setattr(scanner, 'SEARCH_CONTINUITY_COLD_CONFIRM_WINDOW', 60)
    monkeypatch.setattr(scanner, 'SEARCH_CONTINUITY_COLD_CONFIRM_OVERLAP', .75)
    monkeypatch.setattr(scanner, 'adaptive_mark_main_issue', Mock())
    monkeypatch.setattr(scanner, '_memory_maintenance', Mock())
    monkeypatch.setattr(scanner, '_memory_rss_mb', lambda: 100)
    monkeypatch.setattr(scanner, 'EBAY_SEARCH_URL',
                        'https://www.ebay.com/sch/34017/i.html?_nkw=cross+stitch+kit&_sop=10')
    return SimpleNamespace(app=scanner, clock=clock)


def search_page(ids, title='Cross Stitch Kit in Cross Stitch Kits for sale | eBay', root=True):
    cards = ''.join(
        f'<li class="s-item"><a class="s-item__link" href="https://www.ebay.com/itm/{i}">'
        '<div class="s-item__title">Cross stitch kit</div></a>'
        '<span class="s-item__price">$9.99</span></li>' for i in sorted(ids)
    )
    return f'<title>{title}</title>' + (f'<ul id="srp-river-results">{cards}</ul>' if root else cards)


@pytest.mark.parametrize('start', [0.0, 1000.0])
def test_stale_warm_baseline_recovers_after_three_consistent_main_pages(continuity, start):
    app, clock = continuity.app, continuity.clock
    old, fresh = item_ids(100000000000), item_ids(200000000000)
    app._search_continuity_seed(old)
    for delta in (0, 10):
        clock[0] = start + delta
        assert not app._search_continuity_accept(fresh, allow_reconfirm=True)
        assert app.search_continuity_trusted_ids == old
    clock[0] = start + 20
    assert app._search_continuity_accept(fresh, allow_reconfirm=True)
    assert app.search_continuity_trusted_ids == fresh
    assert app.search_continuity_reconfirm_ids == set()


def test_single_outlier_and_return_to_normal_do_not_rebase(continuity):
    app = continuity.app
    old, outlier = item_ids(100000000000), item_ids(200000000000)
    app._search_continuity_seed(old)
    assert not app._search_continuity_accept(outlier, allow_reconfirm=True)
    assert app._search_continuity_accept(old)
    assert app.search_continuity_reconfirm_count == 0
    assert not app._search_continuity_accept(outlier, allow_reconfirm=True)
    assert app.search_continuity_reconfirm_count == 1


def test_alternating_universes_never_accumulate_confirmation(continuity):
    app, clock = continuity.app, continuity.clock
    old, a, b = [item_ids(n) for n in (100000000000, 200000000000, 300000000000)]
    app._search_continuity_seed(old)
    for i, ids in enumerate([a, b] * 8):
        clock[0] += 15
        assert not app._search_continuity_accept(ids, allow_reconfirm=True)
        assert app.search_continuity_trusted_ids == old


def test_confirmation_does_not_walk_from_one_result_universe_to_another(continuity):
    app, clock = continuity.app, continuity.clock
    old = item_ids(100000000000)
    app._search_continuity_seed(old)
    a, b, c = (item_ids(200000000000 + n) for n in (0, 15, 30))
    for ids in (a, b, c):
        clock[0] += 15
        assert not app._search_continuity_accept(ids, allow_reconfirm=True)
    assert app.search_continuity_trusted_ids == old
    assert app.search_continuity_reconfirm_count == 1


def test_fast_repeated_calls_cannot_skip_minimum_observation_time(continuity):
    app, clock = continuity.app, continuity.clock
    app._search_continuity_seed(item_ids(100000000000))
    fresh = item_ids(200000000000)
    for _ in range(5):
        assert not app._search_continuity_accept(fresh, allow_reconfirm=True)
    clock[0] += 20
    assert app._search_continuity_accept(fresh, allow_reconfirm=True)


def test_expired_confirmation_window_starts_again(continuity):
    app, clock = continuity.app, continuity.clock
    app._search_continuity_seed(item_ids(100000000000))
    fresh = item_ids(200000000000)
    assert not app._search_continuity_accept(fresh, allow_reconfirm=True)
    clock[0] += 301
    assert not app._search_continuity_accept(fresh, allow_reconfirm=True)
    assert app.search_continuity_reconfirm_count == 1


def test_normal_newest_sort_turnover_is_not_delayed(continuity):
    app = continuity.app
    old = item_ids(100000000000)
    app._search_continuity_seed(old)
    current = set(sorted(old)[15:]) | item_ids(200000000000, 15)
    assert app._search_continuity_accept(current)
    assert app.search_continuity_trusted_ids == current


@pytest.mark.parametrize('title,root,eligible', [
    ('Cross Stitch Kit in Cross Stitch Kits for sale | eBay', True, True),
    ('Cross Stitch Kit | eBay', False, False),
    ('Camera Kit for sale | eBay', True, False),
    ('Pardon our interruption', True, False),
])
def test_only_the_configured_main_search_can_reconfirm(continuity, title, root, eligible):
    app, clock = continuity.app, continuity.clock
    old, fresh = item_ids(100000000000), item_ids(200000000000)
    app._search_continuity_seed(old)
    items = app.parse_ebay_listings(search_page(fresh, title=title, root=root))
    assert items and set(items) == fresh
    allowed = all(item.get('_continuity_rebase_safe', False) for item in items.values())
    assert allowed is eligible
    for _ in range(2):
        clock[0] += 10
        assert not app._search_continuity_accept(items, allow_reconfirm=allowed)
    clock[0] += 10
    assert app._search_continuity_accept(items, allow_reconfirm=allowed) is eligible


def test_unsafe_page_interrupts_a_pending_confirmation(continuity):
    app, clock = continuity.app, continuity.clock
    app._search_continuity_seed(item_ids(100000000000))
    fresh = item_ids(200000000000)
    assert not app._search_continuity_accept(fresh, allow_reconfirm=True)
    clock[0] += 10
    assert not app._search_continuity_accept(fresh, allow_reconfirm=True)
    clock[0] += 10
    assert not app._search_continuity_accept(fresh, allow_reconfirm=False)
    clock[0] += 10
    assert not app._search_continuity_accept(fresh, allow_reconfirm=True)
    assert app.search_continuity_reconfirm_count == 1


def test_recovery_notifies_unseen_items_once_and_keeps_db_untouched_until_confirmed(continuity, monkeypatch):
    app, clock = continuity.app, continuity.clock
    old, fresh = item_ids(100000000000), item_ids(200000000000)
    app._search_continuity_seed(old)
    monkeypatch.setattr(app, 'fetch_ebay_html_with_retry', lambda: search_page(fresh))
    monkeypatch.setattr(app, 'runtime_guard', Mock())
    monkeypatch.setattr(app, '_verify_new_item_price_shipping', lambda item, **kw: item)
    seen = set(old)

    def claim(ids):
        claimed = set(ids) - seen
        seen.update(ids)
        return claimed

    claim_mock, send = Mock(side_effect=claim), Mock(return_value=True)
    monkeypatch.setattr(app, 'claim_new_seen_ids', claim_mock)
    monkeypatch.setattr(app, 'send_telegram_message', send)
    for delta in (0, 10):
        clock[0] = 1000 + delta
        assert app.check_and_send_new_items() == 'continuity_hold'
        claim_mock.assert_not_called()
        send.assert_not_called()
        assert seen == old
    clock[0] = 1020
    assert app.check_and_send_new_items() == 'ok'
    assert send.call_count == 60
    assert seen == old | fresh
    clock[0] = 1030
    assert app.check_and_send_new_items() == 'ok'
    assert send.call_count == 60
    next_page = set(sorted(fresh)[1:]) | item_ids(300000000000, 1)
    monkeypatch.setattr(app, 'fetch_ebay_html_with_retry', lambda: search_page(next_page))
    clock[0] = 1040
    assert app.check_and_send_new_items() == 'ok'
    assert send.call_count == 61
    assert item_ids(300000000000, 1) <= seen


def test_existing_cold_start_confirmation_is_preserved(continuity, monkeypatch):
    app, clock = continuity.app, continuity.clock
    fresh = item_ids(200000000000)
    monkeypatch.setattr(app, 'peek_unseen_seen_ids', lambda ids: set(ids))
    assert not app._search_continuity_accept(fresh)
    clock[0] += 10
    assert app._search_continuity_accept(fresh)

