from __future__ import annotations

import asyncio

from nonebot import logger
from nonebot_plugin_apscheduler import scheduler

from .database import repository
from .config import plugin_config
from .pipeline import dispatch_content
from .models import LiveAction, PushContent
from .api import NoLiveRoomError, bilibili_api

_dynamic_lock = asyncio.Lock()
_live_lock = asyncio.Lock()
_dynamic_initialized_uids: set[int] = set()
_live_initialized_uids: set[int] = set()
DYNAMIC_JOB_ID = "bilibili_subscription_dynamic"
LIVE_JOB_ID = "bilibili_subscription_live"
IGNORED_DYNAMIC_TYPES = {"DYNAMIC_TYPE_LIVE_RCMD"}


def _dynamic_type(content: PushContent) -> str:
    return str(content.raw.get("type") or "")


def _is_ignored_dynamic(content: PushContent) -> bool:
    return _dynamic_type(content) in IGNORED_DYNAMIC_TYPES


def _ordered_dynamics(items: list[PushContent]) -> list[PushContent]:
    def sort_key(content: PushContent) -> tuple[int, int]:
        numeric_id = int(content.id) if content.id.isdigit() else 0
        return content.published_at, numeric_id

    # The space feed can place an old pinned item before newly published items and may
    # return the same dynamic in both pinned and chronological positions.
    ordered = sorted(items, key=sort_key, reverse=True)
    seen: set[str] = set()
    unique: list[PushContent] = []
    for item in ordered:
        if item.id in seen:
            continue
        seen.add(item.id)
        unique.append(item)
    return unique


async def load_runtime_config() -> None:
    dynamic_interval = await repository.get_setting("runtime_dynamic_interval")
    live_interval = await repository.get_setting("runtime_live_interval")
    enable_dynamic = await repository.get_setting("runtime_enable_dynamic")
    enable_live = await repository.get_setting("runtime_enable_live")
    if dynamic_interval.isdigit() and int(dynamic_interval) >= 30:
        plugin_config.bili_subscription_dynamic_interval = int(dynamic_interval)
    if live_interval.isdigit() and int(live_interval) >= 10:
        plugin_config.bili_subscription_live_interval = int(live_interval)
    if enable_dynamic in {"true", "false"}:
        plugin_config.bili_subscription_enable_dynamic = enable_dynamic == "true"
    if enable_live in {"true", "false"}:
        plugin_config.bili_subscription_enable_live = enable_live == "true"


async def save_runtime_config(
    dynamic_interval: int,
    live_interval: int,
    enable_dynamic: bool,
    enable_live: bool,
) -> None:
    await repository.set_setting("runtime_dynamic_interval", str(dynamic_interval))
    await repository.set_setting("runtime_live_interval", str(live_interval))
    await repository.set_setting("runtime_enable_dynamic", str(enable_dynamic).lower())
    await repository.set_setting("runtime_enable_live", str(enable_live).lower())
    plugin_config.bili_subscription_dynamic_interval = dynamic_interval
    plugin_config.bili_subscription_live_interval = live_interval
    plugin_config.bili_subscription_enable_dynamic = enable_dynamic
    plugin_config.bili_subscription_enable_live = enable_live
    install_jobs()


async def poll_dynamics() -> None:
    if _dynamic_lock.locked():
        return
    if not await repository.get_setting("bilibili_cookie"):
        return
    async with _dynamic_lock:
        semaphore = asyncio.Semaphore(plugin_config.bili_subscription_request_concurrency)
        failures: list[tuple[int, str]] = []

        async def check(uid: int) -> None:
            async with semaphore:
                try:
                    previous = await repository.get_state("dynamic", str(uid))
                    try:
                        items = await bilibili_api.get_dynamics(uid)
                    except Exception:
                        if previous is None:
                            await repository.set_state("dynamic", str(uid), "")
                        raise
                    if not items:
                        if previous is None:
                            await repository.set_state("dynamic", str(uid), "")
                        _dynamic_initialized_uids.add(uid)
                        return
                    items = _ordered_dynamics(items)
                    newest = items[0].id
                    first_check = uid not in _dynamic_initialized_uids
                    _dynamic_initialized_uids.add(uid)
                    if first_check or previous is None:
                        await repository.set_state("dynamic", str(uid), newest)
                        if first_check and previous is not None and previous != newest:
                            logger.info(
                                "Bilibili subscription: synchronized dynamic state for uid {} "
                                "after process startup",
                                uid,
                            )
                        return
                    if previous == "":
                        pending = []
                        for item in items:
                            pending.append(item)
                            if not _is_ignored_dynamic(item):
                                break
                    else:
                        pending = []
                        previous_found = False
                        for item in items:
                            if item.id == previous:
                                previous_found = True
                                break
                            pending.append(item)
                        if not previous_found:
                            await repository.set_state("dynamic", str(uid), newest)
                            logger.warning(
                                "Bilibili subscription: stale dynamic state {} for uid {} "
                                "was not present in the current feed; reset to {} without "
                                "delivering historical items",
                                previous,
                                uid,
                                newest,
                            )
                            return
                    pending = pending[: plugin_config.bili_subscription_initial_dynamic_limit]
                    for item in reversed(pending):
                        if _is_ignored_dynamic(item):
                            await repository.set_state("dynamic", str(uid), item.id)
                            continue
                        if not await dispatch_content(item):
                            logger.warning(
                                "Bilibili subscription: dynamic {} delivery incomplete; "
                                "will retry on the next poll",
                                item.id,
                            )
                            return
                        await repository.set_state("dynamic", str(uid), item.id)
                except Exception as exc:
                    failures.append((uid, str(exc)))
                    logger.opt(exception=True).debug(
                        "Bilibili subscription: dynamic polling failed for uid {}: {}",
                        uid,
                        exc,
                    )

        uids = await repository.list_uids()
        await asyncio.gather(*(check(uid) for uid in uids))
        if failures:
            logger.warning(
                "Bilibili subscription: dynamic polling failed for {}/{} subscriptions "
                "(e.g. uid {}: {}); details are available at DEBUG log level",
                len(failures),
                len(uids),
                failures[0][0],
                failures[0][1],
            )


async def poll_lives() -> None:
    if _live_lock.locked():
        return
    async with _live_lock:
        semaphore = asyncio.Semaphore(plugin_config.bili_subscription_request_concurrency)
        failures: list[tuple[int, str]] = []

        async def check(uid: int) -> None:
            async with semaphore:
                try:
                    content = await bilibili_api.get_live_by_uid(uid)
                    current = content.live_action or LiveAction.STOP
                    previous = await repository.get_state("live", str(uid))
                    first_check = uid not in _live_initialized_uids
                    if (
                        first_check
                        and previous is not None
                        and previous != current.value
                        and current is LiveAction.STOP
                    ):
                        await repository.set_state("live", str(uid), current.value)
                        _live_initialized_uids.add(uid)
                        logger.info(
                            "Bilibili subscription: synchronized stale live state for uid {}",
                            uid,
                        )
                        return
                    _live_initialized_uids.add(uid)
                    if previous is None:
                        if current is LiveAction.START:
                            if await dispatch_content(content):
                                await repository.set_state("live", str(uid), current.value)
                            else:
                                logger.warning(
                                    "Bilibili subscription: initial live {} delivery "
                                    "incomplete; will retry on the next poll",
                                    uid,
                                )
                        else:
                            await repository.set_state("live", str(uid), current.value)
                        return
                    if previous != current.value:
                        if await dispatch_content(content):
                            await repository.set_state("live", str(uid), current.value)
                        else:
                            logger.warning(
                                "Bilibili subscription: live {} delivery incomplete; "
                                "will retry on the next poll",
                                uid,
                            )
                except NoLiveRoomError:
                    return
                except Exception as exc:
                    failures.append((uid, str(exc)))
                    logger.opt(exception=True).debug(
                        "Bilibili subscription: live polling failed for uid {}: {}",
                        uid,
                        exc,
                    )

        uids = await repository.list_uids()
        await asyncio.gather(*(check(uid) for uid in uids))
        if failures:
            logger.warning(
                "Bilibili subscription: live polling failed for {}/{} subscriptions "
                "(e.g. uid {}: {}); details are available at DEBUG log level",
                len(failures),
                len(uids),
                failures[0][0],
                failures[0][1],
            )


def install_jobs() -> None:
    if plugin_config.bili_subscription_enable_dynamic:
        scheduler.add_job(
            poll_dynamics,
            "interval",
            seconds=plugin_config.bili_subscription_dynamic_interval,
            id=DYNAMIC_JOB_ID,
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
    elif scheduler.get_job(DYNAMIC_JOB_ID):
        scheduler.remove_job(DYNAMIC_JOB_ID)
    if plugin_config.bili_subscription_enable_live:
        scheduler.add_job(
            poll_lives,
            "interval",
            seconds=plugin_config.bili_subscription_live_interval,
            id=LIVE_JOB_ID,
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
    elif scheduler.get_job(LIVE_JOB_ID):
        scheduler.remove_job(LIVE_JOB_ID)
