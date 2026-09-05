from __future__ import annotations

import re
import asyncio
import time
from typing import Any
from urllib.parse import urlparse, unquote_plus
from datetime import datetime, timezone, timedelta

import httpx

from nonebot import logger

from .database import repository
from .config import plugin_config
from .models import LiveAction, ContentKind, PushContent, LoginSession, RichTextSegment


class BilibiliApiError(RuntimeError):
    pass


class NoLiveRoomError(BilibiliApiError):
    pass


REQUEST_ATTEMPTS = 3
REQUEST_RETRY_DELAY = 0.5
# 连续多少个请求（每个请求内部已重试 REQUEST_ATTEMPTS 次）都因网络层错误失败时，
# 判定长期复用的连接池已失效，主动重建 HTTP 客户端再试一轮（等效于不重启 bot 重置网络层）。
CONSECUTIVE_FAILED_REQUESTS_TO_RESET = 2
# 重建冷却时间：网络持续故障时避免疯狂重建连接池
POOL_RESET_COOLDOWN_SECONDS = 30.0
BILIBILI_TIMEZONE = timezone(timedelta(hours=8))


class BilibiliApi:
    API = "https://api.bilibili.com"
    LIVE_API = "https://api.live.bilibili.com"
    PASSPORT_API = "https://passport.bilibili.com"

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None
        self._client_generation = 0
        self._client_lock = asyncio.Lock()
        self._consecutive_failed_requests = 0
        self._last_pool_reset = 0.0

    def _build_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=plugin_config.bili_subscription_request_timeout,
            follow_redirects=True,
            headers={
                "User-Agent": plugin_config.bili_subscription_user_agent,
                "Referer": "https://www.bilibili.com/",
            },
        )

    async def _get_client(self) -> tuple[httpx.AsyncClient, int]:
        """返回当前客户端及其代号；代号用于并发重建时避免误关新客户端。"""
        async with self._client_lock:
            if self._client is None:
                self._client = self._build_client()
                self._client_generation += 1
                logger.debug(
                    "Bilibili subscription: created new HTTP client (generation {})",
                    self._client_generation,
                )
            return self._client, self._client_generation

    async def close(self) -> None:
        async with self._client_lock:
            if self._client is not None:
                client, self._client = self._client, None
                self._client_generation += 1
                await client.aclose()

    async def _reset_client(self, generation: int) -> None:
        """丢弃疑似失效的连接池；若并发的其他请求已先行重建则直接跳过。"""
        async with self._client_lock:
            if self._client is None or generation != self._client_generation:
                return
            client, self._client = self._client, None
            self._client_generation += 1
            self._consecutive_failed_requests = 0
            try:
                await client.aclose()
            except Exception:
                logger.opt(exception=True).debug(
                    "Bilibili subscription: failed to close stale HTTP client"
                )

    async def _get_response(
        self, url: str, params: dict[str, Any] | None, cookies: dict[str, str] | None
    ) -> httpx.Response:
        """带重试的 GET；网络层持续失败时重建连接池（自愈）后额外重试一轮。"""
        rebuilt = False
        while True:
            last_error: httpx.TransportError | None = None
            for attempt in range(REQUEST_ATTEMPTS):
                client, generation = await self._get_client()
                try:
                    response = await client.get(url, params=params, cookies=cookies)
                    self._consecutive_failed_requests = 0
                    return response
                except httpx.TransportError as exc:
                    last_error = exc
                    if attempt < REQUEST_ATTEMPTS - 1:
                        await asyncio.sleep(REQUEST_RETRY_DELAY * (2**attempt))
            # 一整轮 REQUEST_ATTEMPTS 次全部失败
            self._consecutive_failed_requests += 1
            if (
                not rebuilt
                and self._consecutive_failed_requests
                >= CONSECUTIVE_FAILED_REQUESTS_TO_RESET
                and time.monotonic() - self._last_pool_reset
                > POOL_RESET_COOLDOWN_SECONDS
            ):
                rebuilt = True
                logger.warning(
                    "Bilibili subscription: 连续 {} 个请求连接 B 站失败，判定连接池失效，"
                    "正在重建 HTTP 客户端并再重试一轮",
                    self._consecutive_failed_requests,
                )
                await self._reset_client(generation)
                self._last_pool_reset = time.monotonic()
                continue
            attempts = REQUEST_ATTEMPTS * (2 if rebuilt else 1)
            raise BilibiliApiError(
                f"连接 B 站失败，已重试 {attempts} 次：{last_error}"
            ) from last_error

    async def _cookies(self) -> dict[str, str]:
        raw = await repository.get_setting("bilibili_cookie")
        result: dict[str, str] = {}
        for item in raw.split(";"):
            if "=" in item:
                key, value = item.strip().split("=", 1)
                if key:
                    result[key] = value
        return result

    async def _get_json(
        self, url: str, params: dict[str, Any] | None = None, *, use_cookie: bool = True
    ) -> dict[str, Any]:
        payload, _ = await self._request_json(url, params, use_cookie=use_cookie)
        return payload

    async def _request_json(
        self, url: str, params: dict[str, Any] | None = None, *, use_cookie: bool = True
    ) -> tuple[dict[str, Any], httpx.Response]:
        cookies = await self._cookies() if use_cookie else None
        response = await self._get_response(url, params, cookies)
        if response.status_code == 412 and use_cookie and cookies:
            # 失效/异常的 Cookie 可能触发 B 站风控（412），去掉 Cookie 匿名重试一次
            logger.debug(
                "B 站接口返回 412，Cookie 可能已失效，尝试去除 Cookie 匿名重试"
            )
            try:
                response = await self._get_response(url, params, None)
            except BilibiliApiError as exc:
                raise BilibiliApiError(
                    "B 站风控拒绝了请求（412），去除 Cookie 重试仍失败，请稍后重试或重新使用 /bili login 登录"
                ) from exc
        if response.status_code == 412:
            raise BilibiliApiError("B 站风控拒绝了请求，请先使用 /bili login 扫码登录并稍后重试")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise BilibiliApiError("B 站接口返回了无法识别的数据")
        code = int(payload.get("code", 0))
        if code == -352 and use_cookie and cookies:
            # 失效/异常的 Cookie 可能触发 B 站风控（-352），去掉 Cookie 匿名重试一次
            logger.debug(
                "B 站接口返回 -352，Cookie 可能已失效，尝试去除 Cookie 匿名重试"
            )
            try:
                response = await self._get_response(url, params, None)
                response.raise_for_status()
                payload = response.json()
            except (BilibiliApiError, httpx.HTTPError, ValueError):
                raise BilibiliApiError(
                    "B 站接口风控（-352），去除 Cookie 重试仍失败，请稍后重试或重新使用 /bili login 登录"
                ) from None
            if not isinstance(payload, dict):
                raise BilibiliApiError("B 站接口返回了无法识别的数据")
            code = int(payload.get("code", 0))
        if code != 0:
            message = payload.get("message") or payload.get("msg") or "未知错误"
            raise BilibiliApiError(f"B 站接口错误 {code}: {message}")
        return payload, response

    async def get_profile(self, uid: int) -> tuple[str, str]:
        # 主播资料接口匿名即可访问，不携带 Cookie，避免失效 Cookie 触发风控
        payload = await self._get_json(
            f"{self.API}/x/web-interface/card",
            {"mid": uid, "photo": "true"},
            use_cookie=False,
        )
        card = payload.get("data", {}).get("card", {})
        return str(card.get("name") or uid), str(card.get("face") or "")

    async def _get_anchor(self, uid: int) -> tuple[str, str]:
        # Fetch anchor name/avatar; retry once on failure, then fall back to the stored subscription name
        for attempt in range(2):
            try:
                return await self.get_profile(uid)
            except BilibiliApiError:
                if attempt == 0:
                    await asyncio.sleep(1)
                    continue
                fallback = await repository.get_uname(uid)
                return (fallback or str(uid)), ""
        return str(uid), ""  # pragma: no cover

    async def get_dynamics(self, uid: int) -> list[PushContent]:
        payload = await self._get_json(
            f"{self.API}/x/polymer/web-dynamic/v1/feed/space",
            {
                "host_mid": uid,
                "offset": "",
                "timezone_offset": -480,
                "features": "itemOpusStyle",
            },
        )
        items = payload.get("data", {}).get("items") or []
        return [self._parse_dynamic(item, uid) for item in items if item.get("id_str")]

    async def get_dynamic(self, dynamic_id: str) -> PushContent:
        payload = await self._get_json(
            f"{self.API}/x/polymer/web-dynamic/v1/detail",
            {
                "id": dynamic_id,
                "timezone_offset": -480,
                "features": "itemOpusStyle",
            },
        )
        item = payload.get("data", {}).get("item")
        if not item:
            raise BilibiliApiError("未找到该动态")
        author = item.get("modules", {}).get("module_author", {})
        return self._parse_dynamic(item, int(author.get("mid") or 0))

    def _parse_dynamic(self, item: dict[str, Any], uid: int) -> PushContent:
        modules = item.get("modules") or {}
        author = modules.get("module_author") or {}
        dynamic = modules.get("module_dynamic") or {}
        major = dynamic.get("major") or {}
        desc = dynamic.get("desc") or {}
        dynamic_type = str(item.get("type") or "")
        kind = {
            "DYNAMIC_TYPE_AV": ContentKind.VIDEO,
            "DYNAMIC_TYPE_MUSIC": ContentKind.MUSIC,
            "DYNAMIC_TYPE_ARTICLE": ContentKind.ARTICLE,
        }.get(dynamic_type, ContentKind.DYNAMIC)

        title = ""
        cover = ""
        images: list[str] = []
        image_dimensions: list[tuple[int, int]] = []
        jump_url = ""
        opus_text = ""
        rich_text: list[RichTextSegment] = []
        for key in ("archive", "article", "music", "opus", "draw", "common"):
            data = major.get(key)
            if not isinstance(data, dict):
                continue
            title = str(data.get("title") or data.get("desc") or title)
            jump_url = str(data.get("jump_url") or jump_url)
            images, image_dimensions = self._image_assets(data)
            cover = (images[0] if images else self._first_cover(data)) or cover
            if key == "opus":
                summary = data.get("summary") or {}
                if isinstance(summary, dict):
                    opus_text, rich_text = self._parse_rich_text(summary)
            break

        dynamic_id = str(item.get("id_str"))
        text = str(desc.get("text") or "") if isinstance(desc, dict) else ""
        if not rich_text and isinstance(desc, dict):
            desc_text, rich_text = self._parse_rich_text(desc)
            text = desc_text or text
        text = opus_text or text
        if not text:
            summary = (major.get("article") or {}).get("desc")
            text = str(summary or title)
        if jump_url.startswith("//"):
            jump_url = f"https:{jump_url}"
        url = jump_url or f"https://t.bilibili.com/{dynamic_id}"
        forward: PushContent | None = None
        original = item.get("orig")
        if dynamic_type == "DYNAMIC_TYPE_FORWARD" and isinstance(original, dict):
            original_author = (original.get("modules") or {}).get("module_author") or {}
            original_uid = int(original_author.get("mid") or 0)
            forward = self._parse_dynamic(original, original_uid)
        return PushContent(
            id=dynamic_id,
            uid=int(author.get("mid") or uid),
            kind=kind,
            title=title or self._kind_title(kind),
            text=text,
            url=url,
            author_name=str(author.get("name") or uid),
            author_avatar=self._normalize_image_url(author.get("face")),
            cover=cover,
            images=images,
            image_dimensions=image_dimensions,
            rich_text=rich_text,
            published_at=int(author.get("pub_ts") or 0),
            forward=forward,
            raw=item,
        )

    @classmethod
    def _parse_rich_text(cls, container: dict[str, Any]) -> tuple[str, list[RichTextSegment]]:
        nodes = container.get("rich_text_nodes") or []
        if not isinstance(nodes, list):
            nodes = []
        segments: list[RichTextSegment] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            text = str(node.get("orig_text") or node.get("text") or "")
            emoji = node.get("emoji") or {}
            emoji_url = ""
            if isinstance(emoji, dict):
                emoji_url = cls._normalize_image_url(emoji.get("icon_url") or emoji.get("url"))
            if text or emoji_url:
                segments.append(RichTextSegment(text=text, emoji_url=emoji_url))
        plain_text = "".join(segment.text for segment in segments)
        return plain_text or str(container.get("text") or ""), segments

    @classmethod
    def _first_cover(cls, data: dict[str, Any]) -> str:
        cover = data.get("cover")
        if isinstance(cover, str):
            return cls._normalize_image_url(cover)
        covers = data.get("covers")
        if isinstance(covers, list) and covers:
            first = covers[0]
            value = first.get("url") if isinstance(first, dict) else first
            return cls._normalize_image_url(value)
        return ""

    @classmethod
    def _image_urls(cls, data: dict[str, Any]) -> list[str]:
        return cls._image_assets(data)[0]

    @classmethod
    def _image_assets(cls, data: dict[str, Any]) -> tuple[list[str], list[tuple[int, int]]]:
        candidates = data.get("pics") or data.get("items") or []
        if not isinstance(candidates, list):
            return [], []
        urls: list[str] = []
        dimensions: list[tuple[int, int]] = []
        for item in candidates:
            value = item
            width = 0
            height = 0
            if isinstance(item, dict):
                value = item.get("url") or item.get("src") or item.get("cover")
                try:
                    width = max(0, int(item.get("width") or 0))
                    height = max(0, int(item.get("height") or 0))
                except (TypeError, ValueError):
                    width = height = 0
            url = cls._normalize_image_url(value)
            if url and url not in urls:
                urls.append(url)
                dimensions.append((width, height))
        return urls, dimensions

    @staticmethod
    def _normalize_image_url(value: object) -> str:
        url = str(value or "").strip()
        if url.startswith("//"):
            return f"https:{url}"
        if url.startswith("http://"):
            return f"https://{url[7:]}"
        return url

    @staticmethod
    def _parse_live_time(value: object) -> int:
        if isinstance(value, (int, float)):
            return max(0, int(value))
        text = str(value or "").strip()
        if not text or text.startswith("0000-00-00"):
            return 0
        if text.isdigit():
            return int(text)
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return 0
        return int(parsed.replace(tzinfo=BILIBILI_TIMEZONE).timestamp())

    @staticmethod
    def _kind_title(kind: ContentKind) -> str:
        return {
            ContentKind.DYNAMIC: "发布了新动态",
            ContentKind.VIDEO: "投稿了新视频",
            ContentKind.MUSIC: "发布了新音乐",
            ContentKind.ARTICLE: "发布了新专栏",
            ContentKind.LIVE: "直播通知",
        }[kind]

    async def get_video(self, identifier: str) -> PushContent:
        identifier = identifier.strip()
        params: dict[str, str | int]
        if identifier.upper().startswith("BV"):
            params = {"bvid": identifier}
        elif identifier.isdigit():
            params = {"aid": int(identifier)}
        else:
            raise BilibiliApiError("视频号必须是 AV 数字号或 BV 号")
        payload = await self._get_json(f"{self.API}/x/web-interface/view", params)
        data = payload.get("data") or {}
        owner = data.get("owner") or {}
        bvid = str(data.get("bvid") or identifier)
        return PushContent(
            id=bvid,
            uid=int(owner.get("mid") or 0),
            kind=ContentKind.VIDEO,
            title=str(data.get("title") or "视频投稿"),
            text=str(data.get("desc") or ""),
            url=f"https://www.bilibili.com/video/{bvid}",
            author_name=str(owner.get("name") or "未知 UP 主"),
            author_avatar=str(owner.get("face") or ""),
            cover=str(data.get("pic") or ""),
            published_at=int(data.get("pubdate") or 0),
            raw=data,
        )

    async def get_live_by_uid(self, uid: int, action: LiveAction | None = None) -> PushContent:
        payload = await self._get_json(f"{self.LIVE_API}/room/v1/Room/getRoomInfoOld", {"mid": uid})
        room = payload.get("data") or {}
        room_id = int(room.get("roomid") or 0)
        if not room_id:
            raise NoLiveRoomError("该用户没有直播间")
        return await self.get_live_by_room(room_id, action=action, fallback_uid=uid)

    async def get_live_by_room(
        self, room_id: int, action: LiveAction | None = None, fallback_uid: int = 0
    ) -> PushContent:
        payload = await self._get_json(
            f"{self.LIVE_API}/room/v1/Room/get_info",
            {"room_id": room_id},
        )
        room_info = payload.get("data") or {}
        uid = int(room_info.get("uid") or fallback_uid)
        anchor_name, anchor_avatar = await self._get_anchor(uid)
        actual_action = action
        if actual_action is None:
            actual_action = (
                LiveAction.START if int(room_info.get("live_status") or 0) == 1 else LiveAction.STOP
            )
        title = str(room_info.get("title") or "直播间")
        return PushContent(
            id=str(room_info.get("room_id") or room_id),
            uid=uid,
            kind=ContentKind.LIVE,
            title=("开播了：" if actual_action is LiveAction.START else "直播结束：") + title,
            text=str(room_info.get("area_name") or room_info.get("parent_area_name") or ""),
            url=f"https://live.bilibili.com/{room_info.get('room_id') or room_id}",
            author_name=anchor_name,
            author_avatar=anchor_avatar,
            cover=str(
                room_info.get("user_cover")
                or room_info.get("cover")
                or room_info.get("keyframe")
                or ""
            ),
            published_at=self._parse_live_time(room_info.get("live_time")),
            live_action=actual_action,
            raw=room_info,
        )

    async def create_qr_login(self) -> LoginSession:
        payload = await self._get_json(
            f"{self.PASSPORT_API}/x/passport-login/web/qrcode/generate", use_cookie=False
        )
        data = payload.get("data") or {}
        return LoginSession(key=str(data.get("qrcode_key") or ""), url=str(data.get("url") or ""))

    async def poll_qr_login(self, key: str) -> tuple[int, str, str]:
        payload, response = await self._request_json(
            f"{self.PASSPORT_API}/x/passport-login/web/qrcode/poll",
            {"qrcode_key": key},
            use_cookie=False,
        )
        data = payload.get("data") or {}
        code = int(data.get("code", -1))
        message = str(data.get("message") or "")
        cookie = ""
        if code == 0:
            callback_url = str(data.get("url") or "")
            cookie = self._login_cookie(callback_url, response)
        return code, message, cookie

    @staticmethod
    def _login_cookie(callback_url: str, response: httpx.Response) -> str:
        allowed = ("SESSDATA", "bili_jct", "DedeUserID", "DedeUserID__ckMd5", "sid")
        values: dict[str, str] = {}
        parsed = urlparse(callback_url)
        for source in (parsed.query, parsed.fragment):
            for item in source.split("&"):
                if "=" not in item:
                    continue
                raw_key, raw_value = item.split("=", 1)
                cookie_name = unquote_plus(raw_key)
                if cookie_name not in allowed:
                    continue
                values[cookie_name] = (
                    raw_value if cookie_name == "SESSDATA" else unquote_plus(raw_value)
                )
        for cookie_name in allowed:
            response_value = response.cookies.get(cookie_name)
            if response_value:
                values[cookie_name] = response_value
        cookie_pattern = re.compile(
            rf"(?:^|[,;]\s*)({'|'.join(map(re.escape, allowed))})=([^;,\s]+)"
        )
        for header in response.headers.get_list("set-cookie"):
            for cookie_name, response_value in cookie_pattern.findall(header):
                values[cookie_name] = response_value
        return "; ".join(
            f"{cookie_name}={values[cookie_name]}"
            for cookie_name in allowed
            if cookie_name in values
        )


bilibili_api = BilibiliApi()
