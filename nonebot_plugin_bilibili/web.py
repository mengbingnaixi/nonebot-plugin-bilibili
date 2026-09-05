from __future__ import annotations

import time
import base64
import asyncio
import secrets
from pathlib import Path
from typing import Annotated
from hmac import compare_digest

from pydantic import Field, BaseModel
from nonebot.adapters.onebot.v11 import Bot
from nonebot import logger, get_app, get_bots
from fastapi import File, Depends, Request, APIRouter, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse, JSONResponse, RedirectResponse

from .renderer import renderer
from .database import repository
from .config import plugin_config
from .pipeline import push_to_group
from .jobs import save_runtime_config
from .login import make_qr_png, mask_cookie
from .api import BilibiliApiError, bilibili_api
from .paths import DATA_DIR, FONT_DIR, TEMPLATE_DIR, ensure_data_dirs
from .models import AtAllKind, LiveAction, ContentKind, PushContent, RenderSelection

MAX_UPLOAD_BYTES = 10 * 1024 * 1024
AUTH_COOKIE = "bili_subscription_token"
AUTH_MAX_AGE = 7 * 24 * 60 * 60
FONT_EXTENSIONS = {".ttf", ".otf", ".woff", ".woff2"}
TEMPLATE_EXTENSIONS = {".html", ".htm"}


class SubscriptionBody(BaseModel):
    group_id: int = Field(gt=0)
    uid: int = Field(gt=0)
    group_name: str = ""


class FilterBody(BaseModel):
    group_id: int = Field(gt=0)
    pattern: str


class AtAllBody(BaseModel):
    group_id: int = Field(gt=0)
    uid: int = Field(gt=0)
    kind: AtAllKind
    enabled: bool


class CookieBody(BaseModel):
    cookie: str


class OverrideBody(BaseModel):
    group_id: int = Field(gt=0)
    kind: str = "all"
    template_name: str | None = None
    font_name: str | None = None
    color: str | None = Field(default=None, pattern=r"^#[0-9a-fA-F]{6}$")
    gradient_color: str | None = Field(default=None, pattern=r"^#[0-9a-fA-F]{6}$")


class SessionBody(BaseModel):
    token: str


class ManualPushBody(BaseModel):
    group_id: int = Field(gt=0)
    kind: str
    identifier: str = Field(min_length=1, max_length=128)


class RuntimeConfigBody(BaseModel):
    dynamic_interval: int = Field(ge=30, le=86400)
    live_interval: int = Field(ge=10, le=86400)
    enable_dynamic: bool
    enable_live: bool


class PreviewBody(BaseModel):
    kind: str = "dynamic"
    identifier: str = Field(default="", max_length=128)
    template_name: str | None = None
    font_name: str | None = None
    color: str | None = Field(default=None, pattern=r"^#[0-9a-fA-F]{6}$")
    gradient_color: str | None = Field(default=None, pattern=r"^#[0-9a-fA-F]{6}$")


async def ensure_web_token() -> str:
    configured = plugin_config.bili_subscription_web_token.strip()
    if configured:
        return configured
    token = await repository.get_setting("web_token")
    if not token:
        token = secrets.token_urlsafe(24)
        await repository.set_setting("web_token", token)
    return token


async def _is_authorized(request: Request) -> bool:
    provided = (
        request.headers.get("x-bili-token")
        or request.query_params.get("token")
        or request.cookies.get(AUTH_COOKIE)
        or ""
    )
    expected = await ensure_web_token()
    return bool(provided) and compare_digest(provided, expected)


async def require_token(request: Request) -> None:
    if not await _is_authorized(request):
        raise HTTPException(status_code=401, detail="Invalid management token")


def _set_auth_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        AUTH_COOKIE,
        token,
        max_age=AUTH_MAX_AGE,
        httponly=True,
        samesite="strict",
        path=plugin_config.bili_subscription_web_path,
    )


def _safe_upload_name(filename: str | None, allowed: set[str]) -> str:
    name = Path(filename or "").name
    if not name or Path(name).suffix.lower() not in allowed:
        raise HTTPException(status_code=400, detail="Unsupported file type")
    return name


async def _save_upload(upload: UploadFile, directory: Path, allowed: set[str]) -> str:
    name = _safe_upload_name(upload.filename, allowed)
    content = await upload.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File is larger than 10 MiB")
    if not content:
        raise HTTPException(status_code=400, detail="File is empty")
    if directory is TEMPLATE_DIR:
        try:
            source = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(status_code=400, detail="Template must use UTF-8") from exc
        if 'id="bili-card"' not in source and "id='bili-card'" not in source:
            raise HTTPException(status_code=400, detail="Template must contain #bili-card")
    await asyncio.to_thread(_write_upload, directory, name, content)
    return name


def _write_upload(directory: Path, name: str, content: bytes) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_bytes(content)


router = APIRouter(prefix=plugin_config.bili_subscription_web_path)
protected = [Depends(require_token)]


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> Response:
    if await _is_authorized(request):
        return RedirectResponse(f"{plugin_config.bili_subscription_web_path}/", status_code=303)
    source = (
        Path(__file__).resolve().parent
        .joinpath("resources/login.html")
        .read_text(encoding="utf-8")
    )
    return HTMLResponse(source)


@router.post("/api/session")
async def create_session(body: SessionBody) -> JSONResponse:
    expected = await ensure_web_token()
    if not body.token or not compare_digest(body.token, expected):
        raise HTTPException(status_code=401, detail="管理令牌不正确")
    response = JSONResponse({"ok": True})
    _set_auth_cookie(response, expected)
    return response


@router.post("/api/logout", dependencies=protected)
async def logout() -> JSONResponse:
    response = JSONResponse({"ok": True})
    response.delete_cookie(AUTH_COOKIE, path=plugin_config.bili_subscription_web_path)
    return response


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> Response:
    query_token = request.query_params.get("token") or ""
    if query_token:
        expected = await ensure_web_token()
        if compare_digest(query_token, expected):
            response = RedirectResponse(
                f"{plugin_config.bili_subscription_web_path}/", status_code=303
            )
            _set_auth_cookie(response, expected)
            return response
    if not await _is_authorized(request):
        return RedirectResponse(
            f"{plugin_config.bili_subscription_web_path}/login", status_code=303
        )
    source = (
        Path(__file__).resolve().parent.joinpath("resources/web.html").read_text(encoding="utf-8")
    )
    return HTMLResponse(source)


@router.get("/api/state", dependencies=protected)
async def api_state() -> dict[str, object]:
    snapshot = await repository.export_snapshot()
    groups = snapshot.get("groups", [])
    for group in groups:  # type: ignore[union-attr]
        group_id = int(group["group_id"])
        group["filters"] = [pattern for _, pattern in await repository.list_filters(group_id)]
        atall = await repository.list_atall(group_id)
        group["atall"] = {str(uid): sorted(kinds) for uid, kinds in atall.items()}
        group["overrides"] = await repository.list_overrides(group_id)
    cookie = await repository.get_setting("bilibili_cookie")
    return {
        **snapshot,
        "cookie": mask_cookie(cookie),
        "fonts": sorted(path.name for path in FONT_DIR.glob("*") if path.is_file()),
        "templates": sorted(path.name for path in TEMPLATE_DIR.glob("*") if path.is_file()),
        "data_dir": str(DATA_DIR),
        "config": {
            "dynamic_interval": plugin_config.bili_subscription_dynamic_interval,
            "live_interval": plugin_config.bili_subscription_live_interval,
            "enable_dynamic": plugin_config.bili_subscription_enable_dynamic,
            "enable_live": plugin_config.bili_subscription_enable_live,
        },
    }


@router.get("/api/groups/scan", dependencies=protected)
async def scan_groups() -> dict[str, object]:
    bot = next((item for item in get_bots().values() if isinstance(item, Bot)), None)
    if bot is None:
        raise HTTPException(status_code=503, detail="OneBot V11 尚未连接")
    try:
        result = await bot.get_group_list()
    except Exception as exc:
        logger.exception("Bilibili subscription: failed to scan OneBot groups")
        raise HTTPException(status_code=502, detail="读取 QQ 群列表失败") from exc
    groups = []
    for item in result:
        group_id = int(item.get("group_id") or 0)
        if group_id <= 0:
            continue
        name = str(item.get("group_name") or f"QQ群 {group_id}")
        avatar = f"https://p.qlogo.cn/gh/{group_id}/{group_id}/100"
        groups.append(
            {
                "group_id": group_id,
                "group_name": name,
                "avatar": avatar,
                "member_count": int(item.get("member_count") or 0),
                "max_member_count": int(item.get("max_member_count") or 0),
            }
        )
        await repository.upsert_group(group_id, name, avatar)
    groups.sort(key=lambda item: str(item["group_name"]).casefold())
    return {"groups": groups}


@router.post("/api/subscriptions", dependencies=protected)
async def add_subscription(body: SubscriptionBody) -> dict[str, object]:
    try:
        uname, avatar = await bilibili_api.get_profile(body.uid)
    except BilibiliApiError as exc:
        logger.warning("获取 UP 主信息失败（{}），暂以 UID 作为昵称订阅", exc)
        uname, avatar = str(body.uid), ""
    added = await repository.add_subscription(
        body.group_id, body.uid, uname, avatar, body.group_name
    )
    return {"ok": True, "added": added, "uname": uname, "avatar": avatar}


@router.delete("/api/subscriptions/{group_id}/{uid}", dependencies=protected)
async def delete_subscription(group_id: int, uid: int) -> dict[str, object]:
    return {"ok": True, "removed": await repository.remove_subscription(group_id, uid)}


@router.post("/api/filters", dependencies=protected)
async def add_filter(body: FilterBody) -> dict[str, object]:
    return {"ok": True, "added": await repository.add_filter(body.group_id, body.pattern)}


@router.delete("/api/filters/{group_id}", dependencies=protected)
async def delete_filter(group_id: int, pattern: str) -> dict[str, object]:
    return {"ok": True, "removed": await repository.remove_filter_exact(group_id, pattern)}


@router.post("/api/atall", dependencies=protected)
async def set_atall(body: AtAllBody) -> dict[str, object]:
    switches = await repository.set_atall(body.group_id, body.uid, body.kind, body.enabled)
    return {"ok": True, "switches": sorted(switches)}


@router.post("/api/cookie", dependencies=protected)
async def set_cookie(body: CookieBody) -> dict[str, object]:
    await repository.set_setting("bilibili_cookie", body.cookie.strip())
    return {"ok": True, "cookie": mask_cookie(body.cookie)}


@router.post("/api/overrides", dependencies=protected)
async def set_override(body: OverrideBody) -> dict[str, object]:
    if body.kind not in {"dynamic", "video", "live"}:
        raise HTTPException(status_code=400, detail="Invalid content kind")
    if body.template_name and not (TEMPLATE_DIR / Path(body.template_name).name).is_file():
        raise HTTPException(status_code=400, detail="Template does not exist")
    if body.font_name and not (FONT_DIR / Path(body.font_name).name).is_file():
        raise HTTPException(status_code=400, detail="Font does not exist")
    await repository.set_override(
        body.group_id,
        body.kind,
        Path(body.template_name).name if body.template_name else None,
        Path(body.font_name).name if body.font_name else None,
        body.color.lower() if body.color else None,
        body.gradient_color.lower() if body.gradient_color else None,
    )
    return {"ok": True}


@router.post("/api/push", dependencies=protected)
async def manual_push(body: ManualPushBody) -> dict[str, object]:
    identifier = body.identifier.strip()
    try:
        if body.kind == "dynamic":
            content = await bilibili_api.get_dynamic(identifier)
        elif body.kind == "video":
            content = await bilibili_api.get_video(identifier)
        elif body.kind == "live":
            if not identifier.isdigit() or int(identifier) <= 0:
                raise HTTPException(status_code=400, detail="直播间号必须是正整数")
            content = await bilibili_api.get_live_by_room(int(identifier))
        else:
            raise HTTPException(status_code=400, detail="推送类型无效")
    except BilibiliApiError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    sent = await push_to_group(body.group_id, content, bypass_filters=True)
    if not sent:
        raise HTTPException(status_code=502, detail="推送失败，请检查 OneBot 连接和日志")
    return {
        "ok": True,
        "title": content.title,
        "author": content.author_name,
        "content_id": content.id,
    }


@router.post("/api/runtime", dependencies=protected)
async def update_runtime_config(body: RuntimeConfigBody) -> dict[str, object]:
    await save_runtime_config(
        body.dynamic_interval,
        body.live_interval,
        body.enable_dynamic,
        body.enable_live,
    )
    return {"ok": True}


def _preview_asset(directory: Path, name: str | None) -> str | None:
    if not name:
        return None
    path = (directory / Path(name).name).resolve()
    if path.parent != directory.resolve() or not path.is_file():
        raise HTTPException(status_code=400, detail="预览资源不存在")
    return str(path)


def _sample_preview_content(kind: ContentKind) -> PushContent:
    return PushContent(
        id="preview",
        uid=0,
        kind=kind,
        title="直播通知" if kind is ContentKind.LIVE else "示例推送标题",
        text="这是模板、字体与主题色组合后的渲染预览。",
        url="https://www.bilibili.com/",
        author_name="示例 UP 主",
        author_avatar="https://i0.hdslb.com/bfs/face/member/noface.jpg",
        cover="",
        published_at=int(time.time()) if kind is ContentKind.LIVE else 0,
        live_action=LiveAction.START if kind is ContentKind.LIVE else None,
    )


async def _preview_content(kind: ContentKind, identifier: str) -> PushContent:
    if not identifier:
        return _sample_preview_content(kind)
    if kind is ContentKind.DYNAMIC:
        return await bilibili_api.get_dynamic(identifier)
    if kind is ContentKind.VIDEO:
        return await bilibili_api.get_video(identifier)
    if not identifier.isdigit() or int(identifier) <= 0:
        raise HTTPException(status_code=400, detail="直播间号必须是正整数")
    return await bilibili_api.get_live_by_room(int(identifier))


@router.post("/api/preview", dependencies=protected)
async def preview_render(body: PreviewBody) -> dict[str, object]:
    if body.kind not in {"dynamic", "video", "live"}:
        raise HTTPException(status_code=400, detail="预览类型无效")
    kind = ContentKind(body.kind)
    try:
        content = await _preview_content(kind, body.identifier.strip())
    except HTTPException:
        raise
    except BilibiliApiError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    selection = RenderSelection(
        template_path=_preview_asset(TEMPLATE_DIR, body.template_name),
        font_path=_preview_asset(FONT_DIR, body.font_name),
        accent_color=body.color.lower() if body.color else None,
        gradient_color=body.gradient_color.lower() if body.gradient_color else None,
    )
    try:
        image = await renderer.render(content, selection)
    except Exception as exc:
        logger.exception("Bilibili subscription: preview rendering failed")
        raise HTTPException(status_code=400, detail=f"预览渲染失败：{exc}") from exc
    encoded = base64.b64encode(image).decode("ascii")
    return {"ok": True, "image": f"data:image/png;base64,{encoded}"}


@router.post("/api/upload/font", dependencies=protected)
async def upload_font(file: Annotated[UploadFile, File()]) -> dict[str, object]:
    return {"ok": True, "name": await _save_upload(file, FONT_DIR, FONT_EXTENSIONS)}


@router.post("/api/upload/template", dependencies=protected)
async def upload_template(file: Annotated[UploadFile, File()]) -> dict[str, object]:
    return {"ok": True, "name": await _save_upload(file, TEMPLATE_DIR, TEMPLATE_EXTENSIONS)}


@router.post("/api/login/qr", dependencies=protected)
async def login_qr() -> dict[str, object]:
    session = await bilibili_api.create_qr_login()
    png = base64.b64encode(make_qr_png(session.url)).decode("ascii")
    return {"key": session.key, "image": f"data:image/png;base64,{png}"}


@router.get("/api/login/poll", dependencies=protected)
async def login_poll(key: str) -> dict[str, object]:
    code, message, cookie = await bilibili_api.poll_qr_login(key)
    if code == 0 and cookie:
        await repository.set_setting("bilibili_cookie", cookie)
    success = code == 0 and bool(cookie)
    if code == 0 and not cookie:
        message = "登录已确认，但未获取到 Cookie，请重新生成二维码"
    return {"code": code, "message": message, "success": success}


_installed = False


def install_web() -> None:
    global _installed
    if _installed or not plugin_config.bili_subscription_web_enabled:
        return
    ensure_data_dirs()
    try:
        get_app().include_router(router)
    except Exception:
        logger.exception("Bilibili subscription web UI requires NoneBot's FastAPI driver")
        return
    _installed = True
    logger.info(
        "Bilibili subscription web UI enabled at {}/",
        plugin_config.bili_subscription_web_path,
    )
