from __future__ import annotations

import asyncio
import re
from typing import Annotated

from nonebot.matcher import Matcher
from nonebot.params import CommandArg
from nonebot import logger, on_command
from nonebot.exception import MatcherException
from nonebot.adapters.onebot.v11 import Bot, Message, MessageEvent, MessageSegment

from .renderer import renderer
from .database import repository
from .config import plugin_config
from .pipeline import push_to_group
from .login import make_qr_png, wait_for_login
from .api import BilibiliApiError, bilibili_api
from .permissions import CommandContext, parse_context
from .models import AtAllKind, ContentKind, PushContent, RenderSelection

HELP_TEXT = """Bilibili 订阅推送
/bili add <uid>                 订阅 UP 主
/bili del <uid>                 取消订阅
/bili list                      查看本群订阅
/bili listall                   查看全部订阅（超管）
/bili delall                    清除本群订阅
/bili delallall                 清除全部订阅（超管）
/bili atall <类型> on|off <uid> 设置 @全体
/bili atall list                查看 @全体配置
/bili filter add <正则>         添加过滤规则
/bili filter del <规则或序号>   删除过滤规则
/bili filter list               查看过滤规则
/bili push dynamic <动态号>     手动推送动态
/bili push video <AV/BV号>      手动推送视频
/bili push live <直播间号>      手动推送直播卡片
/bili login                     扫码登录（超管）

超级用户可在私聊命令末尾添加 -群号。"""


bili = on_command("bili", aliases={"哔哩订阅"}, priority=10, block=True)


def _positive_int(value: str, label: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        raise ValueError(f"{label}必须是正整数")
    return int(value)


def _parse_uid(value: str) -> int:
    """Parse UID, accepting formats like "UID:9200471" from the Bilibili mobile app."""
    text = re.sub(r"^(?:uid\s*[:：]?\s*)", "", value.strip(), flags=re.IGNORECASE)
    if not text.isdigit() or int(text) <= 0:
        raise ValueError("UID 格式错误，请输入纯数字或 UID:数字")
    return int(text)


async def _group_name(bot: Bot, group_id: int) -> str:
    try:
        info = await bot.get_group_info(group_id=group_id, no_cache=False)
        return str(info.get("group_name") or "")
    except Exception:
        return ""


def _require_manage(ctx: CommandContext) -> None:
    if not ctx.can_manage:
        raise PermissionError("仅群主、群管理员或超级用户可执行此操作")


def _require_group(ctx: CommandContext) -> int:
    if ctx.group_id is None:
        raise ValueError("此命令需要在群内使用，或在私聊末尾添加 -群号")
    return ctx.group_id


def _require_super(ctx: CommandContext) -> None:
    if not ctx.is_superuser:
        raise PermissionError("此命令仅限超级用户")


async def _handle_add(matcher: Matcher, bot: Bot, ctx: CommandContext) -> None:
    _require_manage(ctx)
    if len(ctx.tokens) != 2:
        raise ValueError("用法：/bili add <uid> [-群号]")
    uid = _parse_uid(ctx.tokens[1])
    uname, avatar = await bilibili_api.get_profile(uid)
    added = await repository.add_subscription(
        ctx.group_id or 0,
        uid,
        uname,
        avatar,
        await _group_name(bot, ctx.group_id or 0),
    )
    await matcher.finish(f"已订阅 {uname}（{uid}）" if added else f"本群已订阅 {uname}（{uid}）")


async def _handle_del(matcher: Matcher, ctx: CommandContext) -> None:
    _require_manage(ctx)
    if len(ctx.tokens) != 2:
        raise ValueError("用法：/bili del <uid> [-群号]")
    uid = _parse_uid(ctx.tokens[1])
    removed = await repository.remove_subscription(ctx.group_id or 0, uid)
    await matcher.finish("已取消订阅" if removed else "本群没有该订阅")


async def _handle_list(matcher: Matcher, ctx: CommandContext) -> None:
    items = await repository.list_subscriptions(_require_group(ctx))
    if not items:
        await matcher.finish("本群暂无 Bilibili 订阅")
    lines = [f"{index}. {item.uname or 'UID'}（{item.uid}）" for index, item in enumerate(items, 1)]
    await matcher.finish("本群订阅：\n" + "\n".join(lines))


async def _handle_listall(matcher: Matcher, ctx: CommandContext) -> None:
    _require_super(ctx)
    items = await repository.list_subscriptions()
    if not items:
        await matcher.finish("当前没有任何订阅")
    lines = [f"群 {item.group_id}：{item.uname or 'UID'}（{item.uid}）" for item in items]
    await matcher.finish("全部订阅：\n" + "\n".join(lines))


async def _handle_clear(matcher: Matcher, ctx: CommandContext, all_groups: bool) -> None:
    if all_groups:
        _require_super(ctx)
        count = await repository.clear_all()
    else:
        _require_manage(ctx)
        count = await repository.clear_group(ctx.group_id or 0)
    await matcher.finish(f"已删除 {count} 条订阅")


async def _handle_atall(matcher: Matcher, ctx: CommandContext) -> None:
    group_id = _require_group(ctx)
    if len(ctx.tokens) == 2 and ctx.tokens[1].lower() == "list":
        configs = await repository.list_atall(group_id)
        if not configs:
            await matcher.finish("本群未开启任何 @全体 类型")
        lines = [f"{uid}：{', '.join(sorted(kinds))}" for uid, kinds in configs.items()]
        await matcher.finish("@全体配置：\n" + "\n".join(lines))

    _require_manage(ctx)
    if len(ctx.tokens) != 4:
        raise ValueError("用法：/bili atall <all|dynamic|video|music|article|live> on|off <uid>")
    try:
        kind = AtAllKind(ctx.tokens[1].lower())
    except ValueError as exc:
        raise ValueError("未知类型，可用：all dynamic video music article live") from exc
    switch = ctx.tokens[2].lower()
    if switch not in {"on", "off"}:
        raise ValueError("开关必须是 on 或 off")
    uid = _parse_uid(ctx.tokens[3])
    updated = await repository.set_atall(group_id, uid, kind, switch == "on")
    summary = ", ".join(sorted(updated)) if updated else "全部关闭"
    await matcher.finish(f"UID {uid} 的 @全体配置：{summary}")


async def _handle_filter(matcher: Matcher, ctx: CommandContext) -> None:
    group_id = _require_group(ctx)
    if len(ctx.tokens) == 2 and ctx.tokens[1].lower() == "list":
        rules = await repository.list_filters(group_id)
        if not rules:
            await matcher.finish("本群暂无过滤规则")
        lines = [f"{index}. {pattern}" for index, (_, pattern) in enumerate(rules, 1)]
        await matcher.finish("过滤规则：\n" + "\n".join(lines))

    _require_manage(ctx)
    if len(ctx.tokens) < 3:
        raise ValueError("用法：/bili filter add|del <关键词或序号>")
    action = ctx.tokens[1].lower()
    selector = " ".join(ctx.tokens[2:]).strip()
    if action == "add":
        added = await repository.add_filter(group_id, selector)
        await matcher.finish("过滤规则已添加" if added else "该过滤规则已存在")
    if action == "del":
        removed = await repository.remove_filter(group_id, selector)
        await matcher.finish("过滤规则已删除" if removed else "未找到该过滤规则")
    raise ValueError("过滤操作必须是 add、del 或 list")


async def _handle_push(matcher: Matcher, ctx: CommandContext) -> None:
    _require_manage(ctx)
    if len(ctx.tokens) != 3:
        raise ValueError("用法：/bili push dynamic|video|live <编号> [-群号]")
    kind = ctx.tokens[1].lower()
    identifier = ctx.tokens[2]
    if kind == "dynamic":
        content = await bilibili_api.get_dynamic(identifier)
    elif kind == "video":
        content = await bilibili_api.get_video(identifier)
    elif kind == "live":
        content = await bilibili_api.get_live_by_room(_positive_int(identifier, "直播间号"))
    else:
        raise ValueError("手动推送类型必须是 dynamic、video 或 live")
    sent = await push_to_group(ctx.group_id or 0, content, bypass_filters=True)
    await matcher.finish("推送成功" if sent else "推送失败，请查看日志")


async def _handle_login(matcher: Matcher, ctx: CommandContext) -> None:
    _require_super(ctx)
    session = await bilibili_api.create_qr_login()
    if not session.key or not session.url:
        raise RuntimeError("B 站未返回登录二维码")
    await matcher.send("请在 3 分钟内使用哔哩哔哩客户端扫码")
    try:
        await asyncio.wait_for(
            matcher.send(MessageSegment.image(make_qr_png(session.url))),
            timeout=20,
        )
    except Exception as exc:
        logger.warning("Bilibili subscription: failed to send QR image: {}", exc)
        public_url = plugin_config.bili_subscription_web_public_url.strip()
        fallback = f"\n也可打开 Web 管理后台登录：{public_url}" if public_url else ""
        await matcher.send(f"二维码图片发送失败，请打开以下链接：\n{session.url}{fallback}")
    try:
        await wait_for_login(session.key)
    except TimeoutError as exc:
        await matcher.finish(str(exc))
    await matcher.finish("Bilibili 登录成功，Cookie 已保存到插件数据目录")


async def _handle_help(matcher: Matcher, as_image: bool) -> None:
    if not as_image:
        await matcher.finish(HELP_TEXT)
    content = PushContent(
        id="help",
        uid=0,
        kind=ContentKind.DYNAMIC,
        title="Bilibili 订阅推送命令",
        text=HELP_TEXT,
        url="/bili help",
        author_name="NoneBot2",
    )
    try:
        image = await renderer.render(content, RenderSelection(None, None))
    except Exception:
        logger.exception("Bilibili subscription: failed to render command help")
        await matcher.finish(HELP_TEXT)
    await matcher.finish(MessageSegment.image(image))


@bili.handle()
async def handle_bili(
    matcher: Matcher,
    bot: Bot,
    event: MessageEvent,
    argument: Annotated[Message, CommandArg()],
) -> None:
    raw = argument.extract_plain_text().strip()
    tokens = raw.split() if raw else ["help"]
    try:
        if tokens[0].lower() == "help":
            as_image = len(tokens) > 1 and tokens[1].lower() in {"image", "img", "图"}
            await _handle_help(matcher, as_image)
        ctx = parse_context(event, tokens)
        command = ctx.tokens[0].lower() if ctx.tokens else "help"
        if command == "add":
            await _handle_add(matcher, bot, ctx)
        if command == "del":
            await _handle_del(matcher, ctx)
        if command == "list":
            await _handle_list(matcher, ctx)
        if command == "listall":
            await _handle_listall(matcher, ctx)
        if command == "delall":
            await _handle_clear(matcher, ctx, False)
        if command == "delallall":
            await _handle_clear(matcher, ctx, True)
        if command == "atall":
            await _handle_atall(matcher, ctx)
        if command == "filter":
            await _handle_filter(matcher, ctx)
        if command == "push":
            await _handle_push(matcher, ctx)
        if command == "login":
            await _handle_login(matcher, ctx)
        await matcher.finish("未知子命令，请使用 /bili help")
    except (ValueError, PermissionError, BilibiliApiError) as exc:
        await matcher.finish(str(exc))
    except MatcherException:
        raise
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Bilibili subscription: command failed: {}", exc)
        await matcher.finish("操作失败，请查看日志")
