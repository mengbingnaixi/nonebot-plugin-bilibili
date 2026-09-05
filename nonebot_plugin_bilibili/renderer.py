from __future__ import annotations

import re
import sys
import base64
import asyncio
import mimetypes
from pathlib import Path
from contextlib import suppress
from urllib.parse import urlparse

from nonebot import logger
from jinja2.sandbox import SandboxedEnvironment
from playwright.async_api import async_playwright
from playwright.async_api import Browser, Playwright
from jinja2 import StrictUndefined, select_autoescape
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from .config import plugin_config
from .models import PushContent, RenderSelection

ALLOWED_IMAGE_HOST_SUFFIXES = (
    ".hdslb.com",
    ".biliimg.com",
    ".bilibili.com",
    "p.qlogo.cn",
)
DEFAULT_ACCENT_COLOR = "#fb7299"
COLOR_PATTERN = re.compile(r"^#[0-9a-fA-F]{6}$")
# Render at high-DPI so small text remains sharp after OneBot/QQ preview scaling.
RENDER_DEVICE_SCALE_FACTOR = 2


class CardRenderer:
    def __init__(self) -> None:
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._browser_lock = asyncio.Lock()
        self._environment = SandboxedEnvironment(
            autoescape=select_autoescape(("html", "xml")),
            undefined=StrictUndefined,
        )

    async def _get_browser(self) -> Browser:
        if self._browser is None:
            async with self._browser_lock:
                if self._browser is None:
                    self._playwright = await async_playwright().start()
                    self._browser = await self._playwright.chromium.launch(headless=True)
        return self._browser

    async def prepare(self) -> bool:
        """Ensure Chromium can be launched, installing it on first startup if needed."""
        try:
            await self._get_browser()
            return True
        except Exception as first_error:
            await self.close()
            if not plugin_config.bili_subscription_auto_install_browser:
                logger.opt(exception=first_error).error(
                    "Bilibili subscription: Chromium is unavailable and automatic "
                    "installation is disabled. Run `playwright install chromium`."
                )
                return False

        logger.info(
            "Bilibili subscription: Chromium is unavailable; installing Playwright "
            "browser resources for the current Python environment..."
        )
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "playwright",
                "install",
                "chromium",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            output, _ = await process.communicate()
        except Exception:
            logger.exception(
                "Bilibili subscription: unable to start the Chromium installer. "
                "Run `playwright install chromium` manually."
            )
            return False
        install_output = output.decode(errors="replace").strip()
        if process.returncode != 0:
            logger.error(
                "Bilibili subscription: Chromium installation failed (exit code {}). {}",
                process.returncode,
                install_output[-2000:],
            )
            return False

        try:
            await self._get_browser()
        except Exception:
            await self.close()
            logger.exception(
                "Bilibili subscription: Chromium was downloaded but could not be launched. "
                "On Linux, install its system libraries with "
                "`playwright install --with-deps chromium`."
            )
            return False

        logger.info("Bilibili subscription: Playwright Chromium is ready")
        return True

    async def close(self) -> None:
        if self._browser is not None:
            with suppress(Exception):
                await self._browser.close()
            self._browser = None
        if self._playwright is not None:
            with suppress(Exception):
                await self._playwright.stop()
            self._playwright = None

    def _font_css(self, font_path: str | None) -> str:
        if not font_path:
            return ""
        path = Path(font_path)
        if not path.is_file():
            return ""
        mime = mimetypes.guess_type(path.name)[0] or "font/ttf"
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        font_format = {
            ".otf": "opentype",
            ".ttf": "truetype",
            ".woff": "woff",
            ".woff2": "woff2",
        }.get(path.suffix.lower(), "truetype")
        return (
            "@font-face{font-family:'BiliCustom';"
            f"src:url(data:{mime};base64,{encoded}) format('{font_format}');font-display:block}}"
            "body,body *{font-family:'BiliCustom','Microsoft YaHei',sans-serif !important}"
        )

    def _template_text(self, content: PushContent, selection: RenderSelection) -> str:
        if selection.template_path:
            return Path(selection.template_path).read_text(encoding="utf-8")
        template_name = {
            "video": "video.html",
            "live": "live.html",
        }.get(content.kind.value, "dynamic.html")
        return (
            Path(__file__).resolve().parent
            .joinpath(f"resources/{template_name}")
            .read_text(encoding="utf-8")
        )

    async def render(self, content: PushContent, selection: RenderSelection) -> bytes:
        template_text = self._template_text(content, selection)
        template = self._environment.from_string(template_text)
        accent_color = (
            selection.accent_color
            if selection.accent_color and COLOR_PATTERN.fullmatch(selection.accent_color)
            else DEFAULT_ACCENT_COLOR
        )
        gradient_color = (
            selection.gradient_color
            if selection.gradient_color and COLOR_PATTERN.fullmatch(selection.gradient_color)
            else None
        )
        theme_background = (
            f"linear-gradient(135deg, {accent_color} 0%, {gradient_color} 100%)"
            if gradient_color
            else accent_color
        )
        theme_css = (
            f":root{{--bili-accent:{accent_color};"
            f"--bili-accent-2:{gradient_color or accent_color};"
            f"--bili-theme:{theme_background}}}"
        )
        font_css = self._font_css(selection.font_path)
        html = template.render(
            content=content,
            font_css=font_css + theme_css,
            accent_color=accent_color,
            gradient_color=gradient_color,
            theme_background=theme_background,
        )
        # Uploaded templates may omit the documented ``font_css`` placeholder.
        # Keep the selected font effective for those templates as well.
        if font_css and font_css not in html:
            style = f"<style>{font_css}</style>"
            html = html.replace("</head>", f"{style}</head>", 1)
            if style not in html:
                html = style + html
        browser = await self._get_browser()
        context = await browser.new_context(
            viewport={"width": plugin_config.bili_subscription_render_width, "height": 900},
            device_scale_factor=RENDER_DEVICE_SCALE_FACTOR,
            java_script_enabled=False,
        )
        page = await context.new_page()

        async def route_handler(route: object) -> None:
            request = route.request  # type: ignore[attr-defined]
            host = (urlparse(request.url).hostname or "").lower()
            if request.resource_type == "image" and any(
                host == suffix or host.endswith(suffix) for suffix in ALLOWED_IMAGE_HOST_SUFFIXES
            ):
                await route.continue_()  # type: ignore[attr-defined]
            else:
                await route.abort()  # type: ignore[attr-defined]

        await page.route("**/*", route_handler)
        try:
            await page.set_content(
                html,
                wait_until="domcontentloaded",
                timeout=plugin_config.bili_subscription_render_timeout,
            )
            with suppress(PlaywrightTimeoutError):
                await page.wait_for_load_state(
                    "networkidle",
                    timeout=plugin_config.bili_subscription_render_timeout,
                )
            with suppress(PlaywrightTimeoutError):
                await page.wait_for_function(
                    "Array.from(document.images).every((image) => image.complete)",
                    timeout=plugin_config.bili_subscription_render_timeout,
                )
            with suppress(PlaywrightTimeoutError):
                await page.evaluate("document.fonts.ready")
            card = page.locator("#bili-card")
            await card.wait_for(
                state="visible",
                timeout=plugin_config.bili_subscription_render_timeout,
            )
            return await card.screenshot(type="png")
        finally:
            await context.close()


renderer = CardRenderer()
