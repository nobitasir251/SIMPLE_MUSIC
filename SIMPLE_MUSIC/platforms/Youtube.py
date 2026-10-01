# -----------------------------------------------
# 🔸 SIMPLE MUSIC Project
# 🔹 Developed & Maintained by: Simple Boy (https://github.com/Simple-Boy-1k)
# 📅 Copyright © 2026 – All Rights Reserved
#
# 📖 License:
# This source code is open for educational and non-commercial use ONLY.
# You are required to retain this credit in all copies or substantial portions of this file.
# Commercial use, redistribution, or removal of this notice is strictly prohibited
# without prior written permission from the author.
#
# ❤️ Made with dedication and love by Simple_Boy_1k
# -----------------------------------------------

import asyncio
import os
import re
from typing import Union

import aiofiles
import aiohttp
import yt_dlp
from py_yt import Playlist, VideosSearch
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message

from SIMPLE_MUSIC import LOGGER
from SIMPLE_MUSIC.utils.formatters import time_to_seconds

try:
    from config import API_URL as CONFIG_API_URL
    from config import VIDEO_API_URL, API_KEY, YT_API_KEY, YTPROXY_URL
except ImportError:
    CONFIG_API_URL = None
    VIDEO_API_URL = None
    API_KEY = None
    YT_API_KEY = None
    YTPROXY_URL = None


API_URL = CONFIG_API_URL or "https://shrutibots.site"
DOWNLOAD_DIR = "downloads"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

CLIENT_SESSION = None


async def get_session():
    global CLIENT_SESSION

    if CLIENT_SESSION is None or CLIENT_SESSION.closed:
        CLIENT_SESSION = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0)
        )

    return CLIENT_SESSION


async def _download_stream(url, path, headers=None):
    try:
        session = await get_session()

        timeout = aiohttp.ClientTimeout(
            total=None,
            sock_read=20,
        )

        async with session.get(
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        ) as response:

            if response.status != 200:
                return None

            async with aiofiles.open(path, mode="wb") as file:
                async for chunk in response.content.iter_chunked(
                    2 * 1024 * 1024
                ):
                    await file.write(chunk)

            if os.path.exists(path) and os.path.getsize(path) > 1024:
                return path

    except Exception as e:
        LOGGER(__name__).warning(f"Stream download failed: {e}")

    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

    return None


async def engine_shrutibots(
    vid_id: str,
    is_video: bool,
    path: str,
) -> str:
    try:
        session = await get_session()

        media_type = "video" if is_video else "audio"

        async with session.get(
            f"{API_URL}/download",
            params={
                "url": vid_id,
                "type": media_type,
            },
            timeout=7,
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()
            token = data.get("download_token")

            if not token:
                return None

        stream_url = (
            f"{API_URL}/stream/{vid_id}"
            f"?type={media_type}&token={token}"
        )

        return await _download_stream(
            stream_url,
            path,
        )

    except Exception:
        return None


async def engine_xbit(
    vid_id: str,
    is_video: bool,
    path: str,
) -> str:

    if not YTPROXY_URL or not YT_API_KEY:
        return None

    try:
        session = await get_session()

        headers = {
            "x-api-key": YT_API_KEY,
        }

        async with session.get(
            f"{YTPROXY_URL}/info/{vid_id}",
            headers=headers,
            timeout=7,
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

        if data.get("status") != "success":
            return None

        url = (
            data.get("video_url")
            if is_video
            else data.get("audio_url")
        )

        if not url:
            return None

        return await _download_stream(
            url,
            path,
            headers,
        )

    except Exception:
        return None


async def engine_nexgen(
    vid_id: str,
    is_video: bool,
    path: str,
) -> str:

    if not API_KEY:
        return None

    try:
        if is_video:
            url = (
                f"{VIDEO_API_URL}/video/"
                f"{vid_id}?api={API_KEY}"
            )
        else:
            url = (
                f"{API_URL}/song/"
                f"{vid_id}?api={API_KEY}"
            )

        session = await get_session()

        async with session.get(
            url,
            timeout=7,
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

        if data.get("status", "").lower() != "done":
            return None

        download_url = data.get("link")

        if not download_url:
            return None

        return await _download_stream(
            download_url,
            path,
        )

    except Exception:
        return None


async def _core_download(
    link: str,
    is_video: bool,
) -> str:

    if "v=" in link:
        vid_id = link.split("v=")[-1].split("&")[0]
    else:
        vid_id = (
            link.rstrip("/")
            .split("/")[-1]
            .split("?")[0]
        )

    if not vid_id:
        return None

    extension = "mp4" if is_video else "mp3"

    final_path = os.path.join(
        DOWNLOAD_DIR,
        f"{vid_id}.{extension}",
    )

    if (
        os.path.exists(final_path)
        and os.path.getsize(final_path) > 1024
    ):
        return final_path

    tasks = [
        asyncio.create_task(
            engine_shrutibots(
                vid_id,
                is_video,
                f"{final_path}.shruti",
            )
        ),
        asyncio.create_task(
            engine_xbit(
                vid_id,
                is_video,
                f"{final_path}.xbit",
            )
        ),
        asyncio.create_task(
            engine_nexgen(
                vid_id,
                is_video,
                f"{final_path}.nexgen",
            )
        ),
    ]

    winner = None

    try:
        for future in asyncio.as_completed(tasks):
            try:
                result = await future

                if result:
                    winner = result
                    break

            except Exception:
                continue

    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

    if winner and os.path.exists(winner):
        try:
            os.replace(
                winner,
                final_path,
            )
            return final_path
        except Exception:
            return winner

    loop = asyncio.get_running_loop()

    def fallback_ytdl():
        if is_video:
            format_string = (
                "bestvideo[height<=480][fps<=30][ext=mp4]"
                "+bestaudio[ext=m4a]/best"
            )
        else:
            format_string = "bestaudio/best"

        options = {
            "format": format_string,
            "outtmpl": final_path,
            "quiet": True,
            "no_warnings": True,
            "nocheckcertificate": True,
            "ignoreerrors": True,
        }

        if not is_video:
            options["postprocessors"] = [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "192",
                }
            ]

        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.download([link])

    try:
        await loop.run_in_executor(
            None,
            fallback_ytdl,
        )
    except Exception as e:
        LOGGER(__name__).warning(
            f"yt-dlp fallback failed: {e}"
        )

    if (
        os.path.exists(final_path)
        and os.path.getsize(final_path) > 1024
    ):
        return final_path

    return None


async def download_song(link: str) -> str:
    return await _core_download(
        link,
        is_video=False,
    )


async def download_video(link: str) -> str:
    return await _core_download(
        link,
        is_video=True,
    )


class YouTubeAPI:

    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.listbase = "https://www.youtube.com/playlist?list="
        self.regex = r"(https?://)?(www\.)?(youtube\.com|youtu\.be)/.+"
        self.reg = re.compile(
            r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])"
        )

    async def exists(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):
        if videoid:
            link = self.base + link

        return bool(
            re.search(
                self.regex,
                link,
            )
        )

    async def url(
        self,
        message_1: Message,
    ) -> Union[str, None]:

        messages = [message_1]

        if message_1.reply_to_message:
            messages.append(
                message_1.reply_to_message
            )

        for message in messages:

            if message.entities:
                text = message.text or message.caption or ""

                for entity in message.entities:

                    if entity.type == MessageEntityType.URL:
                        return text[
                            entity.offset:
                            entity.offset + entity.length
                        ]

                    if (
                        entity.type
                        == MessageEntityType.TEXT_LINK
                    ):
                        return entity.url

            if message.caption_entities:
                text = message.caption or ""

                for entity in message.caption_entities:

                    if (
                        entity.type
                        == MessageEntityType.TEXT_LINK
                    ):
                        return entity.url

                    if entity.type == MessageEntityType.URL:
                        return text[
                            entity.offset:
                            entity.offset + entity.length
                        ]

        return None

    async def details(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):

        if videoid:
            link = self.base + link

        if "&" in link:
            link = link.split("&")[0]

        results = VideosSearch(
            link,
            limit=1,
        )

        data = await results.next()

        result_list = data.get("result", [])

        if not result_list:
            raise ValueError(
                "YouTube video not found"
            )

        result = result_list[0]

        title = result.get("title", "")
        duration = result.get("duration", "0:00")
        thumbnail = (
            result.get("thumbnails", [{}])[0]
            .get("url")
        )
        vidid = result.get("id")

        duration_sec = time_to_seconds(
            duration
        )

        return (
            title,
            duration,
            duration_sec,
            thumbnail,
            vidid,
        )

    async def title(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):

        return (
            await self.details(
                link,
                videoid,
            )
        )[0]

    async def duration(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):

        return (
            await self.details(
                link,
                videoid,
            )
        )[1]

    async def thumbnail(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):

        return (
            await self.details(
                link,
                videoid,
            )
        )[3]

    async def video(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):

        if videoid:
            link = self.base + link

        if "&" in link:
            link = link.split("&")[0]

        try:
            downloaded_file = await download_video(
                link
            )

            if downloaded_file:
                return 1, downloaded_file

            return 0, "Video download failed"

        except Exception as e:
            return 0, f"Video download error: {e}"

    async def playlist(
        self,
        link,
        limit,
        user_id,
        videoid: Union[bool, str] = None,
    ):

        if videoid:
            link = self.listbase + link

        if "&" in link:
            link = link.split("&")[0]

        try:
            playlist = await Playlist.get(link)
            videos = playlist.get("videos") or []

            return [
                data["id"]
                for data in videos[:limit]
                if data and data.get("id")
            ]

        except Exception:
            return []

    async def track(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):

        if videoid:
            link = self.base + link

        if "&" in link:
            link = link.split("&")[0]

        results = VideosSearch(
            link,
            limit=1,
        )

        data = await results.next()
        result_list = data.get("result", [])

        if not result_list:
            return None, None

        result = result_list[0]

        thumbnail = (
            result.get("thumbnails", [{}])[0]
            .get("url", "")
            .split("?")[0]
        )

        track_details = {
            "title": result.get("title"),
            "link": result.get("link"),
            "vidid": result.get("id"),
            "duration_min": result.get("duration"),
            "thumb": thumbnail,
        }

        return (
            track_details,
            result.get("id"),
        )

    async def formats(
        self,
        link: str,
        videoid: Union[bool, str] = None,
    ):

        if videoid:
            link = self.base + link

        if "&" in link:
            link = link.split("&")[0]

        formats_available = []

        options = {
            "quiet": True,
            "no_warnings": True,
        }

        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(
                    link,
                    download=False,
                )

                for fmt in info.get("formats", []):

                    if (
                        "dash"
                        in str(
                            fmt.get("format", "")
                        ).lower()
                    ):
                        continue

                    formats_available.append(
                        {
                            "format": fmt.get("format"),
                            "filesize": fmt.get("filesize"),
                            "format_id": fmt.get("format_id"),
                            "ext": fmt.get("ext"),
                            "format_note": fmt.get(
                                "format_note"
                            ),
                            "yturl": link,
                        }
                    )

        except Exception:
            pass

        return formats_available, link

    async def slider(
        self,
        link: str,
        query_type: int,
        videoid: Union[bool, str] = None,
    ):

        if videoid:
            link = self.base + link

        if "&" in link:
            link = link.split("&")[0]

        search = VideosSearch(
            link,
            limit=10,
        )

        data = await search.next()
        results = data.get("result", [])

        if not results:
            raise ValueError(
                "No YouTube results found"
            )

        result = results[query_type]

        thumbnail = (
            result.get("thumbnails", [{}])[0]
            .get("url", "")
            .split("?")[0]
        )

        return (
            result.get("title"),
            result.get("duration"),
            thumbnail,
            result.get("id"),
        )

    async def download(
        self,
        link: str,
        mystic,
        video: Union[bool, str] = None,
        videoid: Union[bool, str] = None,
        songaudio: Union[bool, str] = None,
        songvideo: Union[bool, str] = None,
        format_id: Union[bool, str] = None,
        title: Union[bool, str] = None,
    ) -> str:

        if videoid:
            link = self.base + link

        if "&" in link:
            link = link.split("&")[0]

        is_video = bool(video)

        try:
            if video:
                downloaded_file = await download_video(
                    link
                )
            else:
                downloaded_file = await download_song(
                    link
                )

            if downloaded_file:
                return downloaded_file, True

            return None, False

        except Exception as e:
            LOGGER(__name__).warning(
                f"Download error: {e}"
            )
            return None, False
