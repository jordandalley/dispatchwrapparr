#!/usr/bin/env python3

"""
Dispatchwrapparr - A super wrapper for Dispatcharr
"""

from __future__ import annotations
import os
import sys
import re
import signal
import logging
import argparse
import requests
import fnmatch
import json
import subprocess
import tempfile
import hashlib
import threading
import time
import http.cookiejar
import m3u8
from urllib.parse import urlparse, parse_qs
from streamlink.exceptions import PluginError, NoPluginError
from streamlink.session import Streamlink
from streamlink.stream.ffmpegmux import MuxedStream
from streamlink.stream.hls import HLSStream
from streamlink.stream.http import HTTPStream
from streamlink.stream.stream import Stream
from streamlink.options import Options

__version__ = "1.7.0"

def parse_args():
    # Initial wrapper arguments
    parser = argparse.ArgumentParser(description="Dispatchwrapparr: A super wrapper for Dispatcharr")
    parser.add_argument("-i", required=True, help="Required: Stream URL")
    parser.add_argument("-ua", required=True, help="Required: User-Agent string")
    parser.add_argument("-proxy", help="Optional: HTTP proxy server (e.g. http://127.0.0.1:8888)")
    parser.add_argument("-proxybypass", help="Optional: Comma-separated list of hostnames or IP patterns to bypass the proxy (e.g. '192.168.*.*,*.lan')")
    parser.add_argument("-clearkeys", help="Optional: Supply a json file or URL containing URL/Clearkey maps (e.g. 'clearkeys.json' or 'https://some.host/clearkeys.json')")
    parser.add_argument("-cookies", help="Optional: Supply a cookie jar txt file in Mozilla/Netscape format (e.g. 'cookies.txt')")
    parser.add_argument("-customheaders", help="Optional: Supply custom headers as a JSON string (e.g. '{\"Authentication\": \"Bearer token\"}')")
    parser.add_argument("-streamlink_plugins", help="Optional: Specify a custom path for Streamlink plugins")
    parser.add_argument("-stream", help="Optional: Supply streamlink stream selection argument (eg. best, worst, 1080p, 1080p_alt, etc)")
    parser.add_argument("-ffmpeg", help="Optional: Specify a custom ffmpeg binary path")
    parser.add_argument("-ffmpeg_transcode_audio", help="Optional: When muxing with ffmpeg, specify an output audio format (eg. aac, eac3, ac3, copy)")
    parser.add_argument("-ffmpeg_nocopyts", action="store_true", help="Optional: Disable the copying of timestamps during muxing with ffmpeg")
    parser.add_argument("-novariantcheck", action="store_true", help="Optional: Do not autodetect if stream is audio-only or video-only")
    parser.add_argument("-novideo", action="store_true", help="Optional: Forces muxing of a blank video track into a stream that contains no audio")
    parser.add_argument("-noaudio", action="store_true", help="Optional: Forces muxing of a silent audio track into a stream that contains no video")
    parser.add_argument("-nosonginfo", action="store_true", help="Optional: Disable song information during streaming radio plays")
    parser.add_argument("-loglevel", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"], help="Enable logging and set log level. (default: INFO)")
    parser.add_argument("-v", "--version", action="version", version=f"Dispatchwrapparr {__version__}")
    # Deprecated features
    parser.add_argument("-subtitles", action="store_true", help=argparse.SUPPRESS, deprecated=True)
    args = parser.parse_args()

    # Enforce dependency for proxybypass, must be used with proxy
    if args.proxybypass and not args.proxy:
        parser.error("Argument -proxybypass: requires -proxy to be set")

    # Ensure that novariantcheck, novideo, noaudio, and clearkeys are not specified simultaneously
    flags = [args.novideo, args.noaudio, args.novariantcheck, args.clearkeys]
    if sum(bool(f) for f in flags) > 1:
        parser.error("Arguments -novariantcheck, -novideo, -noaudio and -clearkeys can only be used individually")

    # Check if files and directories exist
    if args.ffmpeg:
        if not os.path.isfile(args.ffmpeg):
            parser.error(f"Argument -ffmpeg: The file '{args.ffmpeg}' does not exist!")

    if args.streamlink_plugins:
        if not os.path.isdir(args.streamlink_plugins):
            parser.error(f"Argument -streamlink_plugins: The path '{args.streamlink_plugins}' does not exist!")

    return args

def configure_logging(level="INFO") -> logging.Logger:
    """
    Set up console logging for both the script and Streamlink.

    Args:
        level (str): Logging level. One of: "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET".

    Returns:
        logging.Logger: Configured logger instance.
    """
    level = level.upper()
    numeric_level = getattr(logging, level, logging.INFO)

    # Set root logger (used by Streamlink internally)
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    if not root_logger.handlers:
        formatter = logging.Formatter("[%(name)s] %(asctime)s [%(levelname)s] %(message)s")
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root_logger.addHandler(console)

    # Ensure streamlink logger is not being filtered or silenced
    streamlink_log = logging.getLogger("streamlink")
    streamlink_log.setLevel(numeric_level)
    streamlink_log.propagate = True

    # Your application logger
    log = logging.getLogger("dispatchwrapparr")
    return log

class PlayRadio:
    """
    A class that mimicks Streamlink stream.open() by using a file-like
    object that wraps a radio stream through FFmpeg, muxing blank video in or in
    the case of available metadata, displays song information for use on TV's.
    """

    def __init__(self, url, ffmpeg, ffmpeg_loglevel, headers, cookies, stream_type=None, resolution="854x480", fps=25, acodec="aac", vcodec="libx264", fontsize=22, update_interval=5):
        self.url = url
        self.stream_type = stream_type
        self.ffmpeg = ffmpeg
        self.ffmpeg_loglevel = ffmpeg_loglevel
        self.headers = headers or {}
        self.cookies = cookies or {}
        self.resolution = resolution
        self.fps = fps
        self.acodec = acodec
        self.vcodec = vcodec
        self.fontsize = fontsize
        self.update_interval = update_interval
        self.process = None
        self.metafile = self.generate_temp_metafile()
        self.session = requests.session()
        self.session.headers.update(self.headers)
        self.session.cookies.update(self.cookies)
        # event to signal the metadata thread to stop
        self._stop_metadata_thread = threading.Event()
        self._metadata_thread = None

    def open(self):
        """
        Launch FFmpeg and return a file-like object (self) for reading stdout.
        """
        cmd = [
            self.ffmpeg,
            "-hide_banner",
            "-loglevel", self.ffmpeg_loglevel,
        ]

        # add headers
        for k, v in self.headers.items():
            cmd.extend(["-headers", f"{k}: {v}"])

        # add cookies
        if self.cookies:
            cookie_str = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
            cmd.extend(["-cookies", cookie_str])

        cmd.extend([
            "-i", self.url,
            "-f", "lavfi",
            "-i", f"color=size={self.resolution}:rate={self.fps}:color=black"
        ])

        if self.stream_type:
            log.info(f"Creating metadatafile at '{self.metafile}' for '{self.stream_type}' stream")
            self._stop_metadata_thread.clear()
            self._metadata_thread = threading.Thread(target=self.update_metadata, daemon=True)
            self._metadata_thread.start()
            cmd.extend(["-vf", f"drawtext=textfile={self.metafile}:reload=1:fontcolor=white:fontsize={self.fontsize}:x=(w-text_w)/2:y=(h-text_h)/2"])

        cmd.extend([
            "-c:v", self.vcodec,
            "-c:a", self.acodec,
            "-af", "loudnorm=I=-18:LRA=11:TP=-2:linear=true",
            "-f", "mpegts",
            "pipe:1",
        ])

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            stdin=subprocess.DEVNULL,
        )

        log.debug(f"Running ffmpeg cmd: {cmd}")
        return self

    def read(self, n=-1):
        if self.process is None or self.process.stdout is None:
            raise ValueError("FFmpeg process not started. Call .open() first.")
        return self.process.stdout.read(n)

    def close(self):
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.process = None

        if hasattr(self, "_metadata_thread") and self._metadata_thread is not None:
            self._stop_metadata_thread.set()
            self._metadata_thread.join()
            self._metadata_thread = None

        # Clean up the orphaned temp file
        if hasattr(self, "metafile") and os.path.exists(self.metafile):
            try:
                os.remove(self.metafile)
                log.debug(f"Cleaned up temporary metafile: {self.metafile}")
            except OSError as e:
                log.debug(f"Failed to remove temp metafile {self.metafile}: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def generate_temp_metafile(self):
        """
        Creates a temp filename for writing out stream metadata
        """
        # create an md5 hash of the url
        md5 = hashlib.md5(self.url.encode("utf-8")).hexdigest()
        # use the system temp directory
        temp_dir = tempfile.gettempdir()
        # create full path
        path = os.path.join(temp_dir, f"dw_playradio_{md5}.tmp")
        # create/blank out temp file
        open(path, "w").close()
        # return the file for use
        return path

    def get_metadata(self):
        """
        Get metadata function for getting song information from HLS or ICY radio streams
        """
        # init result as none
        result = None

        if self.stream_type == "icy":
            """
            Get stream metadata for icy type streams
            """
            # result is blank by default
            # update session headers
            icy_headers = {
                "User-Agent": "Lavf/61.7.100",
                "Icy-MetaData": "1"
            }
            self.session.headers.update(icy_headers)
            # get stream url
            with self.session.get(self.url, stream=True) as resp:
                # metadata interval which specifies when the metadata is inserted into the audio
                resp.raise_for_status()
                meta_int = int(resp.headers.get("icy-metaint", 0))
                raw = resp.raw
                # read data at meta_int
                raw.read(meta_int)
                # Read metadata length byte
                length_byte = raw.read(1)
                if length_byte:
                    # check bytes
                    meta_length = length_byte[0] * 16
                    if meta_length > 0:
                        # read metadata
                        meta_data = raw.read(meta_length).strip(b'\0')
                        if b"StreamTitle='" in meta_data:
                           # split off the StreamTitle info
                            result = meta_data.split(b"StreamTitle='")[1].split(b"';")[0].decode("utf-8", errors="ignore")

        if self.stream_type == "hls":
            """
            Extract metadata from HLS streams (EXTINF) for the first segment.
            """
            result = None
            with self.session.get(self.url, stream=True) as resp:
                master = m3u8.loads(resp.text)
                if master.is_variant:
                    # choose highest bandwidth variant
                    variant = max(master.playlists, key=lambda p: p.stream_info.bandwidth)
                    # use existing requests session to pull variant
                    with self.session.get(variant.absolute_uri, stream=True) as resp:
                        # set playlist data
                        playlist = m3u8.loads(resp.text)
                else:
                    playlist = master

            if not playlist.segments:
                return result

            # grab last segment
            extinf = playlist.segments[-1].title
            if not extinf:
                return result

            # Extract key="value" pairs
            matches = re.findall(r'(\w+)="([^"]+)"', extinf)

            if matches:
                # Convert to lowercase dictionary for easy, case-insensitive lookup
                tags = {k.lower(): v.strip() for k, v in matches}
                artist = tags.get("artist", "")
                title = tags.get("title", "")

                if artist and title:
                    # 1. Prevent duplication if artist and title are exactly the same
                    if artist.lower() == title.lower():
                        result = title
                    # 2. Prevent duplication if the artist is already baked into the title string
                    elif artist.lower() in title.lower():
                        result = title
                    elif title.lower() in artist.lower():
                        result = artist
                    # 3. Format cleanly on a single line for FFmpeg
                    else:
                        result = f"{artist} - {title}"
                elif title:
                    result = title
                elif artist:
                    result = artist
                else:
                    # Fallback: if there are other random tags, join them on a single line
                    seen = set()
                    unique_values = [v for _, v in matches if not (v in seen or seen.add(v))]
                    result = " - ".join(unique_values)
            else:
                # Fallback: if there are no key="value" pairs, just use the raw string
                result = extinf.strip()

        # return any valid result
        return result

    def update_metadata(self):
        while not self._stop_metadata_thread.is_set():
            song = self.get_metadata()
            if song:
                with open(self.metafile, "w") as f:
                    f.write(song)
            time.sleep(self.update_interval)

def load_cookies(cookiejar_path: str):
    """
    Load all cookies from a Netscape/Mozilla cookies.txt file
    and return dict suitable for Streamlink or manual headers
    """

    def resolve_path(path: str) -> str:
        if os.path.isabs(path):
            return path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, path)

    resolved_file = resolve_path(cookiejar_path)

    # Load cookie jar
    jar = http.cookiejar.MozillaCookieJar(resolved_file)
    try:
        jar.load(ignore_discard=True, ignore_expires=True)
    except FileNotFoundError:
        raise FileNotFoundError(f"Cookie file not found: {cookiejar_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load cookies from {cookiejar_path}: {e}")

    # Build cookies dict
    cookies_dict = {}
    for c in jar:
        cookies_dict[c.name] = c.value

    return cookies_dict

def get_ffmpeg_loglevel(loglevel: str):
    """
    Simple function to convert a python loglevel to an
    equivalent ffmpeg loglevel
    """

    # dict for python/ffmpeg loglevel equivalencies
    convert_loglevel = {
        "CRITICAL": "panic",
        "ERROR":    "error",
        "WARNING":  "warning",
        "INFO":     "info",
        "DEBUG":    "debug",
        "NOTSET":   "trace"
    }

    return convert_loglevel.get(loglevel.upper())

def find_clearkeys_by_url(stream_url: str, clearkeys_source: str = None) -> str | None:
    """
    Return the ClearKey string from JSON mapping for the given stream URL.
    Supports wildcard pattern matching. Defaults to ./clearkeys.json.

    Args:
        stream_url (str): The stream URL to look up.
        clearkeys_source (str, optional): Local file path or URL. Defaults to 'clearkeys.json' in same directory as dispatchwrapparr.py.

    Returns:
        str or None: ClearKey string, or None if not found.
    """

    def is_url(path_or_url):
        parsed = urlparse(path_or_url)
        return parsed.scheme in ('http', 'https')

    def resolve_path(path: str) -> str:
        """
        Resolve a path to an absolute path.
        If the path is already absolute, return as-is.
        If it's relative, treat it as relative to the script's directory.
        """
        if os.path.isabs(path):
            return path
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(script_dir, path)

    log.info(f"Clearkeys Source: '{clearkeys_source}'")

    try:
        log.info(f"Attempting to load json data from '{clearkeys_source}'")
        if is_url(clearkeys_source):
            response = requests.get(clearkeys_source, timeout=10)
            response.raise_for_status()
            keymap = response.json()
        else:
            resolved_file = resolve_path(clearkeys_source)
            with open(resolved_file, "r") as f:
                keymap = json.load(f)
    except Exception as e:
        log.error(f"Failed to load ClearKey JSON from '{clearkeys_source}': {e}")
        return None

    # Wildcard pattern matching (case-insensitive)
    for pattern, clearkey in keymap.items():
        if fnmatch.fnmatchcase(stream_url.lower(), pattern.lower()):
            log.info(f"Clearkey(s) match for '{stream_url}': '{clearkey}'")
            return clearkey

    log.info(f"No matching clearkey(s) found for '{stream_url}'. Moving on.")
    return None

def split_fragments(raw_url: str):
    """
    Parses the input URL and extracts fragment parameters into a dictionary.

    Args:
        raw_url (str): The full URL, possibly with fragments.

    Returns:
        tuple: (base_url, fragment_dict) where fragment_dict is a dictionary of fragment key-value pairs,
               or None if no fragment is present.
    """
    parsed = urlparse(raw_url)

    base_url = parsed._replace(fragment="").geturl()
    fragment = parsed.fragment

    if fragment:
        # parse_qs returns a dict with values as lists
        parsed_fragments = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(fragment).items()}
        return base_url, parsed_fragments
    else:
        return base_url, None

def parse_fragment_headers(raw_header_values: str | list[str] | None) -> dict[str, str]:
    """
    Parse one or more `header=<name>:<value>` URL fragment entries into a header dict.

    Args:
        raw_header_values: A single header string or list of header strings.

    Returns:
        dict[str, str]: Parsed headers in insertion order.
    """
    if not raw_header_values:
        return {}

    values = raw_header_values if isinstance(raw_header_values, list) else [raw_header_values]
    parsed_headers = {}

    for value in values:
        if not isinstance(value, str):
            log.warning(f"Skipping malformed header fragment value: {value!r}")
            continue

        if ":" not in value:
            log.warning(f"Skipping malformed header fragment '{value}': expected format '<Header-Name>:<Header-Value>'")
            continue

        name, header_value = value.split(":", 1)
        name = name.strip()
        header_value = header_value.strip()

        if not name:
            log.warning(f"Skipping malformed header fragment '{value}': header name cannot be empty")
            continue

        parsed_headers[name] = header_value

    return parsed_headers

def detect_streams(session, url, clearkey):
    """
    Performs extended plugin matching for Streamlink
    Returns a dict of possible streams
    """

    def invoke_drm_plugin(session, url, type, clearkey):
        # Get the dir that dispatchwrapparr.py was loaded from
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Append drmplugins/ to the above
        plugins_dir = os.path.join(script_dir, "drmplugins/")
        # Load the plugins from the directory
        session.plugins.load_path(plugins_dir)
        log.debug(f"Loaded DRM Streamlink plugins from: {plugins_dir}")
        # Begin plugin options
        plugin_options = Options()
        if clearkey:
            # Set decryption keys for HLS/DASH DRM plugins
            plugin_options.set("decryption-key", [clearkey])
        # Set plugin matcher URL's for matching
        if type == "dash":
            # By default, we'll ignore minimumUpdatePeriod
            plugin_options.set("ignore-mup", True)
            url = f"dashdrm://{url}"
        elif type == "hls":
            url = f"hlsdrm://{url}"
        # Match plugin through new URL
        plugin_name, plugin_cls, url = session.resolve_url(url)
        plugin = plugin_cls(session, url, options=plugin_options)
        log.debug(f"Plugin '{plugin_name}' matched via resolver")
        # Return list of streams
        return plugin.streams()

    def find_by_mime_type(session, url):
        try:
            # Use streamlink's existing requests session. I used a GET here because some servers don't allow HEAD.
            session_headers = session.get_option("http-headers") or {}
            probe_headers = {**session_headers, "Range": "bytes=0-1023"}
            with session.http.get(
                url,
                timeout=5,
                stream=True,
                headers=probe_headers
            ) as response:
                content_type = response.headers.get("Content-Type", "").lower()
                log.debug(f"Detected Content-Type: {content_type}")
        except Exception as e:
            log.error(f"Could not detect stream type: {e}")
            raise
        # HLS stream detected by content-type
        if "vnd.apple.mpegurl" in content_type or "x-mpegurl" in content_type:
            stream_type = "hls"
        # MPEG-DASH stream detected by content-type
        # Added text/xml for lazy CDN's that don't set mime type to dash+xml
        elif "dash+xml" in content_type or "text/xml" in content_type:
            stream_type = "dash"
        # Standard HTTP Stream detected by content-type. Return with "live" as only one variant will exist.
        elif "application/octet-stream" in content_type or content_type.startswith("audio/") or content_type.startswith("video/") or content_type.endswith("/ogg"):
            stream_type = "http"
        else:
            stream_type = None

        return stream_type

    try:
        log.debug("First pass plugin matching with Streamlink Plugin Resolver...")
        plugin_name, plugin_cls, url = session.resolve_url(url)
        plugin = plugin_cls(session, url)
        if plugin_name == "dash" and clearkey:
            streams = invoke_drm_plugin(session, url, plugin_name, clearkey)
            return streams
        elif plugin_name == "hls" and clearkey:
            streams = invoke_drm_plugin(session, url, plugin_name, clearkey)
            return streams
        elif plugin_name == "dash":
            # Use our own DASH handler for dash period change and pacing support
            streams = invoke_drm_plugin(session, url, plugin_name, None)
            return streams
        else:
            log.debug(f"Plugin '{plugin_name}' matched via resolver")
            return plugin.streams()
        
    except NoPluginError:
        log.debug("Second pass plugin matching via MIME Type Resolver...")
        stream_type = find_by_mime_type(session, url)
        
        if stream_type == "dash" and clearkey:
            log.debug("DASH DRM matched via MIME Type Resolver")
            streams = invoke_drm_plugin(session, url, stream_type, clearkey)
            return streams
        
        elif stream_type == "hls" and clearkey:
            log.debug("HLS DRM matched via MIME Type Resolver")
            streams = invoke_drm_plugin(session, url, stream_type, clearkey)
            return streams
        
        elif stream_type == "dash":
            log.debug("DASH Stream Detected via MIME Type Resolver")
            # Use our own DASH handler for dash period change and pacing support
            streams = invoke_drm_plugin(session, url, stream_type, None)
            return streams
            
        elif stream_type == "hls":
            log.debug("HLS Stream Detected via MIME Type Resolver")
            streams = HLSStream.parse_variant_playlist(session, url)
            return streams or {"live": HLSStream(session, url)}
            
        elif stream_type == "http":
            log.debug("HTTP Stream Detected via MIME Type Resolver")
            return {"live": HTTPStream(session, url)}
            
        else:
            raise PluginError("Could not detect stream type or no suitable plugin found.")

def check_stream_variant(stream, session=None):
    """ Checks for different stream variants:
    Eg. Audio Only streams or Video streams with no audio

    Can be disabled by using the -nocheckvariant argument

    Returns integer:
    0 = Normal Audio/Video
    1 = Audio Only Stream (Radio streams)
    2 = Video Only Stream (Cameras or other livestreams with no audio)
    """

    log.debug("Starting Stream Variant Checks...")
    # HLSStream case
    if isinstance(stream, HLSStream) and getattr(stream, "multivariant", None):
        log.debug("Variant Check: HLSStream Selected")
        # Find the playlist attributes by "best" selected url
        selected_playlist = None
        for playlist in stream.multivariant.playlists:
            if playlist.uri == stream.url:
                selected_playlist = playlist
                break

        if selected_playlist:
            codecs = selected_playlist.stream_info.codecs or []
            log.debug(f"Stream Codecs: {codecs}")
            # Check for audio/video presence
            has_video = any(c.startswith(("avc", "hev", "vp")) for c in codecs)
            has_audio = any(c.startswith(("mp4a", "aac")) for c in codecs)

            if has_audio and not has_video:
                log.debug("Detected Audio Only Stream")
                return 1
            elif has_video and not has_audio:
                log.debug("Detected Video Only Stream")
                return 2
            else:
                log.debug("Detected Audio+Video Stream")
                return 0

    # HTTPStream case
    if isinstance(stream, HTTPStream):
        log.debug("Variant Check: HTTPStream Selected")
        if session:
            try:
                with session.http.get(stream.url, stream=True, timeout=5) as r:
                    ctype = r.headers.get("Content-Type", "").lower()
                    if ctype.startswith("audio/") or ctype.endswith("/ogg"):
                        log.debug(f"Detected Audio Only Stream by Content-Type: {ctype}")
                        return 1
                    if ctype.startswith("video/"):
                        log.debug(f"Detected Video+Audio Stream by Content-Type: {ctype}")
                        return 0
            except Exception:
                # Ignore errors (405, timeout, etc.)
                return 0
    # Default/fallback
    return 0

def create_silent_audio(session, ffmpeg, ffmpeg_loglevel) -> Stream:
    """
    Return a Streamlink-compatible Stream that produces continuous silent AAC audio.
    Uses ffmpeg with anullsrc.
    """

    cmd = [
        ffmpeg,
        "-loglevel", ffmpeg_loglevel,
        "-f", "lavfi",
        "-i", "anullsrc=channel_layout=stereo:sample_rate=48000",
        "-c:a", "aac",
        "-f", "adts",
        "pipe:1"
    ]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=sys.stderr)

    class SilentAudioStream(Stream):
        def open(self, *args, **kwargs):
            return process.stdout

        def close(self):
            if process.poll() is None:
                process.kill()

    return SilentAudioStream(session)

def main():
    # Set log as global var
    global log
    
    # Collect cli args from argparse and pass initialise dw_opts
    dw_opts = parse_args()
    # Initialise dw_opts attributes that don't have a cli argument
    for attr in ("clearkey", "referer", "origin", "fragment_headers"):
        setattr(dw_opts, attr, None)
    # Configure log level
    log = configure_logging(dw_opts.loglevel)
    log.info(f"Dispatchwrapparr Version: {__version__}")
    log.info(f"Log Level: '{dw_opts.loglevel}'")
    # Process the input url and split off any fragments. Returns nonetype if no fragments
    url, fragments = split_fragments(dw_opts.i)
    log.info(f"Stream URL: '{url}'")
    # Begin processing URL fragments into dw_opts
    if fragments:
        dw_opts.clearkey = fragments.get("clearkey") if fragments.get("clearkey") else None
        dw_opts.stream = fragments.get("stream").lower() if fragments.get("stream") else None
        dw_opts.referer = fragments.get("referer") if fragments.get("referer") else None
        dw_opts.origin = fragments.get("origin") if fragments.get("origin") else None
        dw_opts.fragment_headers = parse_fragment_headers(fragments.get("header"))
        dw_opts.novariantcheck = (fragments["novariantcheck"].lower() == "true") if "novariantcheck" in fragments else False
        dw_opts.ffmpeg_nocopyts = (fragments["ffmpeg_nocopyts"].lower() == "true") if "ffmpeg_nocopyts" in fragments else False
        dw_opts.noaudio = (fragments["noaudio"].lower() == "true") if "noaudio" in fragments else False
        dw_opts.novideo = (fragments["novideo"].lower() == "true") if "novideo" in fragments else False

    # If -clearkeys argument is supplied and clearkey is None, search for a URL match in supplied file/url
    if dw_opts.clearkeys and not dw_opts.clearkey:
        dw_opts.clearkey = find_clearkeys_by_url(url,dw_opts.clearkeys)

    """
    Begin setting up the Streamlink Session
    """
    session = Streamlink()

    # Begin header construction with mandatory user agent string
    headers = {
        "User-Agent": dw_opts.ua
    }
    log.info(f"User Agent: '{dw_opts.ua}'")

    # Load streamlink plugins if -streamlink_plugins argument is supplied
    if dw_opts.streamlink_plugins:
        session.plugins.load_path(os.path.dirname(os.path.abspath(__file__)))

    # If -customheaders argument is supplied, parse and add to headers
    if dw_opts.customheaders:
        try:
            custom_headers = json.loads(dw_opts.customheaders)
            if isinstance(custom_headers, dict):
                headers.update(custom_headers)
                log.info(f"Custom Headers: {custom_headers}")
            else:
                log.error("Custom headers should be a JSON object/dictionary.")
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse custom headers JSON: {e}")

    # Append custom headers from repeated URL fragment entries:
    # #header=Authorization:Bearer%20XYZ&header=Origin:https://example.com
    if dw_opts.fragment_headers:
        headers.update(dw_opts.fragment_headers)
        log.info(f"Header Fragments: {dw_opts.fragment_headers}")

    # Append additional headers if set
    if dw_opts.referer:
        headers["Referer"] = dw_opts.referer
        log.info(f"Referer: '{dw_opts.referer}'")

    if dw_opts.origin:
        headers["Origin"] = dw_opts.origin
        log.info(f"Origin: '{dw_opts.origin}'")

    if dw_opts.cookies:
        # load cookies and create cookies_dict for streamlink
        cookies = load_cookies(dw_opts.cookies)
        session.set_option("http-cookies", cookies)
        log.info(f"Cookies: Loading cookies from file '{dw_opts.cookies}'")

    # Set http-headers for streamlink
    session.set_option("http-headers", headers)
    log.debug(f"Headers: {headers}")

    # Set generic session options for Streamlink
    session.set_option("stream-segment-threads", 2)
    # Start HLS stream further in from the live edge
    session.set_option("hls-live-edge", 6)
    # Increase the size of the Streamlink Ringbuffer to 64MiB
    session.set_option("ringbuffer-size", 67108864)
    # If cli -proxy argument supplied
    if dw_opts.proxy:
        # Set proxies as env vars for streamlink/requests/ffmpeg et al
        session.set_option("http-trust-env", True)
        os.environ["HTTP_PROXY"] = dw_opts.proxy
        os.environ["HTTPS_PROXY"] = dw_opts.proxy
        log.info(f"HTTP Proxy: '{dw_opts.proxy}'")
        # Set ipv4 only mode when using proxy (fixes reliability issues with dual stack streams)
        session.set_option("ipv4", True)
        # If -proxybypass is also supplied
        if dw_opts.proxybypass:
            proxybypass = dw_opts.proxybypass.strip("*") # strip any globs off as they're no longer supported
            os.environ["NO_PROXY"] = proxybypass
            log.info(f"Proxy Bypass: '{dw_opts.proxybypass}'")

    """
    FFmpeg Options that apply to all streams should they require muxing
    """

    # Check for -ffmpeg cli option
    if dw_opts.ffmpeg:
        session.set_option("ffmpeg-ffmpeg", dw_opts.ffmpeg)
        log.info(f"FFmpeg: Location '{dw_opts.ffmpeg}'")
    else:
        # Check if an ffmpeg binary exists in the script path and use that if it's there
        ffmpeg_check = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffmpeg")
        if os.path.isfile(ffmpeg_check):
            dw_opts.ffmpeg = ffmpeg_check
            log.info(f"FFmpeg: Found at '{dw_opts.ffmpeg}'")
            session.set_option("ffmpeg-ffmpeg", dw_opts.ffmpeg)
        else:
            # set global ffmpeg if no other found
            session.set_option("ffmpeg-ffmpeg", "ffmpeg")
    if dw_opts.ffmpeg_transcode_audio:
        session.set_option("ffmpeg-audio-transcode", dw_opts.ffmpeg_transcode_audio)
        log.info(f"FFmpeg: Transcode audio to '{dw_opts.ffmpeg_transcode_audio}'")
    # Set copy timestamps for ffmpeg muxing based on user input
    session.set_option("ffmpeg-copyts", not dw_opts.ffmpeg_nocopyts) 
    log.info(f"FFmpeg: Set copy timestamps to '{not dw_opts.ffmpeg_nocopyts}'")
    # Convert current python loglevel in an equivalent ffmpeg loglevel
    dw_opts.ffmpeg_loglevel = get_ffmpeg_loglevel(dw_opts.loglevel)
    session.set_option("ffmpeg-loglevel", dw_opts.ffmpeg_loglevel) # Set ffmpeg loglevel
    session.set_option("ffmpeg-verbose", True) # Pass ffmpeg stderr through to streamlink
    session.set_option("ffmpeg-fout", "mpegts") # Encode as mpegts when ffmpeg muxing (not matroska like default)
    session.set_option("ffmpeg-no-validation", True) # Don't bother wasting time checking for FFmpeg. We know it's baked into Dispatcharr :)

    """
    Stream detection and plugin loading
    """

    try:
        # Pass stream detection off to the detect_streams function. Returns a dict of available streams in varying quality.
        streams = detect_streams(session, url, dw_opts.clearkey)
    except Exception as e:
        log.error(f"Stream setup failed: {e}")
        return

    # No streams found, log and error and exit
    if not streams:
        log.error("No playable streams found.")
        return

    # Send a list of available streams to log output
    log.info(f"Available streams: {', '.join(streams.keys())}")

    """
    Select the best stream(s) from the list of streams
    """

    # Logic for either manual or automatic stream selection
    if dw_opts.stream:
        # 'stream' fragment found. Select stream based on that selection.
        log.info(f"Stream Selection: Manually specifying {dw_opts.stream}")
        stream = streams.get(dw_opts.stream)
    else:
        log.info("Stream Selection: Automatic")
        stream = streams.get("best") or streams.get("live") or next(iter(streams.values()), None)

    # Stream not available, log error and exit
    if not stream:
        log.error("Stream selection not available.")
        return

    """
    Check the chosen stream for nuances such as video-only or audio-only feeds
    """

    # Do a variant check only if novideo, noaudio and novariantcheck are False and there dw_opts.clearkey is None
    if dw_opts.novideo is False and dw_opts.noaudio is False and dw_opts.novariantcheck is False and dw_opts.clearkey is None:
        # Attempt to detect stream variant automatically (Eg. Video Only or Audio Only)
        log.debug("Checking stream variation...")
        variant = check_stream_variant(stream,session)
        if variant == 1:
            log.info("Stream detected as audio only/no video")
            dw_opts.novideo = True
        if variant == 2:
            log.info("Stream detected as video only/no audio")
            dw_opts.noaudio = True
    else:
        log.info("Skipping stream variant check")

    if dw_opts.noaudio and not dw_opts.novideo and not dw_opts.clearkey:
        log.info("No Audio: Muxing silent audio into supplied video stream")
        audio_stream = create_silent_audio(session,dw_opts.ffmpeg,dw_opts.ffmpeg_loglevel)
        video_stream = stream
        stream = MuxedStream(session, video_stream, audio_stream)

    elif not dw_opts.noaudio and dw_opts.novideo and not dw_opts.clearkey:
        log.info("No Video: Muxing blank video into supplied audio stream")
        stream_type = None
        if dw_opts.nosonginfo is False:
            if isinstance(stream, HLSStream):
                stream_type = "hls"
            elif isinstance(stream, HTTPStream):
                stream_type = "icy"
        stream = PlayRadio(url, session.options.get("ffmpeg-ffmpeg"), dw_opts.ffmpeg_loglevel, headers=None, cookies=None, stream_type=stream_type)

    elif dw_opts.noaudio and dw_opts.novideo:
        log.warning("Both 'noaudio' and 'novideo' specified. Ignoring both.")

    try:
        log.info("Starting stream...")
        # MPEG-TS packet size
        PACKET_SIZE = 188
        # OS pipe alignment for reading chunks from streamlink/ffmpeg
        READ_CHUNK = PACKET_SIZE * 340
        # Write buffer size set to match dispatcharr's read buffer (1MB)
        WRITE_BUFFER_SIZE = PACKET_SIZE * 5644

        # Create a buffer
        buffer = bytearray()

        with stream.open() as fd:
            while True:
                data = fd.read(READ_CHUNK)
                if not data:
                    break
                buffer.extend(data)
                # Flush whenever buffer exceeds WRITE_BUFFER_SIZE
                if len(buffer) >= WRITE_BUFFER_SIZE:
                    try:
                        sys.stdout.buffer.write(buffer)
                        sys.stdout.buffer.flush()
                    except BrokenPipeError:
                        break
                    buffer.clear()

        # Flush any remaining data
        if buffer:
            try:
                sys.stdout.buffer.write(buffer)
                sys.stdout.buffer.flush()
            except BrokenPipeError:
                # when flushing buffer ignore broken pipe errors
                pass

    except KeyboardInterrupt:
        log.info("Stream interrupted, canceling.")

# Set default SIGPIPE behavior so dispatchwrapparr exits cleanly when the pipe is closed
signal.signal(signal.SIGPIPE, signal.SIG_DFL)
# Establish logging
log = logging.getLogger("dispatchwrapparr")

if __name__ == "__main__":
    main()
