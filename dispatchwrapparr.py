#!/usr/bin/env python3

"""
Dispatchwrapparr - A super wrapper for Dispatcharr

Usage: dispatchwrapper.py -i <URL> -ua <User Agent String>
Optional: -proxy <proxy server> -proxybypass <proxy bypass list> -clearkeys <json file/url> -cookies <txt file> -loglevel <level> -stream <selection> -subtitles -novariantcheck -novideo -noaudio -nosonginfo
"""

from __future__ import annotations
import os
import sys
import re
import signal
import itertools
import logging
import base64
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
from typing import Any, Mapping
from datetime import timedelta
from urllib.parse import urlparse, parse_qs
from collections import defaultdict
from contextlib import suppress
from streamlink import Streamlink
from streamlink.utils.parse import parse_xml
from streamlink.plugins.dash import MPEGDASH
from streamlink.plugins.hls import HLSPlugin
from streamlink.exceptions import PluginError, FatalPluginError, NoPluginError, StreamError
from streamlink.stream.dash import DASHStream, DASHStreamReader, DASHStreamWorker, DASHStreamWriter
from streamlink.stream.dash.manifest import Representation, MPD, freeze_timeline
from streamlink.stream.ffmpegmux import FFMPEGMuxer
from streamlink.stream import HTTPStream, HLSStream, DASHStream, MuxedStream, Stream
from streamlink.session import Streamlink
from streamlink.stream.hls.hls import HLSStreamWriter, HLSStreamReader, Response
from streamlink.stream.hls.segment import HLSSegment
from streamlink.utils.l10n import Language
from streamlink.utils.times import now
from streamlink.plugins.http import HTTPStreamPlugin

__version__ = "1.5.5"

def parse_args():
    # Initial wrapper arguments
    parser = argparse.ArgumentParser(description="Dispatchwrapparr: A super wrapper for Dispatcharr")
    parser.add_argument("-i", required=True, help="Required: Stream URL")
    parser.add_argument("-ua", required=True, help="Required: User-Agent string")
    parser.add_argument("-proxy", help="Optional: HTTP proxy server (e.g. http://127.0.0.1:8888)")
    parser.add_argument("-proxybypass", help="Optional: Comma-separated list of hostnames or IP patterns to bypass the proxy (e.g. '192.168.*.*,*.lan')")
    parser.add_argument("-clearkeys", help="Optional: Supply a json file or URL containing URL/Clearkey maps (e.g. 'clearkeys.json' or 'https://some.host/clearkeys.json')")
    parser.add_argument("-cookies", help="Optional: Supply a cookie jar txt file in Mozilla/Netscape format (e.g. 'cookies.txt')")
    parser.add_argument("-stream", help="Optional: Supply streamlink stream selection argument (eg. best, worst, 1080p, 1080p_alt, etc)")
    parser.add_argument("-ffmpeg", help="Optional: Specify a custom ffmpeg binary path")
    parser.add_argument("-ffmpeg_transcode_audio", help="Optional: When muxing with ffmpeg, specify an output audio format (eg. aac, eac3, ac3, copy)")
    parser.add_argument("-subtitles", action="store_true", help="Optional: Enable support for subtitles (if available)")
    parser.add_argument("-novariantcheck", action="store_true", help="Optional: Do not autodetect if stream is audio-only or video-only")
    parser.add_argument("-novideo", action="store_true", help="Optional: Forces muxing of a blank video track into a stream that contains no audio")
    parser.add_argument("-noaudio", action="store_true", help="Optional: Forces muxing of a silent audio track into a stream that contains no video")
    parser.add_argument("-nosonginfo", action="store_true", help="Optional: Disable song information during streaming radio plays")
    parser.add_argument("-loglevel", type=str, default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"], help="Enable logging and set log level. (default: INFO)")
    parser.add_argument("-v", "--version", action="version", version=f"Dispatchwrapparr {__version__}")
    args = parser.parse_args()

    # Enforce dependency for proxybypass, must be used with proxy
    if args.proxybypass and not args.proxy:
        parser.error("Argument -proxybypass: requires -proxy to be set")

    # Ensure that novariantcheck, novideo, noaudio, and clearkeys are not specified simultaneously
    flags = [args.novideo, args.noaudio, args.novariantcheck, args.clearkeys]
    if sum(bool(f) for f in flags) > 1:
        parser.error("Arguments -novariantcheck, -novideo, -noaudio and -clearkeys can only be used individually")

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

class FFMPEGMuxerDRM(FFMPEGMuxer):
    """
    Inherits and extends Streamlink's FFMPEGMuxer class to add
    the additional -decryption_key arguments for DRM decryption
    """

    @classmethod
    def _get_keys(cls, session):
        keys=[]
        if session.options.get("clearkeys"):
            keys = session.options.get("clearkeys")
            # If only 1 key, then we use that for all remaining streams
            if len(keys) == 1:
                keys.extend(keys)
        return keys

    def __init__(self, session, *streams, **kwargs):
        super().__init__(session, *streams, **kwargs)

        # initialise keys var by calling get_keys func with session
        keys = self._get_keys(session)
        key = 0
        subtitles = self.session.options.get("mux-subtitles")
        old_cmd = self._cmd.copy()
        self._cmd = []
        while len(old_cmd) > 0:
            cmd = old_cmd.pop(0)
            if keys and cmd == "-i":
                _ = old_cmd.pop(0)
                self._cmd.extend(["-decryption_key", keys[key]])
                self._cmd.extend(["-fflags", "+genpts"])
                self._cmd.extend(["-re"])
                self._cmd.extend(["-copyts"])
                self._cmd.extend(["-start_at_zero"])
                key += 1
                # If we had more streams than keys, start with the first
                # audio key again
                if key == len(keys):
                    key = 1
                self._cmd.extend([cmd, _])
            elif subtitles and cmd == "-c:a":
                _ = old_cmd.pop(0)
                self._cmd.extend([cmd, _])
                self._cmd.extend(["-c:s", "copy"])
            else:
                self._cmd.append(cmd)
        if self._cmd and (self._cmd[-1].startswith("pipe:") or not self._cmd[-1].startswith("-")):
            final_output = self._cmd.pop()
            self._cmd.extend(["-async", "1"])
            self._cmd.extend(["-fps_mode", "passthrough"])
            self._cmd.extend(["-mpegts_copyts", "1"])
            self._cmd.extend(["-mpegts_flags", "+pat_pmt_at_frames+resend_headers+initial_discontinuity"])
            self._cmd.append(final_output)
        log.debug("Updated ffmpeg command %s", self._cmd)

class DASHStreamWriterDRM(DASHStreamWriter):
    reader: DASHStreamReaderDRM
    stream: DASHStreamDRM

class DASHStreamWorkerDRM(DASHStreamWorker):
    reader: DASHStreamReaderDRM
    writer: DASHStreamWriterDRM
    stream: DASHStreamDRM

    def iter_segments(self):
        init = True
        back_off_factor = 1
        while not self.closed:
            # find the representation by ID
            representation = self.mpd.get_representation(self.reader.ident)

            if self.mpd.type == "static":
                refresh_wait = 5
            else:
                refresh_wait = (
                    max(
                        self.mpd.minimumUpdatePeriod.total_seconds(),
                        representation.period.duration.total_seconds() if representation else 0,
                    )
                    or 5
                )

            with self.sleeper(refresh_wait * back_off_factor):
                if not representation:
                    continue

                iter_segments = representation.segments(
                    sequence=self.sequence,
                    init=init,
                    # sync initial timeline generation between audio and video threads
                    timestamp=self.reader.timestamp if init else None,
                )
                for segment in iter_segments:
                    if init and not segment.init:
                        self.sequence = segment.num
                        init = False
                    yield segment

                # close worker if type is not dynamic (all segments were put into writer queue)
                if self.mpd.type != "dynamic":
                    self.close()
                    return

                if not self.reload():
                    back_off_factor = max(back_off_factor * 1.3, 10.0)
                else:
                    back_off_factor = 1

    def reload(self):
        """Dispatchwrapparr modified reload func with period change detection"""
        self.old_num_periods = len(self.mpd.periods)
        self.old_periods = [f"{idx}{f' (id={p.id!r})' if p.id is not None else ''}" for idx, p in enumerate(self.mpd.periods)]

        if self.closed:
            return

        self.reader.buffer.wait_free()
        log.debug(f"Reloading DASH DRM manifest {self.reader.ident!r}")
        res = self.session.http.get(
            self.mpd.url,
            exception=StreamError,
            retries=self.manifest_reload_retries,
            **self.stream.args,
        )

        new_mpd = MPD(
            self.session.http.xml(res, ignore_ns=True),
            base_url=self.mpd.base_url,
            url=self.mpd.url,
            timelines=self.mpd.timelines,
        )

        # get the current amount of periods before reload
        self.new_num_periods = len(new_mpd.periods)

        # check if period count has changed
        if self.old_num_periods != self.new_num_periods:
            log.debug(f"DASH DRM for REP {self.reader.ident[2]} and changed from {self.old_num_periods} to {self.new_num_periods} periods")
            self.new_periods = [f"{idx}{f' (id={p.id!r})' if p.id is not None else ''}" for idx, p in enumerate(new_mpd.periods)]
            log.debug(f"Old DASH DRM periods for REP {self.reader.ident[2]}: {', '.join(self.old_periods)}")
            log.debug(f"New DASH DRM periods for REP {self.reader.ident[2]}: {', '.join(self.new_periods)}")
            new_period_idx = next(
                (idx for idx, p in enumerate(new_mpd.periods) if getattr(p, 'duration', None) in (None, 0)),
                len(new_mpd.periods) - 1  # fallback: last period
            )
            log.debug(f"Auto-selected new DASH DRM period {new_period_idx} for REP {self.reader.ident[2]}")
            # get new period id by index
            new_period_id = new_mpd.periods[new_period_idx].id
            # reader.ident is an immutable tuple (period_id, timeline_id, rep_id).
            # Replace it by constructing a new tuple preserving the remaining parts.
            try:
                old_ident = getattr(self.reader, "ident", None)
                if isinstance(old_ident, tuple):
                    rest = old_ident[1:]
                    self.reader.ident = (new_period_id,) + rest
                else:
                     # fallback: set a simple tuple with the new period id
                    self.reader.ident = (new_period_id,)
                log.debug("DASH DRM: updated reader.ident -> %r", getattr(self.reader, "ident", None))
            except Exception:
                log.exception("DASH DRM: failed to update reader.ident after period change")

        """
        Probe the new MPD to see if that representation has available segments (without iterating the whole timeline);
        used to decide whether to adopt new_mpd (i.e. replace self.mpd) or keep the old one.
        """
        new_rep = new_mpd.get_representation(self.reader.ident)
        with freeze_timeline(new_mpd):
            changed = len(list(itertools.islice(new_rep.segments(), 1))) > 0

        if changed:
            self.mpd = new_mpd

        return changed


    def change_period(self):
        # get the current amount of periods before reload
        self.old_num_periods = len(self.mpd.periods)
        # get the current period id
        current_period = self.reader.ident[0]
        # find the period index by period id
        current_period_index = next(
            (idx for idx, p in enumerate(self.mpd.periods) if p.id == current_period),
            -1  # fallback: last period
        )
        log.debug(f"Current DASH DRM period index for REP {self.reader.ident[2]}: {current_period_index}")

class DASHStreamReaderDRM(DASHStreamReader):
    __worker__ = DASHStreamWorkerDRM
    __writer__ = DASHStreamWriterDRM

    worker: DASHStreamWorkerDRM
    writer: DASHStreamWriterDRM
    stream: DASHStreamDRM

class DASHStreamDRM(DASHStream):
    """
    This is effectively a hacked up version of the 'DASHStream' class from Streamlink's dash.py (14/11/2025)
    https://github.com/streamlink/streamlink/blob/94c964751be2b318cfcae6c4eb103aafaac6b75c/src/streamlink/stream/dash/dash.py
    Modifications to the original include bypassing DRM checking and additional live edge control
    """
    __shortname__ = "dashdrm"
    __dashdrm_live_edge__ = 10

    @staticmethod
    def parse_mpd(manifest: str, mpd_params: Mapping[str, Any]) -> MPD:
        node = parse_xml(manifest, ignore_ns=True)

        return MPD(node, **mpd_params)

    @classmethod
    def parse_manifest(
        cls,
        session: Streamlink,
        url_or_manifest: str,
        period: int | str = -1,
        with_video_only: bool = False,
        with_audio_only: bool = False,
        **kwargs,
    ) -> dict[str, DASHStreamDRM]:
        """
        Parse a DASH DRM manifest file and return its streams.

        :param session: Streamlink session instance
        :param url_or_manifest: URL of the manifest file or an XML manifest string
        :param period: Which MPD period to use (index number (int) or ``id`` attribute (str)) for finding representations
        :param with_video_only: Also return video-only streams, otherwise only return muxed streams
        :param with_audio_only: Also return audio-only streams, otherwise only return muxed streams
        :param kwargs: Additional keyword arguments passed to :meth:`requests.Session.request`
        """

        manifest, mpd_params = cls.fetch_manifest(session, url_or_manifest, **kwargs)

        try:
            mpd = cls.parse_mpd(manifest, mpd_params)
        except Exception as err:
            raise PluginError(f"Failed to parse MPD manifest: {err}") from err

        # Increase the suggestedPresentationDelay to avoid stuttering playback
        mpd.suggestedPresentationDelay += timedelta(seconds=cls.__dashdrm_live_edge__)
        log.debug(f"MPEG-DASH Adjusted Presentation Delay: {mpd.suggestedPresentationDelay}")

        source = mpd_params.get("url", "MPD manifest")
        video: list[Representation | None] = [None] if with_audio_only else []
        audio: list[Representation | None] = [None] if with_video_only else []

        available_periods = [f"{idx}{f' (id={p.id!r})' if p.id is not None else ''}" for idx, p in enumerate(mpd.periods)]
        log.debug(f"Available DASH periods: {', '.join(available_periods)}")

        """
        Select the period with duration=None or duration=0 if period==0 and multiple periods exist.
        Ensures that we always select the livestream period by default in multi-period DASH manifests.
        """

        if len(mpd.periods) > 1:
            period = next(
                (idx for idx, p in enumerate(mpd.periods) if getattr(p, 'duration', None) in (None, 0)),
                len(mpd.periods) - 1  # fallback: last period
            )
            log.debug(f"Auto-selected DASH period {period} for livestream with duration=None or duration=0")

        try:
            if isinstance(period, int):
                # selects period by index
                period_selection = mpd.periods[period]
            else:
                # selects period by ID
                period_selection = mpd.periods_map[period]

        except LookupError:
            raise PluginError(
                f"DASH period {period!r} not found. Select a valid period by index or by id attribute value.",
            ) from None

        """
        Search for suitable video and audio representations. Modified to continue without DRM checks
        """
        for aset in period_selection.adaptationSets:
            for rep in aset.representations:
                if rep.mimeType.startswith("video"):
                    video.append(rep)
                elif rep.mimeType.startswith("audio"):  # pragma: no branch
                    audio.append(rep)

        if not video:
            video.append(None)
        if not audio:
            audio.append(None)

        locale = session.localization
        locale_lang = locale.language
        lang = None
        available_languages = set()

        # if the locale is explicitly set, prefer that language over others
        for aud in audio:
            if aud and aud.lang:
                available_languages.add(aud.lang)
                with suppress(LookupError):
                    if locale.explicit and aud.lang and Language.get(aud.lang) == locale_lang:
                        lang = aud.lang

        if not lang:
            # filter by the first language that appears
            lang = audio[0].lang if audio[0] else None

        log.debug(
            f"Available languages for DASH audio streams: {', '.join(available_languages) or 'NONE'} (using: {lang or 'n/a'})",
        )

        # if the language is given by the stream, filter out other languages that do not match
        if len(available_languages) > 1:
            audio = [a for a in audio if a and (a.lang is None or a.lang == lang)]

        ret = []
        for vid, aud in itertools.product(video, audio):
            if not vid and not aud:
                continue

            stream = DASHStreamDRM(session, mpd, vid, aud, **kwargs)
            stream_name = []

            if vid:
                stream_name.append(f"{vid.height or vid.bandwidth_rounded:0.0f}{'p' if vid.height else 'k'}")
            if aud and len(audio) > 1:
                stream_name.append(f"a{aud.bandwidth:0.0f}k")
            ret.append(("+".join(stream_name), stream))

        # rename duplicate streams
        dict_value_list = defaultdict(list)
        for k, v in ret:
            dict_value_list[k].append(v)

        def sortby_bandwidth(dash_stream: DASHStreamDRM) -> float:
            if dash_stream.video_representation:
                return dash_stream.video_representation.bandwidth
            if dash_stream.audio_representation:
                return dash_stream.audio_representation.bandwidth
            return 0  # pragma: no cover

        ret_new = {}
        for q in dict_value_list:
            items = dict_value_list[q]

            with suppress(AttributeError):
                items = sorted(items, key=sortby_bandwidth, reverse=True)

            for n in range(len(items)):
                if n == 0:
                    ret_new[q] = items[n]
                elif n == 1:
                    ret_new[f"{q}_alt"] = items[n]
                else:
                    ret_new[f"{q}_alt{n}"] = items[n]

        # Return a list of dashdrm streams
        return ret_new

    def open(self):
        video, audio = None, None
        rep_video, rep_audio = self.video_representation, self.audio_representation

        timestamp = now()

        if rep_video:
            video = DASHStreamReaderDRM(self, rep_video, timestamp)
            log.debug(f"Opening DASHDRM reader for: {rep_video.ident!r} - {rep_video.mimeType}")

        if rep_audio:
            audio = DASHStreamReaderDRM(self, rep_audio, timestamp)
            log.debug(f"Opening DASHDRM reader for: {rep_audio.ident!r} - {rep_audio.mimeType}")

        """
        Always pass to muxer (ffmpeg) for DRM streams as this handles the decryption
        """
        if video and audio and FFMPEGMuxerDRM.is_usable(self.session):
            video.open()
            audio.open()
            return FFMPEGMuxerDRM(self.session, video, audio).open()
        elif video:
            video.open()
            return FFMPEGMuxerDRM(self.session, video).open()
        elif audio:
            audio.open()
            return FFMPEGMuxerDRM(self.session, audio).open()

class DASHPluginDRM(MPEGDASH):
    # clear out matchers to prevent url checking
    matchers = []
    def _get_streams(self):
        return DASHStreamDRM.parse_manifest(self.session, self.url)

class HLSStreamDRMWriter(HLSStreamWriter):
    """
    Writer that bypasses Streamlink's internal AES-128/SAMPLE-AES handling.
    Raw encrypted segments are passed through unchanged.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ignore_names: re.Pattern | None = None
        ignore_names = set(self.session.options.get("hls-segment-ignore-names") or [])
        if ignore_names:
            segments = "|".join(map(re.escape, ignore_names))
            self.ignore_names = re.compile(rf"(?:{segments})", re.IGNORECASE)

    def create_decryptor(self, key, sequence):
        log.debug(
            "HLSStreamDRMWriter: skipping internal decryption for key method=%s uri=%s",
            getattr(key, "method", None),
            getattr(key, "uri", None),
        )
        return None  # sent to _write()

    def _write(self, segment: HLSSegment, result: Response, is_map: bool):
        """
        Writes raw segment bytes directly to buffer, skipping Streamlink decryptor logic.
        """
        try:
            for chunk in result.iter_content(self.WRITE_CHUNK_SIZE):
                if not chunk:
                    continue
                # IMPORTANT: write directly to buffer (same as normal writer's decrypted output)
                self.reader.buffer.write(chunk)
        except Exception as err:
            log.error("Segment %s download failed: %s", segment.num, err)
            return

        log.debug("HLS DRM segment %s complete (%s)", segment.num, "map" if is_map else "media")

class HLSStreamDRMReader(HLSStreamReader):
    __writer__ = HLSStreamDRMWriter

class HLSStreamDRM(HLSStream):
    """
    Inherits and extends Streamlink's HLSStream class and modifies functions
    for DRM handling.
    """
    __shortname__ = "hlsdrm"
    __reader__ = HLSStreamDRMReader

    def open(self):
        """
        Override Streamlink's open() so that ALL HLS DRM streams are handled
        by FFMPEGMuxerDRM
        """

        reader = self.__reader__(self)   # same as HLSStreamReader
        log.debug(f"HLSDRM: Opening HLS-DRM reader for {self.url}")

        # Always open the reader before muxing
        reader.open()

        # identical to your DASH logic
        if FFMPEGMuxerDRM.is_usable(self.session):
            log.debug("HLSDRM: Routing stream to FFMPEGMuxerDRM")
            return FFMPEGMuxerDRM(self.session, reader).open()

        # fallback (rare)
        return reader

class HLSPluginDRM(HLSPlugin):
    # clear out matchers to prevent url checking
    matchers = []
    def _get_streams(self):
        streams = HLSStreamDRM.parse_variant_playlist(self.session, self.url)
        return streams or {"live": HLSStreamDRM(self.session, self.url)}

class HLSPluginForced(HLSPlugin):
    # clear out matchers to prevent url checking
    matchers = []
    def _get_streams(self):
        streams = HLSStream.parse_variant_playlist(self.session, self.url)
        return streams or {"live": HLSStream(self.session, self.url)}

class DASHPluginForced(MPEGDASH):
    # clear out matchers to prevent url checking
    matchers = []
    def _get_streams(self):
        return DASHStream.parse_manifest(self.session, self.url)

class HTTPStreamPluginForced(HTTPStreamPlugin):
    # clear out matchers to prevent url checking
    matchers = []
    def _get_streams(self):
        return {"live": HTTPStream(self.session, self.url)}

class PlayRadio:
    """
    A class that mimicks Streamlink stream.open() by using a file-like
    object that wraps a radio stream through FFmpeg, muxing blank video in or in
    the case of available metadata, displays song information for use on TV's.
    """

    def __init__(self, url, ffmpeg_loglevel, headers, cookies, stream_type=None, resolution="854x480", fps=25, acodec="aac", vcodec="libx264", fontsize=22, update_interval=5):
        self.url = url
        self.stream_type = stream_type
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
            "ffmpeg",
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
            matches = re.findall(r'(\w+)="([^"]+)"', extinf)
            seen = set()
            unique_values = [v for _, v in matches if not (v in seen or seen.add(v))]
            result = "\n".join(unique_values)

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

def detect_streams(session, url, clearkey, subtitles):
    """
    Performs extended plugin matching for Streamlink
    Returns a dict of possible streams
    """

    def find_by_mime_type(session, url):
        try:
            # Use streamlink's existing requests session. I used a GET here because some servers don't allow HEAD.
            with session.http.get(
                url,
                timeout=5,
                stream=True,
                headers={"Range": "bytes=0-1023"}
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
        elif "dash+xml" in content_type:
            stream_type = "dash"
        # Standard HTTP Stream detected by content-type. Return with "live" as only one variant will exist.
        elif "application/octet-stream" in content_type or content_type.startswith("audio/") or content_type.startswith("video/") or content_type.endswith("/ogg"):
            stream_type = "http"
        else:
            stream_type = None

        return stream_type

    try:
        log.debug("First pass plugin matching with Streamlink Plugin Resolver...")
        plugin_name, plugin_cls, resolved_url = session.resolve_url(url)
        log.debug(f"Plugin '{plugin_name}' matched via resolver")
        plugin = plugin_cls(session, resolved_url)

        if clearkey:
            stream_type = None
            try:
                stream_type = find_by_mime_type(session, resolved_url)
            except Exception:
                log.debug("Unable to detect stream type for DRM handling via plugin resolver", exc_info=True)
            if stream_type == "dash":
                log.debug("DASH DRM detected via Plugin Resolver")
                plugin = DASHPluginDRM(session, resolved_url)
            elif stream_type == "hls":
                log.debug("HLS DRM detected via Plugin Resolver")
                plugin = HLSPluginDRM(session, resolved_url)

    except NoPluginError:
        log.debug("Second pass plugin matching via MIME Type Resolver...")
        stream_type = find_by_mime_type(session, url)
        if stream_type == "dash" and clearkey:
            log.debug("DASH DRM Detected via MIME Type Resolver")
            plugin = DASHPluginDRM(session, url)
        elif stream_type == "hls" and clearkey:
            log.debug("HLS DRM Detected via MIME Type Resolver")
            plugin = HLSPluginDRM(session, url)
        elif stream_type == "dash" and not clearkey:
            log.debug("DASH Stream Detected via MIME Type Resolver")
            plugin = DASHPluginForced(session, url)
        elif stream_type == "hls" and not clearkey:
            log.debug("HLS Stream Detected via MIME Type Resolver")
            plugin = HLSPluginForced(session, url)
        elif stream_type == "http" and not clearkey:
            log.debug("HTTP Stream Detected via MIME Type Resolver")
            plugin = HTTPStreamPluginForced(session, url)
        else:
            raise PluginError("Could not detect stream type or no suitable plugin found.")

    return plugin.streams()

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

def create_silent_audio(session, ffmpeg_loglevel) -> Stream:
    """
    Return a Streamlink-compatible Stream that produces continuous silent AAC audio.
    Uses ffmpeg with anullsrc.
    """
    ffmpeg_bin = session.get_option("ffmpeg-ffmpeg") or "ffmpeg"
    cmd = [
        ffmpeg_bin,
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

def process_keys(clearkeys):
    """
    Process provided clearkeys to ensure they are in the correct format for ffmpeg
    Adapted from code by Titus-AU: https://github.com/titus-au/
    """
    # Convert provided string into a tuple
    keys = [clearkeys]
    return_keys = []
    for k in keys:
        # if a colon separated key is given, assume its kid:key and take the
        # last component after the colon
        key = k.split(':')
        key_len = len(key[-1])
        log.debug('Decryption Key %s has %s digits', key[-1], key_len)
        if key_len in (21, 22, 23, 24):
            # key len of 21-24 may mean a base64 key was provided, so we
            # try and decode it
            log.debug("Decryption key length is too short to be hex and looks like it might be base64, so we'll try and decode it..")
            b64_string = key[-1]
            padding = (-len(b64_string)) % 4
            if padding:
                b64_string = b64_string + ("=" * padding)
            b64_key = base64.urlsafe_b64decode(b64_string).hex()
            if b64_key:
                key = [b64_key]
                key_len = len(b64_key)
                log.debug('Decryption Key (post base64 decode) is %s and has %s digits', key[-1], key_len)
        if key_len == 32:
            # sanity check that it's a valid hex string
            try:
                int(key[-1], 16)
            except ValueError as err:
                raise FatalPluginError(f"Expecting 128bit key in 32 hex digits, but the key contains invalid hex.")
        elif key_len != 32:
            raise FatalPluginError(f"Expecting 128bit key in 32 hex digits.")
        return_keys.append(key[-1])
    return return_keys

def main():
    # Set log as global var
    global log
    # Collect cli args from argparse and pass initialise dw_opts
    dw_opts = parse_args()
    # Initialise dw_opts attributes that don't have a cli argument
    for attr in ("clearkey", "referer", "origin"):
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
        dw_opts.novariantcheck = (fragments["novariantcheck"].lower() == "true") if "novariantcheck" in fragments else False
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
    session.set_option("hls-segment-ignore-names", [".cmfv", ".cmfa"])
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

    # If -subtitles arg supplied
    if dw_opts.subtitles:
        session.set_option("mux-subtitles", True)
        log.info(f"Mux Subtitles (Experimental): Enabled")

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
    if dw_opts.ffmpeg_transcode_audio:
        session.set_option("ffmpeg-audio-transcode", dw_opts.ffmpeg_transcode_audio)
        log.info(f"FFmpeg: Transcode audio to '{dw_opts.ffmpeg_transcode_audio}'")
    # Convert current python loglevel in an equivalent ffmpeg loglevel
    dw_opts.ffmpeg_loglevel = get_ffmpeg_loglevel(dw_opts.loglevel)
    session.set_option("ffmpeg-loglevel", dw_opts.ffmpeg_loglevel) # Set ffmpeg loglevel
    session.set_option("ffmpeg-verbose", True) # Pass ffmpeg stderr through to streamlink
    session.set_option("ffmpeg-fout", "mpegts") # Encode as mpegts when ffmpeg muxing (not matroska like default)

    """
    Stream detection and plugin loading
    """

    try:
        # Pass stream detection off to the detect_streams function. Returns a dict of available streams in varying quality.
        streams = detect_streams(session, url, dw_opts.clearkey, dw_opts.subtitles)
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
        audio_stream = create_silent_audio(session,dw_opts.ffmpeg_loglevel)
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
        stream = PlayRadio(url, dw_opts.ffmpeg_loglevel, headers=None, cookies=None, stream_type=stream_type)

    elif dw_opts.noaudio and dw_opts.novideo:
        log.warning("Both 'noaudio' and 'novideo' specified. Ignoring both.")

    if dw_opts.clearkey:
        # Process clearkeys into format that ffmpeg understands
        processed_keys = process_keys(dw_opts.clearkey)
        # Set processed keys as session option
        session.options.set("clearkeys", processed_keys)
        log.info(f"DRM Clearkey(s): '{dw_opts.clearkey}' -> {processed_keys}")

    try:
        log.info("Starting stream...")
        # MPEG-TS packet size
        PACKET_SIZE = 188
        # Match Dispatcharr's read buffer size (12 KB)
        READ_CHUNK = PACKET_SIZE * 64
        # Write buffer size set to 192 KB to prevent flushing on every 12KB chunk
        WRITE_BUFFER_SIZE = PACKET_SIZE * 1024

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
                pass

    except KeyboardInterrupt:
        log.info("Stream interrupted, canceling.")

# Set default SIGPIPE behavior so dispatchwrapparr exits cleanly when the pipe is closed
signal.signal(signal.SIGPIPE, signal.SIG_DFL)
# Establish logging
log = logging.getLogger("dispatchwrapparr")

if __name__ == "__main__":
    main()
