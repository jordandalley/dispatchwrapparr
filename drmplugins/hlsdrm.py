from __future__ import annotations

import re
import logging
import base64

from streamlink.exceptions import FatalPluginError
from streamlink.plugin import Plugin, pluginmatcher, pluginargument
from streamlink.plugin.plugin import HIGH_PRIORITY, parse_params
from streamlink.stream.hls import HLSStream
from streamlink.stream.ffmpegmux import FFMPEGMuxer, MuxedStream
from streamlink.stream.stream import Stream
from streamlink.utils.url import update_scheme

log = logging.getLogger(__name__)

HLSDRM_OPTIONS = [
    "decryption-key",
]

@pluginmatcher(
    priority=HIGH_PRIORITY,
    pattern=re.compile(r"hlsdrm://(?P<url>\S+)(?:\s(?P<params>.+))?$"),
)
@pluginargument(
    "decryption-key",
    type="comma_list",
    help="Decryption key to be passed to ffmpeg."
)
class HLSDRM(Plugin):
    def _get_streams(self):
        data = self.match.groupdict()
        url = update_scheme("https://", data.get("url"), force=False)
        params = parse_params(data.get("params"))
        log.debug(f"URL={url}; params={params}")
        # Set streamlink to pass through encrypted
        self.session.set_option("stream-passthrough-encrypted", True)
        # Process and store plugin options
        for option in HLSDRM_OPTIONS:
            if option == 'decryption-key' and self.get_option('decryption-key'):
                self.session.options[option] = self._process_keys()
            elif self.get_option(option):
                self.session.options[option] = self.get_option(option)

        # Let Streamlink parse the HLS manifest natively
        streams = HLSStream.parse_variant_playlist(self.session, url, **params)
        if not streams:
            streams = {"live": HLSStream(self.session, url, **params)}

        # Wrap the returned streams to force them through our DRM Muxer
        wrapped_streams = {}
        for name, stream in streams.items():
            if isinstance(stream, MuxedStream):
                # If it's a multi-track HLS (separate audio/video), wrap the substreams
                wrapped_streams[name] = MuxedStreamDRM(self.session, stream)
            else:
                # If it's a single-track HLS, force it into FFmpeg so we can apply the key
                wrapped_streams[name] = SingleStreamDRM(self.session, stream)

        return wrapped_streams

    def _process_keys(self):
        keys = self.get_option('decryption-key')
        return_keys = []
        for k in keys:
            key = k.split(':')
            key_len = len(key[-1])
            log.debug('Decryption Key %s has %s digits', key[-1], key_len)
            
            if key_len in (21, 22, 23, 24):
                log.debug("Key looks like base64, attempting decode...")
                b64_string = key[-1]
                padding = 4 - (len(b64_string) % 4)
                b64_string = b64_string + ("=" * padding)
                b64_key = base64.urlsafe_b64decode(b64_string).hex()
                if b64_key:
                    key = [b64_key]
                    key_len = len(b64_key)
                    
            if key_len == 32:
                try:
                    int(key[-1], 16)
                except ValueError:
                    raise FatalPluginError("Expecting 128bit key in 32 hex digits, but invalid hex found.")
            elif key_len != 32:
                raise FatalPluginError("Expecting 128bit key in 32 hex digits.")
                
            return_keys.append(key[-1])
        return return_keys


class FFMPEGMuxerDRM(FFMPEGMuxer):
    """
    Custom FFmpeg Muxer that injects the decryption keys directly into the command arguments
    """
    @classmethod
    def _get_keys(cls, session):
        keys = []
        if session.options.get("decryption-key"):
            keys = session.options.get("decryption-key")
            if len(keys) == 1:
                keys.extend(keys)
        return keys

    def __init__(self, session, *streams, **options):
        super().__init__(session, *streams, **options)
        
        keys = self._get_keys(session)
        key_idx = 0

        old_cmd = self._cmd.copy()
        self._cmd = []
        
        while len(old_cmd) > 0:
            cmd = old_cmd.pop(0)
            if keys and cmd == "-i":
                _ = old_cmd.pop(0)
                self._cmd.extend(["-decryption_key", keys[key_idx]])
                key_idx += 1
                if key_idx == len(keys):
                    key_idx = 1
                self._cmd.extend([cmd, _])
                self._cmd.extend(['-thread_queue_size', '4096'])
            else:
                self._cmd.append(cmd)
                
        log.debug("Updated HLS FFmpeg command: %s", self._cmd)


class SingleStreamDRM(Stream):
    """Wrapper to force a single-track HLS stream through our DRM FFmpeg muxer"""
    def __init__(self, session, stream):
        super().__init__(session)
        self.stream = stream

    def open(self):
        reader = self.stream.open()
        fmt = self.session.options.get("ffmpeg-fout") or "mpegts"
        copyts = self.session.options.get("ffmpeg-copyts")
        if copyts is None:
            copyts = True
            
        muxer = FFMPEGMuxerDRM(self.session, reader, format=fmt, copyts=copyts)
        return muxer.open()


class MuxedStreamDRM(Stream):
    """Wrapper to safely unpack a multi-track HLS stream into our DRM FFmpeg muxer"""
    def __init__(self, session, muxed_stream):
        super().__init__(session)
        self.substreams = muxed_stream.substreams

    def open(self):
        fds = [substream.open() for substream in self.substreams]
        
        fmt = self.session.options.get("ffmpeg-fout") or "mpegts"
        copyts = self.session.options.get("ffmpeg-copyts")
        if copyts is None:
            copyts = True
            
        muxer = FFMPEGMuxerDRM(self.session, *fds, format=fmt, copyts=copyts)
        return muxer.open()
    
__plugin__ = HLSDRM