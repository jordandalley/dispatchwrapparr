from __future__ import annotations

import re
import logging
import base64

from streamlink.exceptions import FatalPluginError
from streamlink.plugin import Plugin, pluginmatcher, pluginargument
from streamlink.plugin.plugin import LOW_PRIORITY, parse_params
from streamlink.stream.hls import HLSStream
from streamlink.stream.ffmpegmux import FFMPEGMuxer, MuxedStream
from streamlink.stream.stream import Stream
from streamlink.utils.url import update_scheme

log = logging.getLogger(__name__)

'''
HLSDRM plugin for Dispatchwrapparr & Streamlink
Requires: Streamlink >= 8.4.0

Trying to keep this implementation as lite-touch as possible and just let Streamlink do what it does best and handle
the playlist parsing and segment downloads.

All I'm doing here ensuring that the session option "stream-passthrough-encrypted" is set to "True" if a clearkey or clearkeys are passed
so that we can then get ffmpeg to do the decryption of the livestream.

In case of an HLS stream where normally muxing is not required, we force muxing using our own class so that we can again get ffmpeg to
decrypt the stream with supplied clearkey(s).

Thanks to Titus-AU, whose code is used as a reference and who laid a lot of a groundwork for DRM handling in Streamlink: https://github.com/titus-au
'''


HLSDRM_OPTIONS = [
    "decryption-key",
]

@pluginmatcher(
    re.compile(r"hlsdrm(?:variant)?://(?P<url>\S+)(?:\s(?P<params>.+))?$"),
)

@pluginmatcher(
    priority=LOW_PRIORITY,
    pattern=re.compile(
        # URL with explicit scheme, or URL with implicit HTTPS scheme and a path
        r"(?P<url>[^/]+/\S+\.m3u8(?:\?\S*)?)(?:\s(?P<params>.+))?$",
        re.IGNORECASE,
    ),
)

@pluginargument(
    "decryption-key",
    type="comma_list",
    help="Decryption key(s) to be passed to ffmpeg."
)

class HLSDRM(Plugin):
    def _get_streams(self):
        data = self.match.groupdict()
        url = update_scheme("https://", data.get("url"), force=False)
        params = parse_params(data.get("params"))
        log.debug(f"HLSDRM: URL={url}; params={params}")
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
        '''
        Function for processing clearkeys
        Based on work by Titus-AU: https://github.com/titus-au (Thank you!!)
        '''
        keys = self.get_option('decryption-key')
        # if a colon separated key is given, assume its kid:key and take the
        # last component after the colon
        return_keys = []
        for k in keys:
            key = k.split(':')
            key_len = len(key[-1])
            log.debug("HLSDRM: Decryption Key %s has %s digits", key[-1], key_len)
            if key_len in (21, 22, 23, 24):
                # key len of 21-24 may mean a base64 key was provided, so we 
                # try and decode it
                log.debug("HLSDRM: Decryption key length is too short to be hex and looks like it might be base64, so we'll try and decode it..")
                b64_string = key[-1]
                padding = 4 - (len(b64_string) % 4)
                b64_string = b64_string + ("=" * padding)
                b64_key = base64.urlsafe_b64decode(b64_string).hex()
                if b64_key:
                    key = [b64_key]
                    key_len = len(b64_key)
                    log.debug("HLSDRM: Decryption Key (post base64 decode) is %s and has %s digits", key[-1], key_len)
            if key_len == 32:
                # sanity check that it's a valid hex string
                try:
                    int(key[-1], 16)
                except ValueError as err:
                    raise FatalPluginError(f"HLSDRM: Expecting 128bit key in 32 hex digits, but the key contains invalid hex.")
            elif key_len != 32:
                raise FatalPluginError(f"HLSDRM: Expecting 128bit key in 32 hex digits.")
            return_keys.append(key[-1])
        return return_keys

class FFMPEGMuxerDRM(FFMPEGMuxer):
    '''
    Muxer class for injecting clearkeys for decryption
    Based on work by Titus-AU: https://github.com/titus-au (Thank you!!)
    '''

    @classmethod
    def _get_keys(cls, session):
        keys=[]
        if session.options.get("decryption-key"):
            keys = session.options.get("decryption-key")
            # If only 1 key is given, then we use that also for all remaining streams
            if len(keys) == 1:
                keys.extend(keys)
            log.debug("FFMPEGMuxerDRM: Decryption Keys %s", keys)
        return keys

    def __init__(self, session, *streams, **options):
        super().__init__(session, *streams, **options)
        # if a decryption key is set, we rebuild the ffmpeg command list
        # to include the key before specifying the input streams
        # after that we append our inputs
        keys = self._get_keys(session)
        key = 0
        # begin building a new ffmpeg command list
        old_cmd = self._cmd.copy()
        self._cmd = []
        while len(old_cmd) > 0:
            cmd = old_cmd.pop(0)
            if cmd == "-i":
                _ = old_cmd.pop(0)
                # increase thread queue
                self._cmd.extend(['-thread_queue_size', '5120'])
                # generate presentation timestamps from dts
                self._cmd.extend(['-fflags', '+genpts'])
                if keys:
                    self._cmd.extend(["-decryption_key", keys[key]])
                    key += 1
                    # If we had more streams than keys, start with the first audio key again
                    if key == len(keys):
                        key = 1
                self._cmd.extend([cmd, _])
            else:
                self._cmd.append(cmd)
        # pop the last argument (the output pipe, e.g., "pipe:1")
        output_pipe = self._cmd.pop()
        # put any output ffmpeg options here if ever needed
        # append the output pipe back to the very end
        self._cmd.append(output_pipe)
        log.debug("FFMPEGMuxerDRM: Updated ffmpeg command %s", self._cmd)

class SingleStreamDRM(Stream):
    """
    Wrapper for forcing the DRM FFmpeg muxer for single-track hls streams
    """
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
    """
    Wrapper for invoking the DRM FFmpeg muxer for multi-track hls streams
    """
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