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

__version__ = "1.7.8"

'''
HLSDRM plugin for Dispatchwrapparr & Streamlink
Requires: Streamlink >= 8.6.0

Trying to keep this implementation as lite-touch as possible and just let Streamlink do what it does best and handle
the playlist parsing and segment downloads.

All I'm doing here ensuring that the session option "stream-passthrough-encrypted" is set to "True" if a clearkey or clearkeys are passed
so that we can then get ffmpeg to do the decryption of the livestream.

In case of an HLS stream where normally muxing is not required, we force muxing using our own class so that we can again get ffmpeg to
decrypt the stream with supplied clearkey(s).

Thanks to Titus-AU, whose code is used as a reference and who laid a lot of a groundwork for DRM handling in Streamlink: https://github.com/titus-au
'''

HLSDRM_OPTIONS = [
    "decryption-key"
]

@pluginmatcher(re.compile(r"hlsdrm(?:variant)?://(?P<url>\S+)(?:\s(?P<params>.+))?$"))

@pluginmatcher(
    priority=LOW_PRIORITY,
    pattern=re.compile(r"(?P<url>[^/]+/\S+\.m3u8(?:\?\S*)?)(?:\s(?P<params>.+))?$", re.IGNORECASE)
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
        # Process and store plugin options      
        for option in HLSDRM_OPTIONS:
            if option == 'decryption-key' and self.get_option('decryption-key'):
                self.session.options[option] = self._process_keys()
                # If decryption key provided, set Streamlink session option 'stream-passthrough-encrypted'
                self.session.set_option("stream-passthrough-encrypted", True)
            elif self.get_option(option):
                self.session.options[option] = self.get_option(option)

        # Let Streamlink parse the HLS manifest natively
        streams = HLSStream.parse_variant_playlist(self.session, url, **params)
        if not streams:
            streams = {"live": HLSStream(self.session, url, **params)}

        wrapped_streams = {}
        for name, stream in streams.items():
            if isinstance(stream, MuxedStream):
                # muxed stream passed to MuxedStreamDRM class regardless of whether or not clearkey(s) provided
                wrapped_streams[name] = MuxedStreamDRM(self.session, stream)
            elif isinstance(stream, HLSStream) and self.session.options.get("decryption-key"):
                # if a single stream which normally isn't muxed, and decryption-key(s) provided, force muxing for decryption
                wrapped_streams[name] = SingleStreamDRM(self.session, stream)
            else:
                # single stream with no decryption-key(s) provided. Just a dumb stream - pass through without muxing
                wrapped_streams = streams

        return wrapped_streams

    def _process_keys(self):
        '''
        Function for processing clearkeys
        Based on work by Titus-AU: https://github.com/titus-au (Thank you!!)
        '''
        keys = self.get_option('decryption-key')
        return_keys = []
        
        for k in keys:
            if k.lower() == "none":
                return_keys.append(None)
                continue
                
            key = k.split(':')
            key_val = key[-1]
            key_len = len(key_val)
            log.debug("PROCESSKEYS: Decryption Key %s has %s digits", key_val, key_len)
            
            is_valid_hex = False
            if key_len == 32:
                try:
                    int(key_val, 16)
                    is_valid_hex = True
                except ValueError:
                    pass
            
            if not is_valid_hex:
                try:
                    padding = 4 - (key_len % 4)
                    b64_string = key_val + ("=" * padding) if padding != 4 else key_val
                    decoded_bytes = base64.urlsafe_b64decode(b64_string)
                    
                    if len(decoded_bytes) == 16:
                        # Handle base64 encoded raw bytes
                        key_val = decoded_bytes.hex()
                    elif len(decoded_bytes) == 32:
                        # Handle base64 encoded hex strings (e.g. YmQ3ZWVh...)
                        key_val = decoded_bytes.decode('utf-8')
                        int(key_val, 16)  # Validate it's hex
                    else:
                        raise ValueError
                except Exception:
                    raise FatalPluginError("PROCESSKEYS: Expecting 128bit key in 32 hex digits, or base64 equivalent.")
                    
            if len(key_val) != 32:
                raise FatalPluginError("PROCESSKEYS: Expecting 128bit key in 32 hex digits.")
                
            return_keys.append(key_val)
            
        # Duplicate if only a single key is provided
        if len(return_keys) == 1:
            return_keys.append(return_keys[0])
            
        return return_keys

class FFMPEGMuxerDRM(FFMPEGMuxer):
    '''
    Muxer class for injecting clearkeys for decryption
    Based on work by Titus-AU: https://github.com/titus-au (Thank you!!)
    '''

    @classmethod
    def _get_keys(cls, session):
        keys = session.options.get("decryption-key") or []
        if keys:
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
                    if keys[key] is not None:
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
        # ffmpeg output options here if needed
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
        if copyts is None: copyts = True
        log.debug("HLSDRM: Forcing Muxing for single")
            
        muxer = FFMPEGMuxerDRM(self.session, reader, format=fmt, copyts=copyts)
        return muxer.open()

class MuxedStreamDRM(Stream):
    """
    Wrapper for invoking the DRM FFmpeg muxer for multi-track hls streams
    Delegates to Streamlink's native _open_streams for handling packed audio PTS extraction.
    """

    def __init__(self, session, muxed_stream):
        super().__init__(session)
        self.muxed_stream = muxed_stream

    def open(self):
        # Open substreams using the native _open_streams method (if available)
        # This will extract the PTS and update self.muxed_stream.options with 'itsoffset'
        if hasattr(self.muxed_stream, "_open_streams"):
            fds = self.muxed_stream._open_streams()
        else:
            fds = [substream.open() for substream in self.muxed_stream.substreams]
            
        options = self.muxed_stream.options.copy()
            
        fmt = self.session.options.get("ffmpeg-fout") or "mpegts"
        copyts = self.session.options.get("ffmpeg-copyts")
        if copyts is None:
            copyts = True
            
        options["format"] = fmt
        options["copyts"] = copyts
            
        muxer = FFMPEGMuxerDRM(self.session, *fds, **options)
        return muxer.open()
    
__plugin__ = HLSDRM