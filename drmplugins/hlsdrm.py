from __future__ import annotations

import re
import logging
import base64
import struct

from streamlink.exceptions import FatalPluginError
from streamlink.plugin import Plugin, pluginmatcher, pluginargument
from streamlink.plugin.plugin import LOW_PRIORITY, parse_params
from streamlink.stream.hls import HLSStream
from streamlink.stream.ffmpegmux import FFMPEGMuxer, MuxedStream
from streamlink.stream.stream import Stream
from streamlink.utils.url import update_scheme

log = logging.getLogger(__name__)

__version__ = "1.7.4"

'''
HLSDRM plugin for Dispatchwrapparr & Streamlink
Requires: Streamlink >= 8.4.0

Trying to keep this implementation as lite-touch as possible and just let Streamlink do what it does best and handle
the playlist parsing and segment downloads.

All I'm doing here ensuring that the session option "stream-passthrough-encrypted" is set to "True" if a clearkey or clearkeys are passed
so that we can then get ffmpeg to do the decryption of the livestream.

In case of an HLS stream where normally muxing is not required, we force muxing using our own class so that we can again get ffmpeg to
decrypt the stream with supplied clearkey(s).

This plugin also contains experimental support for HLS muxed streams where PTS timestamps are extracted during a preload for insertion into the FFmpeg muxer.
Seeks to address the following issue: https://github.com/streamlink/streamlink/issues/4721
To activate, pass the --hlsdrm-packed-audio argument. Off by default.

Thanks to Titus-AU, whose code is used as a reference and who laid a lot of a groundwork for DRM handling in Streamlink: https://github.com/titus-au
'''

HLSDRM_OPTIONS = [
    "decryption-key",
    "packed-audio"
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
@pluginargument(
    "packed-audio",
    action="store_true",
    help="Prereads muxed HLS audio streams to extract PTS values from Apple ID3 tags."
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
        return keys

    def __init__(self, session, *streams, **options):
        self.audio_pts = options.pop("audio_pts", None)
        super().__init__(session, *streams, **options)
        # if a decryption key is set, we rebuild the ffmpeg command list
        # to include the key before specifying the input streams
        # after that we append our inputs

        keys = self._get_keys(session)
        key = 0
        # input counter
        input = 0
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
                if input == 1:
                    # input is audio (always second)
                    if self.audio_pts is not None:
                        # set default 90KHz clock for Apple HLS streams
                        self.audio_clock = 90000
                        log.debug(f"FFMPEGMuxerDRM: Applying itsoffset of {self.audio_pts/self.audio_clock} to audio input stream")
                        # apply timestamp offset for packed audio input
                        self._cmd.extend(['-itsoffset', f'{self.audio_pts/self.audio_clock}'])
                if keys:
                    self._cmd.extend(["-decryption_key", keys[key]])
                    key += 1
                    if key == len(keys):
                        key = 1
                input += 1
                self._cmd.extend([cmd, _])
            else:
                self._cmd.append(cmd)
                
        output_pipe = self._cmd.pop()
        self._cmd.append(output_pipe)
        log.debug("FFMPEGMuxerDRM: Updated ffmpeg command %s", self._cmd)

class PreReadStream:
    """
    A wrapper class that returns the PTS for wrapped audio streams
    by reading the Apple HLS ID3 tag by pre-reading the stream bytes before
    falling back to the original file descriptor
    """

    def __init__(self, fd, pre_data):
        self.fd = fd
        self.pre_data = pre_data
        self.pts = self._extract_pts()

    def _extract_pts(self):
        # scans the byte buffer for the Apple HLS ID3 tag and extracts the PTS for use in the muxer later
        if not self.pre_data:
            return None
            
        marker = b"com.apple.streaming.transportStreamTimestamp\x00"
        idx = self.pre_data.find(marker)
        
        if idx == -1:
            return None
            
        start_idx = idx + len(marker)
        if start_idx + 8 > len(self.pre_data):
            return None
            
        pts_bytes = self.pre_data[start_idx : start_idx + 8]
        pts = struct.unpack(">Q", pts_bytes)[0]
        
        # mask the upper 31 bits per the RFC
        return pts & 0x1FFFFFFFF

    def read(self, size=-1):
        if self.pre_data:
            if size == -1 or size >= len(self.pre_data):
                data = self.pre_data
                self.pre_data = b""
                return data
            else:
                data = self.pre_data[:size]
                self.pre_data = self.pre_data[size:]
                return data
        return self.fd.read(size)
        
    def close(self):
        if hasattr(self.fd, 'close'):
            self.fd.close()

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
            
        muxer = FFMPEGMuxerDRM(self.session, reader, format=fmt, copyts=copyts)
        return muxer.open()

class MuxedStreamDRM(Stream):
    """
    Wrapper for invoking the DRM FFmpeg muxer for multi-track hls streams
    Includes support for extracting PTS value from Apple ID3 tags by prereading the stream
    before ffmpeg muxer invoked.
    """

    def __init__(self, session, muxed_stream):
        super().__init__(session)
        self.substreams = muxed_stream.substreams

    def open(self):
        # initialise audio_pts variable
        audio_pts = None

        if self.session.options.get("packed-audio"):
            # If packed-audio option specified, open the streams,
            # read the first 2KB, and let the wrapper extract the PTS
            # from the id3 tag
            fds = []
            for substream in self.substreams:
                fd = substream.open()
                try:
                    chunk = fd.read(2048)
                    pre_stream = PreReadStream(fd, chunk)
                    
                    if pre_stream.pts is not None:
                        log.debug(f"HLSDRM: Successfully intercepted Audio PTS from raw data: {pre_stream.pts}")
                        audio_pts = pre_stream.pts
                        
                    fds.append(pre_stream)
                except Exception as e:
                    log.debug(f"HLSDRM: Failed to pre-read stream data: {e}")
                    fds.append(fd)
        else:
            fds = [substream.open() for substream in self.substreams]
        fmt = self.session.options.get("ffmpeg-fout") or "mpegts"
        copyts = self.session.options.get("ffmpeg-copyts")
        if copyts is None:
            copyts = True
            
        muxer = FFMPEGMuxerDRM(self.session, *fds, format=fmt, copyts=copyts, audio_pts=audio_pts)
        return muxer.open()
    
__plugin__ = HLSDRM