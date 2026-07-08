from __future__ import annotations

import re
import base64
import logging
import itertools
import datetime

from streamlink.exceptions import FatalPluginError, StreamError
from streamlink.plugin import Plugin, pluginmatcher, pluginargument
from streamlink.plugin.plugin import HIGH_PRIORITY, parse_params, stream_weight
from streamlink.stream.dash import DASHStream, DASHStreamWorker, DASHStreamReader
from streamlink.stream.dash.manifest import MPD, freeze_timeline
from streamlink.stream.ffmpegmux import FFMPEGMuxer
from streamlink.stream.stream import Stream
from streamlink.utils.url import update_scheme
from streamlink.utils.times import now
from requests.adapters import HTTPAdapter

log = logging.getLogger(__name__)

__version__ = "1.7.6"

'''
DASHDRM plugin for Dispatchwrapparr & Streamlink
Requires: Streamlink >= 8.4.0

This plugin mostly inherits and extends the existing Streamlink DASH classes for additional support of DRM decryption of streams using supplied clearkey(s).
It can also be used for normal DASH streams and supports shifting of periods due to various ad injection techniques.

- Sets the Streamlink session option "stream-passthrough-encrypted" to "True" if a clearkey(s) are passed using the decyption-key plugin option
- Custom FFMPEGMuxerDRM class which creates a custom FFmpeg muxer injects the clearkeys, and also adds muxing flags to generate presentation timestamps (-fflags +genpts)
- Support for playing streams with ad injection or SCTE-35 implementations where MPD periods move and shift. This plugin will following the live period by attempting
  to play the period where there is no duration, and if that changes, finding a new period with no duration. By default it will start with the last period unless a different period is specified.
- A similar feature to the 'hls-segment-stream-data' option in Streamlink has been implemented which allows segment data to be written as it is being downloaded and prevent
  requests from blocking (dash-segment-stream-data).
- The 'ignore-mup' option ignores the minimumUpdatePeriod in the manifest and clamps the refresh time to 5 seconds. This is useful where certain broadcasters add new segments at each update
  period that do not align with the minimumUpdatePeriod, causing gaps in playback.
- The 'autodetect-epoch' option enables automatic detection of streams that use epoch-aligned manifests, and normalises the presentationTimeOffsets to zero.

Thanks to Titus-AU, whose code is used as a reference and who laid a lot of a groundwork for DRM handling in Streamlink: https://github.com/titus-au
'''

DASHDRM_OPTIONS = [
    "decryption-key",
    "dash-segment-stream-data",
    "ignore-mup",
    "autodetect-epoch"
]
@pluginmatcher(
    priority=HIGH_PRIORITY,
    pattern=re.compile(r"dashdrm://(?P<url>\S+)(?:\s(?P<params>.+))?$"),
)
@pluginargument(
    "decryption-key",
    type="comma_list",
    help="Decryption key(s) to be passed to ffmpeg."
)
@pluginargument(
    "dash-segment-stream-data",
    action="store_true",
    help="Same as 'hls-segment-stream-data' option in Streamlink, but for DASH"
)
@pluginargument(
    "ignore-mup",
    action="store_true",
    help="Ignore the manifest minimumUpdatePeriod to check for new segments more frequently"
)
@pluginargument(
    "autodetect-epoch",
    action="store_true",
    help="Enable automatic detection of epoch-aligned manifests and normalise presentationTimeOffsets to zero"
)

class MPEGDASHDRM(Plugin):
    @classmethod
    def stream_weight(cls, stream):
        match = re.match(r"^(?:(.*)\+)?(?:a(\d+)k)$", stream)
        if match and match.group(1) and match.group(2):
            weight, group = stream_weight(match.group(1))
            weight += int(match.group(2))
            return weight, group
        elif match and match.group(2):
            return stream_weight(f"{match.group(2)}k")
        else:
            return stream_weight(stream)
        
    @staticmethod
    def _process_epoch_aligned_presentation_offsets(mpd):
        """
        Function for detecting if the manifest contains epoch-aligned presentationTimeOffsets
        and if so, zero's them out so Streamlink finds the live edge successfully
        """
        if mpd.type != "dynamic" or not getattr(mpd, "availabilityStartTime", None):
            return

        normalised = set()
        for period in mpd.periods:
            for adaptation_set in period.adaptationSets:
                for representation in adaptation_set.representations:
                    # find the template, whether it's on the representation or the adaptation set
                    template = getattr(representation, "segmentTemplate", None) or representation.walk_back_get_attr("segmentTemplate")
                    
                    if not template or not getattr(template, "presentationTimeOffset", None) or id(template) in normalised:
                        continue

                    # extract total_seconds() from the timedelta object for the comparison
                    if template.presentationTimeOffset.total_seconds() > 1000000000:
                        log.debug(f"MPEGDASHDRM: Normalising epoch-aligned presentationTimeOffset for representation {representation.id}.")
                        # zero out the offset using an empty timedelta object
                        template.presentationTimeOffset = datetime.timedelta()
                        normalised.add(id(template))

    def _get_streams(self):
        data = self.match.groupdict()
        url = update_scheme("https://", data.get("url"), force=False)
        params = parse_params(data.get("params"))
        if not params.get("period"):
            log.debug("MPEGDASHDRM: No period parameter specified. Defaulting to last available period.")
            params['period'] = -1
        # process and store plugin options before passing streams back
        for option in DASHDRM_OPTIONS:
            if option == 'decryption-key':
                if self.get_option('decryption-key'):
                    self.session.options[option] = self._process_keys()
                    # Force Streamlink to accept encrypted streams
                    self.session.set_option("stream-passthrough-encrypted", True)
                if self.get_option('ignore-mup'):
                    # increase the stream-segmented-queue-deadline when ignoring
                    # minimumUpdatePeriod so streams don't bomb out prematurely
                    # arbitrarily suggest 2 minutes of 5 second refreshs (24)
                    self.session.set_option("stream-segmented-queue-deadline", 24)
                if self.get_option('dash-segment-stream-data'):
                    # Increase connection pool size if streaming
                    adapter = HTTPAdapter(pool_connections=25, pool_maxsize=25)
                    self.session.http.mount('https://', adapter)
                    self.session.http.mount('http://', adapter)
            else:
                self.session.options[option] = self.get_option(option)

        streams = DASHStream.parse_manifest(self.session, url, **params)
        
        wrapped_streams = {}
        for name, stream in streams.items():
            # normalise presentationTimeOffsets for epoch-aligned manifests when autodetect-epoch setting is enabled
            if self.session.options.get("autodetect-epoch"):
                log.debug("MPEGDASHDRM: Option 'autodetect-epoch' option specified. Normalising presentationTimeOffsets for epoch-aligned manifests.")
                self._process_epoch_aligned_presentation_offsets(stream.mpd)
            # apply the MUP override if the option is enabled
            if self.session.options.get("ignore-mup"):
                if getattr(stream.mpd, 'minimumUpdatePeriod', None):
                    if stream.mpd.minimumUpdatePeriod.total_seconds() > 5.0:
                        stream.mpd.minimumUpdatePeriod = datetime.timedelta(seconds=5.0)
                        log.debug("MPEGDASHDRM: Clamping manifest update periods to 5.0s due to 'ignore-mup' option.")
                        
            wrapped_streams[name] = DASHStreamDRM(self.session, stream)
            
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
            log.debug("MPEGDASHDRM: Decryption Key %s has %s digits", key_val, key_len)
            
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
                    raise FatalPluginError("MPEGDASHDRM: Expecting 128bit key in 32 hex digits, or base64 equivalent.")
                    
            if len(key_val) != 32:
                raise FatalPluginError("MPEGDASHDRM: Expecting 128bit key in 32 hex digits.")
                
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

class DASHStreamWorkerDRM(DASHStreamWorker):
    reader: DASHStreamReaderDRM

    def reload(self):
        """
        Dispatchwrapparr modified reload func with period change detection
        """
        self.old_num_periods = len(self.mpd.periods)
        self.old_periods = [f"{idx}{f' (id={p.id!r})' if p.id is not None else ''}" for idx, p in enumerate(self.mpd.periods)]

        if self.closed:
            return

        self.reader.buffer.wait_free()
        log.debug(f"DASHStreamWorkerDRM: Reloading DASH manifest {self.reader.ident!r}")

        res = self.session.http.get(
            self.mpd.url,
            exception=StreamError,
            retries=self.manifest_reload_retries,
            # ensure that stream=False for manifest fetches in case `dash-segment-stream-data` option is specified
            **{**self.stream.args, "stream": False}
        )

        new_mpd = MPD(
            self.session.http.xml(res, ignore_ns=True),
            base_url=self.mpd.base_url,
            url=self.mpd.url,
            timelines=self.mpd.timelines,
        )

        # Apply the MUP override on reloaded manifests if the option is enabled
        if self.session.options.get("ignore-mup"):
            if getattr(new_mpd, 'minimumUpdatePeriod', None):
                if new_mpd.minimumUpdatePeriod.total_seconds() > 5.0:
                    new_mpd.minimumUpdatePeriod = datetime.timedelta(seconds=5.0)

        # get the current amount of periods before reload
        self.new_num_periods = len(new_mpd.periods)

        # check if period count has changed
        if self.old_num_periods != self.new_num_periods:
            log.debug(f"DASHStreamWorkerDRM: DASH Stream for Representation {self.reader.ident[2]} has changed from {self.old_num_periods} to {self.new_num_periods} periods")
            self.new_periods = [f"{idx}{f' (id={p.id!r})' if p.id is not None else ''}" for idx, p in enumerate(new_mpd.periods)]
            log.debug(f"DASHStreamWorkerDRM: Old DASH periods for Representation {self.reader.ident[2]}: {', '.join(self.old_periods)}")
            log.debug(f"DASHStreamWorkerDRM: New DASH periods for Representation {self.reader.ident[2]}: {', '.join(self.new_periods)}")
            new_period_idx = next(
                (idx for idx, p in enumerate(new_mpd.periods) if getattr(p, 'duration', None) in (None, 0)),
                len(new_mpd.periods) - 1  # fallback to last period
            )
            log.debug(f"DASHStreamWorkerDRM: Auto-selected new DASH period {new_period_idx} for Representation {self.reader.ident[2]}")
            # get new period id by index
            new_period_id = new_mpd.periods[new_period_idx].id
            # reader.ident is an immutable tuple (period_id, timeline_id, rep_id).
            # we need to replace it by constructing a new tuple while preserving the remaining parts
            try:
                old_ident = getattr(self.reader, "ident", None)
                if isinstance(old_ident, tuple):
                    rest = old_ident[1:]
                    self.reader.ident = (new_period_id,) + rest
                else:
                     # fallback: set a simple tuple with the new period id
                    self.reader.ident = (new_period_id,)
                log.debug("DASHStreamWorkerDRM: Updated reader.ident -> %r", getattr(self.reader, "ident", None))
            except Exception:
                log.exception("DASHStreamWorkerDRM: Failed to update reader.ident after period change!")

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
        log.debug(f"DASHStreamWorkerDRM: Current DASH stream period index for Representation {self.reader.ident[2]}: {current_period_index}")
    
class DASHStreamReaderDRM(DASHStreamReader):
    __worker__ = DASHStreamWorkerDRM

class DASHStreamDRM(Stream):
    def __init__(self, session, dash_stream):
        super().__init__(session)
        self.dash_stream = dash_stream

        if session.options.get("dash-segment-stream-data"):
            # This section enables functionality that's the equivalent to the 'hls-segment-stream-data' option native to Streamlink
            # Doing this prevents the requests library from blocking when downloading segments, however we don't want streaming to apply to manifest downloads.
            # Since we already override the DASHStreamWorker.reload() class.function with our own for period change detection, it's simpler to just enable Stream=True globally
            # so it covers segment downloads, but within our own DASHStreamWorkerDRM.reload() function we change stream=False so it doesn't apply here.
            if not hasattr(self.dash_stream, "args"):
                self.dash_stream.args = {}
            self.dash_stream.args["stream"] = True

    def open(self):
        video, audio = None, None
        rep_video = self.dash_stream.video_representation
        rep_audio = self.dash_stream.audio_representation
        timestamp = now()
        fds = []

        if rep_video:
            video = DASHStreamReaderDRM(self.dash_stream, rep_video, timestamp, name="video")
            video.open()
            fds.append(video)

        if rep_audio:
            rep = rep_audio[0] if isinstance(rep_audio, list) else rep_audio
            audio = DASHStreamReaderDRM(self.dash_stream, rep, timestamp, name="audio")
            audio.open()
            fds.append(audio)

        if video and audio and FFMPEGMuxerDRM.is_usable(self.session):
            return FFMPEGMuxerDRM(self.session, *fds, copyts=True).open()
        elif video:
            return video
        elif audio:
            return audio

__plugin__ = MPEGDASHDRM