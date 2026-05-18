from __future__ import annotations

import re
import base64
import logging
import itertools

from streamlink.exceptions import FatalPluginError, StreamError
from streamlink.plugin import Plugin, pluginmatcher, pluginargument
from streamlink.plugin.plugin import HIGH_PRIORITY, parse_params, stream_weight
from streamlink.stream.dash import DASHStream, DASHStreamWorker, DASHStreamReader, DASHStreamWriter
from streamlink.stream.dash.manifest import MPD, freeze_timeline
from streamlink.stream.ffmpegmux import FFMPEGMuxer
from streamlink.stream.stream import Stream
from streamlink.utils.url import update_scheme
from streamlink.utils.times import now

log = logging.getLogger(__name__)

DASHDRM_OPTIONS = [
    "decryption-key"
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

    def _get_streams(self):
        data = self.match.groupdict()
        url = update_scheme("https://", data.get("url"), force=False)
        params = parse_params(data.get("params"))
        if not params.get("period"):
            # If no params specified, always start DASH streams from the last available period
            params['period'] = -1

        log.debug(f"URL={url}; params={params}")

        # process and store plugin options before passing streams back
        for option in DASHDRM_OPTIONS:
            if option == 'decryption-key':
                if self.get_option('decryption-key'):
                    self.session.options[option] = self._process_keys()
                    # Force Streamlink to accept encrypted streams
                    self.session.set_option("stream-passthrough-encrypted", True)
            else:
                self.session.options[option] = self.get_option(option)

        streams = DASHStream.parse_manifest(self.session, url, **params)
        
        # Wrap streams in our Custom Reader architecture
        wrapped_streams = {name: DASHStreamDRM(self.session, stream) for name, stream in streams.items()}
        return wrapped_streams

    def _process_keys(self):
        keys = self.get_option('decryption-key')
        # if a colon separated key is given, assume its kid:key and take the
        # last component after the colon
        return_keys = []
        for k in keys:
            key = k.split(':')
            key_len = len(key[-1])
            log.debug('Decryption Key %s has %s digits', key[-1], key_len)
            if key_len in (21, 22, 23, 24):
                # key len of 21-24 may mean a base64 key was provided, so we 
                # try and decode it
                log.debug("Decryption key length is too short to be hex and looks like it might be base64, so we'll try and decode it..")
                b64_string = key[-1]
                padding = 4 - (len(b64_string) % 4)
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
            # If only 1 key is given, then we use that also for all remaining
            # streams
            if len(keys) == 1:
                keys.extend(keys)
        log.debug('Decryption Keys %s', keys)
        return keys

    def __init__(self, session, *streams, **options):
        super().__init__(session, *streams, **options)
        # if a decryption key is set, we rebuild the ffmpeg command list
        # to include the key before specifying the input stream
        keys = self._get_keys(session)
        key = 0
        vid_codec_preset = None
        # Build new ffmpeg command list
        old_cmd = self._cmd.copy()
        self._cmd = []
        while len(old_cmd) > 0:
            cmd = old_cmd.pop(0)
            if keys and cmd == "-i":
                _ = old_cmd.pop(0)
                self._cmd.extend(['-thread_queue_size', '4096'])
                self._cmd.extend(['-fflags', '+genpts+discardcorrupt'])
                # 30 second timeout if no data receiving from pipes
                self._cmd.extend(['-rw_timeout', '30000000'])
                self._cmd.extend(["-decryption_key", keys[key]])
                key += 1
                # If we had more streams than keys, start with the first
                # audio key again
                if key == len(keys):
                    key = 1
                self._cmd.extend([cmd, _])
            else:
                self._cmd.append(cmd)
        log.debug("Updated ffmpeg command %s", self._cmd)

class DASHStreamWorkerDRM(DASHStreamWorker):
    reader: DASHStreamReaderDRM

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
        """
        Dispatchwrapparr modified reload func with period change detection
        """
        self.old_num_periods = len(self.mpd.periods)
        self.old_periods = [f"{idx}{f' (id={p.id!r})' if p.id is not None else ''}" for idx, p in enumerate(self.mpd.periods)]

        if self.closed:
            return

        self.reader.buffer.wait_free()
        log.debug(f"Reloading DASH manifest {self.reader.ident!r}")
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
            log.debug(f"DASH Stream for Representation {self.reader.ident[2]} has changed from {self.old_num_periods} to {self.new_num_periods} periods")
            self.new_periods = [f"{idx}{f' (id={p.id!r})' if p.id is not None else ''}" for idx, p in enumerate(new_mpd.periods)]
            log.debug(f"Old DASH periods for Representation {self.reader.ident[2]}: {', '.join(self.old_periods)}")
            log.debug(f"New DASH periods for Representation {self.reader.ident[2]}: {', '.join(self.new_periods)}")
            new_period_idx = next(
                (idx for idx, p in enumerate(new_mpd.periods) if getattr(p, 'duration', None) in (None, 0)),
                len(new_mpd.periods) - 1  # fallback: last period
            )
            log.debug(f"Auto-selected new DASH period {new_period_idx} for Representation {self.reader.ident[2]}")
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
                log.debug("DASH Stream: updated reader.ident -> %r", getattr(self.reader, "ident", None))
            except Exception:
                log.exception("DASH Stream: failed to update reader.ident after period change")

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
        log.debug(f"Current DASH stream period index for Representation {self.reader.ident[2]}: {current_period_index}")

class DASHStreamReaderDRM(DASHStreamReader):
    __worker__ = DASHStreamWorkerDRM

class DASHStreamWriterDRM(DASHStreamWriter):
    __worker__ = DASHStreamWorkerDRM

class DASHStreamDRM(Stream):
    def __init__(self, session, dash_stream):
        super().__init__(session)
        self.dash_stream = dash_stream

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