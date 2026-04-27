# Dispatchwrapparr - Super wrapper for Dispatcharr

<p align="center">
  <img src="https://github.com/user-attachments/assets/eb65168b-e24f-4e0c-b17b-7d72021d1d15" height="250" alt="Dispatchwrapparr Logo" />
</p>

## 🤝 What does Dispatchwrapparr do?

✅ **Builtin DASH and HLS Clearkey/DRM Support** — Either append a `#clearkey=<clearkey>` fragment to the end of the URL or include a clearkeys json file or URL for DRM decryption\
✅ **High Performance** — Uses streamlink API's for segment dowloading which significantly improves channel start times\
✅ **Highly Flexible** — Can support standard HLS, Mpeg-DASH as well as DASH-DRM, Youtube, Twitch and other livestreaming services as channels\
✅ **Proxy and Proxy Bypass Support** — Full support for passing proxy servers to bypass geo restrictions. Also support for bypassing proxy for specific URL's used in initial redirections or supply of clearkeys\
✅ **Custom Header Support** — Currently supports the 'Referer' and 'Origin' headers by appending `#referer=<URL>` or `#origin=<URL>` (or both) fragments to the end of the URL\
✅ **Cookie Jar Support** — Supports loading of cookie jar txt files in Netscape/Mozilla format\
✅ **Extended Stream Type Detection** — Fallback option that checks MIME type of stream URL for streamlink plugin selection\
✅ **Streaming Radio Support with Song Information** — Play streaming radio to your TV with song information displayed on your screen for ICY and HLS stream types\
✅ **Automated Stream Variant Detection** — Detects streams with no video or no audio and muxes in the missing components for compatibility with most players\
✅ **Support for SSAI/DAI** — Supports streams using SCTE-35 type discontinuities for Server-Side or Dynamic Ad Injection

---

## 🚀 Installation

For ease of installation, Dispatchwrapparr can be installed through the official Dispatcharr plugin repository. Alternatively, it can be downloaded here: [Dispatchwrapparr Releases](https://github.com/jordandalley/dispatchwrapparr/releases/latest)

Once the Dispatchwrapparr plugin is installed, configuring your first profile is easy!

Simply enter in a name for your stream profile, then click the 'Generate Stream Profile' button.

Note: For the new stream profile to appear, you will need to refresh Dispatcharr from your browser.

<img width="608" height="982" alt="image" src="https://github.com/user-attachments/assets/bcfe255f-6454-437f-8016-e8a52776f39b" />

---

## 🛞 URL Fragment Options

URL fragment options can be used to tell Dispatchwrapparr what to do with a specific stream.

***Important: When using URL fragment options, it is recommended that you remove "URL" from the "M3U Hash Key" option in Dispatcharr. This setting can be found in 'Settings' > 'Stream Settings'.***

Below is a list of fragment options and their specifc usage:

| Fragment        | Type          | Example Usage                                | Description                                                                                                                                                                                  |
| :---            | :---          | :---                                         | :---                                                                                                                                                                                         | 
| clearkey        | String        | `#clearkey=7ff8541ab5771900c442f0ba5885745f` | Defines the DRM Clearkey for decryption of stream content                                                                                                                                    | 
| header          | String        | `#header=Authorization:Bearer%20XYZ&header=Origin:https://example.com` | Adds one or more custom HTTP headers using repeated `header=<Header-Name>:<Header-Value>` fragments                                                                |
| referer         | String        | `#referer=https://somesite.com/`             | Defines the 'Referer' header to use for the stream URL                                                                                                                                       | 
| origin          | String        | `#origin=https://somesite.com/`              | Defines the 'Origin' header to use for the stream URL                                                                                                                                        | 
| stream          | String        | `#stream=1080p_alt`                          | Override Dispatchwrapparr automatic stream selection with a manual selection for the stream URL                                                                                              | 
| novariantcheck  | Bool          | `#novariantcheck=true`                       | Do not automatically detect audio-only or video-only streams and mux in blank video or silent audio for compatibility purposes. Just pass through the stream as-is (without video or audio). |
| ffmpeg_nocopyts | Bool          | `#ffmpeg_nocopyts=true`                      | Do not copy timestamps during stream muxing. May help solve playability issues with some streams.                                                                                            |
| noaudio         | Bool          | `#noaudio=true`                              | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no audio. This instructs Dispatchwrapparr to mux in silent audio.                                |
| novideo         | Bool          | `#novideo=true`                              | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no video. This instructs Dispatchwrapparr to mux in blank video.                                 |

Important notes about fragment options:

- Fragments can be added to stream URL's inside m3u8 playlists, or added to stream URL's that are manually added as channels into Dispatcharr.
- Fragments are never passed to the origin. They are stripped off the URL before the actual stream is requested.
- Fragments will override any identical options specified by CLI arguments (Eg. `-clearkeys` / `#clearkey` or `-stream` / `#stream` ).
- `header` fragments can be repeated. If the same header appears more than once, the last value wins.
- Multiple fragments can be used, and can be separated by ampersand. (Eg. `https://stream.url/stream.manifest#clearkey=7ff8541ab5771900c442f0ba5885745f&referer=https://somesite.com/&stream=1080p_alt`).

### 🧑‍💻 Using the 'clearkey' URL fragment for DRM decryption

To use a clearkey for a particular stream using a URL fragment, simply create a custom m3u8 file that places the #clearkey=<clearkey> fragment at the end of the stream URL.

Below is an example that could be used for Channel 4 (UK):

```channel-4-uk.m3u8
#EXTM3U
#EXTINF:-1 group-title="United Kingdom" channel-id="Channel4London.uk" tvg-id="Channel4London.uk" tvg-logo="https://raw.githubusercontent.com/tv-logo/tv-logos/main/countries/united-kingdom/channel-4-uk.png", Channel 4
https://olsp.live.dash.c4assets.com/dash_iso_sp_tl/live/channel(c4)/manifest.mpd#clearkey=5ce85f1aa5771900b952f0ba58857d7a
```

You can also add the cleakey fragment to the end of a URL of a channel that you add manually into Dispatcharr.

More channels can be added to the same m3u8 file, and may also contain a mixture of DRM and non-DRM encrypted streams.

Simply upload your m3u8 file into Dispatcharr, select a Dispatchwrapparr stream profile, and it'll do the rest.

### ▶️ Using the 'stream' URL fragment for manual stream variant/quality selection

The `#stream` fragment allows you to manually select a stream variant. Sometimes there may be occasions where you may want to manually select various stream variants depending on your preferences.

To find a list of available variants for a particular stream, simply run the following on the system running the dispatcharr docker container:

`docker logs dispatcharr -f --since 0m | grep 'Available streams'`

Once this command is running, play the stream and look at the output of the above command. It should show you a list of what variants are available.

Some examples of outputs are shown below:

`2025-10-14 19:44:01,863 INFO ts_proxy.stream_manager FFmpeg info for channel 86952480-4c6b-4df1-a60b-306e28a43cb3: [dispatchwrapparr] 2025-10-14 19:44:01,860 [info] Available streams: 270p_alt, 270p, 360p_alt, 360p, 480p_alt, 480p, 720p_alt2, 720p_alt, 720p, 1080p_alt, 1080p, worst, best`

`2025-10-14 19:58:25,926 INFO ts_proxy.stream_manager FFmpeg info for channel bda87427-1a81-43d0-8e3d-84b2cf3484b4: [dispatchwrapparr] 2025-10-14 19:58:25,926 [info] Available streams: 720p+a128k_48k, 720p+a128k_44k_alt, 720p+a128k_48k_alt2, 720p+a128k_44k_alt3, 540p+a128k_48k, 540p+a128k_44k_alt, 360p+a128k_48k, 360p+a128k_44k_alt, 270p+a128k_48k, 270p+a128k_44k_alt, best, worst`

Once you have the stream you wish to use, eg. '1080p_alt', then all you need to do is append the fragment to the end of the stream URL as follows: `https://some.stream.com/playlist.m3u8#stream=1080p_alt`

In instances where a stream variants contain special characters such as '+' like in the second example above, you will need to ensure that the URL is encoded correctly. The '+' character is '%2B'.

For example, to select the '720p+a128k_48k' stream variant, then it would look like this: `https://some.stream.com/playlist.m3u8#stream=720p%2Ba128k_48k`

---

## ⚙️ CLI Arguments

| Argument                | Type     | Example Values                                            | Description                                                                                                                                                                                  |
| :---                    | :---     | :---                                                      | :---                                                                                                                                                                                         | 
| -i                      | Required | `{streamUrl}`                                             | Input stream URL from Dispatcharr.                                                                                                                                                           |
| -ua                     | Required | `{userAgent}`                                             | Input user-agent header from Dispatcharr.                                                                                                                                                    |
| -clearkeys              | Optional | `/path/to/clearkeys.json` or `https://url.to/clearkeys`   | Supply a json file or URL containing URL -> Clearkey pairs.                                                                                                                                  |
| -proxy                  | Optional | `http://proxy.server:8080`                                | Configure a proxy server. Supports HTTP and HTTPS proxies only.                                                                                                                              |
| -proxybypass            | Optional | `.domain.com,192.168.0.100:80`                            | A comma delimited list of hostnames to bypass. Eg. '.local,192.168.0.44:90'. Do not use "*", this is unsupported. Whole domains match with '.'                                               |
| -cookies                | Optional | `cookies.txt` or `/path/to/cookies.txt`                   | Supply a cookies txt file in Mozilla/Netscape format for use with streams                                                                                                                    |
| -customheaders          | Optional | `'{"Authentication": "Bearer abc123", "Header": "Value"}'`| Supply a JSON string containing custom header values                                                                                                                                         |
| -streamlink_plugins     | Optional | `/path/to/streamlink/plugins`                             | Specify a custom path for any Streamlink plugins that you wish to load                                                                                                                       |
| -stream                 | Optional | `1080p_alt` or `worst`                                    | Override Dispatchwrapparr automatic stream selection with a manual selection for the stream URL                                                                                              |
| -ffmpeg                 | Optional | `/path/to/ffmpeg`                                         | Specify the location of an ffmpeg binary for use in stream muxing instead of auto detecting ffmpeg binaries in PATH or in the same directory as dispatchwrapparr.py                          |
| -ffmpeg_transcode_audio | Optional | `copy`, `eac3`, `aac`, `ac3`                              | Enables the ffmpeg option to transcode audio. By default, dispatchwrapparr just copies the audio.                                                                                            |
| -ffmpeg_nocopyts        | Optional |                                                           | Do not copy timestamps during stream muxing. May help solve playability issues with some streams.                                                                                            |
| -novariantcheck         | Optional |                                                           | Do not automatically detect audio-only or video-only streams and mux in blank video or silent audio for compatibility purposes. Just pass through the stream as-is (without video or audio). |
| -noaudio                | Optional |                                                           | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no audio. This instructs Dispatchwrapparr to mux in silent audio.                                |
| -novideo                | Optional |                                                           | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no video. This instructs Dispatchwrapparr to mux in blank video.                                 |
| -nosonginfo             | Optional |                                                           | Disables the display of song information for radio streams. Only a blank video will be muxed                                                                                                 |
| -loglevel               | Optional | `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`, `NOTSET` | Sets the python and ffmpeg log levels. By default, the loglevel is set to 'INFO'                                                                                                             |
| -subtitles              | Optional |                                                           | Enable muxing of subtitles. Disabled by default. NOTE: Subtitle support in streamlink is limited at best - this may not work as intended                                                     |

Example: `dispatchwrapparr.py -i {streamUrl} -ua {userAgent} -proxy http://your.proxy.server:3128 -proxybypass 192.168.0.55,.somesite.com -clearkeys clearkeys.json -loglevel INFO`

### 💡 Using the -clearkeys CLI argument for DRM decryption

The -clearkeys CLI argument is perfect for building custom API's or supplying json files that contain or supply automatically rotated URL -> Clearkey pairs to Dispatchwrapparr for DRM decryption.

Below is an example of what Dispatchwrapparr expects in the json API response or file contents:

```clearkeys.json
{
  "https://olsp.live.dash.c4assets.com/dash_iso_sp_tl/live/channel(c4)/manifest.mpd": "5ce85f1aa5771900b952f0ba58857d7a",
  "https://some.other.stream.com/somechannel/*.mpd": "7ff8541ab5771900c442f0ba5885745f"
}
```

For streams where the video and audio use different clearkeys, place them in a comma separated list. Eg:

```clearkeys.json
{
  "https://olsp.live.dash.c4assets.com/dash_iso_sp_tl/live/channel(c4)/manifest.mpd": "5ce85f1aa5771900b952f0ba58857d7a,7ff8541ab5771900c442f0ba5885745f",
  "https://some.other.stream.com/somechannel/*.mpd": "7ff8541ab5771900c442f0ba5885745f"
}
```

- A json file can be specified by just the filename (Eg. `-clearkeys clearkeys.json`) where it will use the file 'clearkeys.json' within the same directory as dispatchwrapparr.py (Usually /data/dispatchwrapparr), or an absolute path to a json file (Eg. `-clearkeys /path/to/clearkeys.json`)
- A json HTTP API can be specified by providing the URL to the -clearkeys argument (Eg. `-clearkeys https://someserver.local/clearkeys?getkeys`)
- When using the `-proxy` directive, be careful to ensure that you add your clearkeys api endpoints into the `-proxybypass` list if the endpoints are local to your network
- Wildcards/Globs (*) are supported by Dispatchwrapparr for URL -> Clearkey matching. (Eg. The URL string could look like this and still match a Clearkey `https://olsp.live.dash.c4assets.com/*/live/channel(c4)/*.mpd`)
- Supports KID:KEY combinations, and comma delimited lists of clearkeys where multiple keys are required, although only the Clearkey is needed.
- If `-clearkeys` is specified, and no stream URL matches a clearkey, Dispatchwrapparr will simply carry on as normal and treat the stream as if it's not DRM encrypted

---

## ‼️ Troubleshooting

### Jellyfin IPTV streaming issues

In Jellyfin there are a number of settings related to m3u8 manifests.

Make sure that all options ("Allow fMP4 transcoding container", "Allow stream sharing", "Auto-loop live streams", "Ignore DTS (decoding timestamp)", and "Read input at native frame rate") are unticked/disabled.

### My stream only plays audio or won't start

Sometimes broadcasters don't include timestamps in the audio stream. Since version 1.6.2, Dispatchwrapparr copies timestamps by default when muxing. There may be occasions where you may need to disable this feature.

See the `-ffmpeg_nocopyts` option or `#ffmpeg_nocopyts=true` url fragment options for more details.

### My streams stop on ad breaks, why?

This is a technology called SCTE-35 (aka. SSAI or DAI) which injects ads/commercial breaks into streams based on parameters such as geolocation and demographics etc.

While dispatchwrapparr has had some success in dealing with these types of streams, due to the way that some broadcasters implement SCTE-35 it may not always be stable.

### Can I use a custom Streamlink plugin?

Yes, maybe. Dispatchwrapparr will look for plugins that are placed into the same directory as itself and load them. However, some plugins require Chromium based browsers for session tokens, and/or require additional arguments which Dispatchwrapparr will not pass through.

---

## ❤️ Shoutouts

This script was made possible thanks to many wonderful python libraries and open source projects.

- [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) development community for making such an awesome stream manager!
- [SergeantPanda](https://github.com/SergeantPanda) for support and guidance on the Dispatcharr discord
- [OkinawaBoss](https://github.com/OkinawaBoss) for creating the Dispatcharr plugin system and providing example code
- [Streamlink](https://streamlink.github.io/) for their awesome API and stream handling capability
- [titus-au](https://github.com/titus-au/streamlink-plugin-dashdrm) who laid a lot of the groundwork for managing DASHDRM streams in streamlink!
- [matthuisman](https://github.com/matthuisman) this guy is a local streaming legend in New Zealand. His code and work with streams has taught me heaps!

## ⚖️ License
This project is licensed under the [MIT License](LICENSE).
