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
✅ **Automated Stream Variant Detection** — Detects streams with no video or no audio and muxes in the missing components for compatibility with most players

---

## 🚀 Installation

For ease of installation, Dispatchwrapparr can be installed via the Dispatchwrapparr Plugin.

1. Download the latest [Dispatchwrapparr Plugin](https://github.com/jordandalley/dispatchwrapparr/releases/latest) zip file 
2. In Dispatcharr, navigate to 'Settings' > 'Plugins'
3. Click the 'Import Plugin' button and select the Dispatchwrapparr Plugin zip file you just downloaded
3. Select 'Enable Now', and then 'Enable'
4. Once the plugin is loaded, click 'Run' inside the 'Install Dispatchwrapparr' section
<img width="489" height="278" alt="image" src="https://github.com/user-attachments/assets/0b00bdd6-7ad9-428c-b2b0-66e62279e747" />

5. An alert box should come up to confirm installation
<img width="350" height="87" alt="image" src="https://github.com/user-attachments/assets/082e4a58-6d1e-4945-bcae-168692a667be" />

6. Click the refresh icon <img width="29" height="29" alt="image" src="https://github.com/user-attachments/assets/0945ad01-9af6-49bf-80e6-ff9607bdc501" /> to display all available settings

## ➡️ Create a Dispatchwrapparr stream profile

Dispatchwrapparr profiles can either be created manually under 'Settings' > 'Stream Profiles', or through the plugin interface.
To create profiles manually, Dispatchwrapparr is usually installed under `/data/dispatchwrapparr/dispatchwrapparr.py`.

1. Create a new profile using the Dispatchwrapparr Plugin by navigating to 'Settings' > 'Plugins'
2. Enter a 'Profile Name' and fill in any other relevant details for the profile.
3. Click 'Run' next to 'Create Stream Profile'
4. Refresh your browser, then apply the profile to any particular streams that you want
5. Now select 'dispatchwrapparr' as your preferred profile on any particular streams!

---

## 🛞 URL Fragment Options

URL fragment options can be used to tell Dispatchwrapparr what to do with a specific stream.

***Important: When using URL fragment options, it is recommended that you remove "URL" from the "M3U Hash Key" option in Dispatcharr. This setting can be found in 'Settings' > 'Stream Settings'.***

Below is a list of fragment options and their specifc usage:

| Fragment       | Type          | Example Usage                                | Description                                                                                                                                                                                  |
| :---           | :---          | :---                                         | :---                                                                                                                                                                                         | 
| clearkey       | String        | `#clearkey=7ff8541ab5771900c442f0ba5885745f` | Defines the DRM Clearkey for decryption of stream content                                                                                                                                    | 
| referer        | String        | `#referer=https://somesite.com/`             | Defines the 'Referer' header to use for the stream URL                                                                                                                                       | 
| origin         | String        | `#origin=https://somesite.com/`              | Defines the 'Origin' header to use for the stream URL                                                                                                                                        | 
| stream         | String        | `#stream=1080p_alt`                          | Override Dispatchwrapparr automatic stream selection with a manual selection for the stream URL                                                                                              | 
| novariantcheck | Bool          | `#novariantcheck=true`                       | Do not automatically detect audio-only or video-only streams and mux in blank video or silent audio for compatibility purposes. Just pass through the stream as-is (without video or audio). |
| noaudio        | Bool          | `#noaudio=true`                              | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no audio. This instructs Dispatchwrapparr to mux in silent audio.                                |
| novideo        | Bool          | `#novideo=true`                              | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no video. This instructs Dispatchwrapparr to mux in blank video.                                 |

Important notes about fragment options:

- Fragments can be added to stream URL's inside m3u8 playlists, or added to stream URL's that are manually added as channels into Dispatcharr.
- Fragments are never passed to the origin. They are stripped off the URL before the actual stream is requested.
- Fragments will override any identical options specified by CLI arguments (Eg. `-clearkeys` / `#clearkey` or `-stream` / `#stream` ).
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

| Argument          | Type     | Example Values                                            | Description                                                                                                                                                                                  |
| :---              | :---     | :---                                                      | :---                                                                                                                                                                                         | 
| -i                | Required | `{streamUrl}`                                             | Input stream URL from Dispatcharr.                                                                                                                                                           |
| -ua               | Required | `{userAgent}`                                             | Input user-agent header from Dispatcharr.                                                                                                                                                    |
| -clearkeys        | Optional | `/path/to/clearkeys.json` or `https://url.to/clearkeys`   | Supply a json file or URL containing URL -> Clearkey pairs.                                                                                                                                  |
| -proxy            | Optional | `http://proxy.server:8080`                                | Configure a proxy server. Supports HTTP and HTTPS proxies only.                                                                                                                              |
| -proxybypass      | Optional | `.domain.com,192.168.0.100:80`                            | A comma delimited list of hostnames to bypass. Eg. '.local,192.168.0.44:90'. Do not use "*", this is unsupported. Whole domains match with '.'                                               |
| -cookies          | Optional | `cookies.txt` or `/path/to/cookies.txt`                   | Supply a cookies txt file in Mozilla/Netscape format for use with streams                                                                                                                    |
| -stream           | Optional | `1080p_alt` or `worst`                                    | Override Dispatchwrapparr automatic stream selection with a manual selection for the stream URL                                                                                              |
| -ffmpeg           | Optional | `/path/to/ffmpeg`                                         | Specify the location of an ffmpeg binary for use in stream muxing instead of auto detecting ffmpeg binaries in PATH or in the same directory as dispatchwrapparr.py                          |
| -ffmpeg_transcode_audio | Optional |                                                     | Enables the ffmpeg option to transcode audio. By default, dispatchwrapparr just copies the audio. Valid inputs are 'aac', 'ac3', 'eac3' etc.                                                                        |
| -novariantcheck   | Optional |                                                           | Do not automatically detect audio-only or video-only streams and mux in blank video or silent audio for compatibility purposes. Just pass through the stream as-is (without video or audio). |
| -noaudio          | Optional |                                                           | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no audio. This instructs Dispatchwrapparr to mux in silent audio.                                |
| -novideo          | Optional |                                                           | Disables variant checking (-novariantcheck) and manually specifies that the stream contains no video. This instructs Dispatchwrapparr to mux in blank video.                                 |
| -nosonginfo       | Optional |                                                           | Disables the display of song information for radio streams. Only a blank video will be muxed                                                                                                 |
| -loglevel         | Optional | `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`, `NOTSET` | Sets the python and ffmpeg log levels. By default, the loglevel is set to 'INFO'                                                                                                             |
| -subtitles        | Optional |                                                           | Enable muxing of subtitles. Disabled by default. NOTE: Subtitle support in streamlink is limited at best - this may not work as intended                                                     |

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

- A json file can be specified by just the filename (Eg. `-clearkeys clearkeys.json`) where it will use the file 'clearkeys.json' within the same directory as dispatchwrapparr.py (Usually /data/dispatchwrapparr), or an absolute path to a json file (Eg. `-clearkeys /path/to/clearkeys.json`)
- A json HTTP API can be specified by providing the URL to the -clearkeys argument (Eg. `-clearkeys https://someserver.local/clearkeys?getkeys`)
- When using the `-proxy` directive, be careful to ensure that you add your clearkeys api endpoints into the `-proxybypass` list if the endpoints are local to your network
- Wildcards/Globs (*) are supported by Dispatchwrapparr for URL -> Clearkey matching. (Eg. The URL string could look like this and still match a Clearkey `https://olsp.live.dash.c4assets.com/*/live/channel(c4)/*.mpd`)
- Supports KID:KEY combinations, and comma delimited lists of clearkeys where multiple keys are required, although only the Clearkey is needed.
- If `-clearkeys` is specified, and no stream URL matches a clearkey, Dispatchwrapparr will simply carry on as normal and treat the stream as if it's not DRM encrypted

---

## ‼️ Troubleshooting

### Garbled/Green Video for DRM streams Dispatcharr builds 0.9.0-??

There is currently an issue with decrypting DRM streams since the release of Dispatcharr 0.9.0.

Dispatcharr uses [docker-ffmpeg](https://github.com/linuxserver/docker-ffmpeg) as a build layer, which was recently updated to ffmpeg 8.0. The particular build of ffmpeg 8.0 used in docker-ffmpeg contains a bug which causes garbled/green video content.

This bug has been fixed in subsequent builds of ffmpeg 8.0 but is not yet part of the main release.

The solution at this stage is to download either an older or newer 'portable' ffmpeg binary and place it into the same directory as dispatchwrapparr.py. This is usually in `/data/dispatchwrapparr`.

Some options are:

- Jellyfin FFmpeg 7.0 Releases: Download the latest [jellyfin-ffmpeg_7.x_portable_linux64-gpl.tar.xz](https://github.com/jellyfin/jellyfin-ffmpeg/releases) release binaries
- BtbN FFmpeg 8.0 Auto Builds: Download the latest [ffmpeg-master-latest-linux64-gpl.tar.xz](https://github.com/BtbN/FFmpeg-Builds/releases) auto-build binaries with bugfix applied

### Out of Sync Audio or Streams stopping prematurely

Different broadcasters use an array of different settings to deliver streaming content.

If audio is out of sync, or streams are stopping prematurely (freezing), try the cli option `-ff_start_at_zero`.

You may need to split this option off into its own Dispatcharr profile as it may affect other channels.

### Jellyfin IPTV streaming issues

In Jellyfin there are a number of settings related to m3u8 manifests.

Make sure that all options ("Allow fMP4 transcoding container", "Allow stream sharing", "Auto-loop live streams", "Ignore DTS (decoding timestamp)", and "Read input at native frame rate") are unticked/disabled.

### My streams stop on ad breaks, why?

This is a technology called SCTE-35 (aka. SSAI or DAI) which is injects ads/commercial breaks into streams based on parameters such as geolocation and demographics etc.

FFmpeg and pretty much all players balk at it, because instead of a continuous stream like players/ffmpeg expects, it's more like a playlist.

When the TV channel gets to an ad break, the ads are inserted into the dash or hls manifest. In some cases, it switches from separate audio and video streams, to a single stream with an already muxed mp4 file containing audio and video.

This makes it extremely difficult to work around. At this stage, it is not anticipated the Dispatchwrapparr will support SCTE-35, however it won't be ruled out in a future release.

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
