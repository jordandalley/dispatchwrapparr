import os
import shutil
import stat
import subprocess
import re
from pathlib import Path
from urllib.parse import urlparse
from core.models import StreamProfile
from apps.plugins.models import PluginConfig

class Plugin:
    name = "Dispatchwrapparr"
    version = "1.6.1"
    description = "An intelligent DRM/Clearkey capable stream profile for Dispatcharr"
    profile_name = "Dispatchwrapparr"
    install_path = "/data/dispatchwrapparr/dispatchwrapparr.py"
    plugin_dir = Path(__file__).resolve().parent
    install_source = plugin_dir / "dispatchwrapparr.py"
    plugin_key = plugin_dir.name.replace(" ", "_").lower()
    default_profile_name = "Dispatchwrapparr"

    @staticmethod
    def parse_version(version_str):
        # parse a version string into a comparable tuple of ints. Returns None if unparseable
        if not version_str:
            return None
        try:
            return tuple(int(x) for x in version_str.strip().split("."))
        except (ValueError, AttributeError):
            return None

    def __init__(self):
        self.actions = []
        try:
            self.context = PluginConfig.objects.get(key=self.plugin_key)
            self.settings = self.context.settings
        except PluginConfig.DoesNotExist:
            self.context = None
            self.settings = {}

        if os.path.isfile(self.install_path) is False:
            # on load, check if the file exists. If not, invoke installation
            self.install()
        else:
            self.local_version, self.local_version_error = self.check_local_version()
            self.packaged_version = self.version

            local_v = self.parse_version(self.local_version)
            package_v = self.parse_version(self.packaged_version)

            if local_v is not None and package_v is not None and package_v > local_v:
                # packaged version is newer than local version, invoke installation to update
                self.install()

        # Dispatchwrapparr fields for profile creation
        self.fields = [
            {
                "id": "profile_name",
                "label": "Profile Name *",
                "type": "string",
                "default": "",
                "description": "Mandatory: Enter a name for your stream profile",
            },
            {
                "id": "loglevel", "label": "Log Level", "type": "select", "default": "INFO",
                    "options": [
                        {"value": "INFO", "label": "INFO"},
                        {"value": "CRITICAL", "label": "CRITICAL"},
                        {"value": "ERROR", "label": "ERROR"},
                        {"value": "WARNING", "label": "WARNING"},
                        {"value": "DEBUG", "label": "DEBUG"},
                        {"value": "NOTSET", "label": "NOTSET"},
                ]
            },
            {
                "id": "proxy",
                "label": "Proxy Server",
                "type": "string",
                "default": "",
                "description": "Optional: Use an http proxy server for streams in this profile | Default: leave blank | Eg: 'http://proxy.address:8080'",
            },
            {
                "id": "proxybypass",
                "label": "Proxy Bypass",
                "type": "string",
                "default": "",
                "description": "Optional: If using an http proxy server, enter a comma-delimited list of hostnames to bypass | Default: leave blank | Eg: '.example.com,.example.local:8080,192.168.0.2'",
            },
            {
                "id": "clearkeys",
                "label": "Clearkeys JSON file/URL",
                "type": "string",
                "default": "",
                "description": "Optional: Specify a json file or URL that can be used to match DRM clearkeys to URL's (See Dispatchwrapparr documentation) | Default: leave blank | Eg: 'clearkeys.json' or 'https://path.to.clearkeys.api/clearkeys.json'",
            },
            {
                "id": "cookies",
                "label": "Cookies TXT file",
                "type": "string",
                "default": "",
                "description": "Optional: Specify a cookies.txt file in Mozilla format containing session information for streams | Default: leave blank | Eg: 'cookies.txt'",
            },
            {
                "id": "footnote",
                "label": "Note:",
                "type": "info",
                "description": "Please click the 'Docs' link in the plugin description for more advanced options. New profiles added via this plugin will only become available after refreshing Dispatcharr from your browser!"
            }
        ]
        confirm_profile = {
            "required": True,
            "title": "Create stream profile?",
            "message": "New profiles added via this plugin will only become available after refreshing Dispatcharr from your browser!",
        }
        self.actions = [
            {
                "id": "generate_profile",
                "label": "Generate Stream Profile",
                "button_label": "Generate Stream Profile",
                "button_color": "green",
                "description": "Create a new stream profile for Dispatchwrapparr with the specified settings",
                "confirm": confirm_profile
            },
        ]
    
    # Versioning functions
    def check_local_version(self):
        """Returns (version_string_or_None, error_string_or_None)"""
        if os.path.isfile(self.install_path):
            try:
                result = subprocess.run(
                    ["python3", self.install_path, "-v"],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    tokens = result.stdout.strip().split()
                    if len(tokens) >= 2:
                        return tokens[1].strip(), None
                    else:
                        return None, "Version output was unexpected: " + result.stdout.strip()
                else:
                    error = (result.stderr.strip() or result.stdout.strip() or "Unknown error (no output)")
                    return None, error
            except Exception as e:
                return None, str(e)
        else:
            return None, None

    # Handles installation and updates
    def install(self):
        path = os.path.dirname(self.install_path)
        os.makedirs(path, exist_ok=True)
        # copy self.install_source from source to destination
        shutil.copy2(self.install_source, self.install_path)
        # set executable
        st = os.stat(self.install_path)
        os.chmod(self.install_path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    
    # Check if URL is valid
    def is_valid_url(self, url: str) -> bool:
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def is_valid_proxy_bypass(self, value: str) -> bool:
        # Checks for compatible formats for env var NO_PROXY format
        noproxy_regex = re.compile(
            r'^(\.?[A-Za-z0-9.-]+(:\d+)?|\d{1,3}(\.\d{1,3}){3}(:\d+)?)$'
        )
        if not value:
            return True  # empty is valid (no bypass)

        parts = [v.strip() for v in value.split(",") if v.strip()]
        for part in parts:
            if not noproxy_regex.match(part):
                return False
        return True
    
    def generate_profile(self):
        if (self.settings.get("profile_name") or None) is None:
            return {"status": "error", "message": "Please specify a profile name!"}
        path = os.path.dirname(self.install_path)
        profile_name = self.settings.get("profile_name")
        if StreamProfile.objects.filter(name__iexact=profile_name).first():
            return {"status": "error", "message": f"Profile '{profile_name}' already exists, please choose a different name!"}

        parameters = [
            "-ua", "{userAgent}",
            "-i", "{streamUrl}",
            "-loglevel", (self.settings.get("loglevel") or "INFO").strip()
        ]

        # Validate and set proxy settings
        proxy = (self.settings.get("proxy") or "").strip()
        if proxy:
            if self.is_valid_url(proxy) is False:
                return {"status": "error", "message": f"Proxy Server: '{proxy}' is not a valid proxy server!"}
            parameters += ["-proxy", proxy]

        # Validate and set proxy bypass settings
        proxybypass = (self.settings.get("proxybypass") or "").strip()
        if proxybypass and not proxy:
            return {"status": "error", "message": f"Proxy Bypass cannot be used without a proxy!"}
        if proxy and proxybypass:
            if self.is_valid_proxy_bypass(proxybypass) is False:
                return {"status": "error", "message": f"Proxy Bypass: '{proxybypass}' is not valid for NO_PROXY format"}
            parameters += ["-proxybypass", proxybypass]

        # Validate and set clearkeys sources
        clearkeys = (self.settings.get("clearkeys") or "").strip()
        if clearkeys:
            # Check if it's a valid URL or if a file exists
            if self.is_valid_url(clearkeys) or os.path.isfile(clearkeys) or os.path.isfile(os.path.join(path,clearkeys)):
                parameters += ["-clearkeys", clearkeys]
            else:
                return {"status": "error", "message": f"Clearkeys: The file/url '{clearkeys}' does not exist or is invalid"}

        # Validate and set cookies file
        cookies = (self.settings.get("cookies") or "").strip()
        if cookies:
            if os.path.isfile(cookies) or os.path.isfile(os.path.join(path,cookies)):
                parameters += ["-cookies", cookies]
            else:
                return {"status": "error", "message": f"Cookies: The file '{cookies}' does not exist!"}

        # Convert all paramaters into a string
        parameter_string = " ".join(parameters)

        profile = StreamProfile(
            name=profile_name,
            command=self.install_path,
            parameters=parameter_string,
            locked=False,
            is_active=True,
        )
        profile.save()

        return {
            "status": "ok",
            "message": f"Created '{profile_name}' profile"
        }

    # Main run function
    def run(self, action: str, params: dict, context: dict):
        self.settings = context.get("settings", {})
        self.logger = context.get("logger")
        if action == "generate_profile":
            return self.generate_profile()
        return {"status": "error", "message": f"Unknown action: {action}"}