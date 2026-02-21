import os
import stat
import re
import subprocess
import requests
from pathlib import Path
from urllib.parse import urlparse
from core.models import StreamProfile
from apps.plugins.models import PluginConfig
from apps.plugins.loader import PluginManager
from apps.plugins.api_views import PluginReloadAPIView
from django.db import transaction
from typing import Dict, Any

class Plugin:
    name = "Dispatchwrapparr Plugin"
    version = "1.0.1"
    description = "An installer/updater for Dispatchwrapparr"
    dw_path = "/data/dispatchwrapparr/dispatchwrapparr.py"
    profile_name = "Dispatchwrapparr"
    help_text = f"Dispatchwrapparr is installed at {dw_path} and create a basic 'Dispatchwrapparr' profile."
    dw_latest = "https://raw.githubusercontent.com/jordandalley/dispatchwrapparr/refs/heads/main/VERSION"
    dw_url = "https://raw.githubusercontent.com/jordandalley/dispatchwrapparr/refs/heads/main/dispatchwrapparr.py"
    base_dir = Path(__file__).resolve().parent
    plugin_key = base_dir.name.replace(" ", "_").lower()

    def __init__(self):
        self.actions = []
        if os.path.isfile(self.dw_path) is False:
            self.actions.append(
                {
                    "id": "install",
                    "label": "Install Dispatchwrapparr",
                    "button_label": "Install",
                    "button_color": "green",
                    "description": "Installs Dispatchwrapparr into Dispatcharr. When complete, click the refresh button on the top right of the page.",
                }
            )

        else:
            confirm_update = {
                "required": True,
                "title": "Update Dispatchwrapparr?",
                "button_label": "Update",
                "button_color": "blue",
                "message": "This will update Dispatchwrapparr to the latest version. Are you sure you want to continue?",
            }
            confirm_uninstall = {
                "required": True,
                "title": "Uninstall Dispatchwrapparr?",
                "message": "After uninstallation of Dispatchwrapparr, you can then delete this plugin. You will need to manually remove any Dispatchwrapparr stream profiles from 'Settings' -> 'Stream Profiles'",
            }
            self.local_version = self.check_local_version()
            self.remote_version = self.check_remote_version()
            if self.local_version != self.remote_version:
                self.actions.append(
                    {
                        "id": "update_dw",
                        "label": "A new update is available!",
                        "button_label": "Update",
                        "description": f"This will update Dispatchwrapparr from v{self.local_version} to v{self.remote_version}. Once complete, click the refresh button on the top right of the page.",
                        "confirm": confirm_update
                    }
                )
            self.actions.append(
                {   
                    "id": "uninstall",
                    "label": "Uninstall Dispatchwrapparr",
                    "button_label": "Uninstall",
                    "button_color": "red",
                    "description": f"Uninstall Dispatchwrapparr v{self.local_version} from Dispatcharr.",
                    "confirm": confirm_uninstall
                }
            )

    # Versioning functions
    def check_local_version(self):
        if os.path.isfile(self.dw_path):
            try:
                result = subprocess.run(
                    ["python3", self.dw_path, "-v"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                tokens = result.stdout.strip().split()
                if len(tokens) >= 2:
                    return tokens[1].strip()
                else:
                    return None
            except subprocess.CalledProcessError:
                return None
        else:
            return None

    def check_remote_version(self):
        resp = requests.get(self.dw_latest)
        resp.raise_for_status()
        version = resp.text.strip()
        return version

    # Handles installation and updates
    def install(self):
        path = os.path.dirname(self.dw_path)
        os.makedirs(path, exist_ok=True)
        resp = requests.get(self.dw_url)
        resp.raise_for_status()
        with open(self.dw_path, "w", encoding="utf-8") as f:
            f.write(resp.text)
        # set executable
        st = os.stat(self.dw_path)
        os.chmod(self.dw_path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        if not StreamProfile.objects.filter(name__iexact=self.profile_name).first():
            parameters = [
                "-ua", "{userAgent}",
                "-i", "{streamUrl}"
            ]

            # Convert all paramaters into a string
            parameter_string = " ".join(parameters)

            profile = StreamProfile(
                name=self.profile_name,
                command=self.dw_path,
                parameters=parameter_string,
                locked=False,
                is_active=True,
            )
            profile.save()
        
        return {"status": "ok", "message": f"Installed Dispatchwrapparr to {self.dw_path}"}

    def update_dw(self):
        resp = requests.get(self.dw_url)
        resp.raise_for_status()
        with open(self.dw_path, "w", encoding="utf-8") as f:
            f.write(resp.text)
        # set executable
        st = os.stat(self.dw_path)
        os.chmod(self.dw_path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        return {"status": "ok", "message": f"Updated Dispatchwrapparr from v{self.local_version} to v{self.remote_version}"}

    # Uninstalled dispatchwrapparr
    def uninstall(self):
        if os.path.exists(self.dw_path):
            os.remove(self.dw_path)
            return {"status": "ok", "message": "Uninstalled Dispatchwrapparr"}
        else:
            return {"status": "error", "message": f"Path {self.dw_path} does not exist!"}

    # Main run function
    def run(self, action: str, params: dict, context: dict):
        if action == "install":
            return self.install()

        if action == "update_dw":
            return self.update_dw()

        if action == "uninstall":
            return self.uninstall()

        return {"status": "error", "message": f"Unknown action: {action}"}

