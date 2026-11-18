"""Temporary Rabisu outbound for this collector; main ClashSub is read-only."""

from __future__ import annotations

import copy
import hashlib
import json
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

from sigbot.errors import AuthorizationRequired, ContractNotVerified, TransportUnknown


class RabisuProxy:
    def __init__(self, *, base=None, node="Rabisu-LA", config_path=None):
        if base is None:
            base = Path.home() / "clashctl"
        self.base, self.node = base, node
        self.config_path = (
            Path(config_path)
            if config_path
            else base / "resources/subscriptions/rabisu-share-2026-09-13.yaml"
        )
        self.url = None
        self.process = None
        self.temp = None
        self.evidence: dict = {"route": "RABISU_ISOLATED", "node": node}

    @staticmethod
    def local_yaml(path):
        import yaml  # type: ignore[import-untyped]

        try:
            data = yaml.safe_load(path.read_text())
        except (yaml.YAMLError, OSError):
            # Parser exception text can include credential-bearing source lines.
            raise ContractNotVerified("RABISU_LOCAL_CONFIG_UNAVAILABLE_OR_INVALID") from None
        if not isinstance(data, dict):
            raise ContractNotVerified("RABISU_LOCAL_CONFIG_NOT_OBJECT")
        return data

    def snapshot(self):
        paths = [self.base / "resources" / x for x in ("profiles.yaml", "runtime.yaml")]
        result: dict = {
            "files": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
        }
        config = self.local_yaml(paths[1])
        port = str(config.get("external-controller", "")).rsplit(":", 1)[-1]
        if not port.isdecimal():
            result["selectors_status"] = "UNAVAILABLE"
            return result
        request = urllib.request.Request("http://127.0.0.1:" + port + "/proxies")
        if config.get("secret"):
            request.add_header("Authorization", "Bearer " + str(config["secret"]))
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=3) as response:
                data = json.load(response)
            result.update(
                selectors_status="OBSERVED",
                selectors={
                    k: v.get("now")
                    for k, v in data["proxies"].items()
                    if v.get("type") == "Selector"
                },
            )
        except (OSError, ValueError, KeyError):
            result["selectors_status"] = "UNAVAILABLE"
        return result

    def __enter__(self):
        import yaml  # type: ignore[import-untyped]

        # The cached Rabisu subscription is data. Never execute it, refresh it,
        # change a main selector, copy its general settings, or expose a controller.
        config = self.local_yaml(self.config_path)
        nodes = [p for p in config.get("proxies", []) if p.get("name") == self.node]
        if len(nodes) != 1:
            raise AuthorizationRequired("RABISU_NODE_NOT_UNIQUE_IN_CACHED_SUBSCRIPTION")
        node = copy.deepcopy(nodes[0])
        if node.get("dialer-proxy") or node.get("detour"):
            raise ContractNotVerified("RABISU_DEPENDENT_OUTBOUND_UNRESOLVED")
        self.evidence["main_before"] = self.snapshot()
        try:
            self.temp = tempfile.TemporaryDirectory(prefix="sig-rabisu-")
            private = Path(self.temp.name)
            private.chmod(0o700)
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
                node["name"] = "collector-rabisu"
                isolated = {
                    "allow-lan": False,
                    "bind-address": "127.0.0.1",
                    "mode": "rule",
                    "log-level": "silent",
                    "ipv6": False,
                    "proxies": [node],
                    "listeners": [
                        {
                            "name": "collector",
                            "type": "http",
                            "listen": "127.0.0.1",
                            "port": port,
                            "proxy": node["name"],
                        }
                    ],
                    "rules": ["MATCH,collector-rabisu"],
                    "profile": {"store-selected": False},
                    "tun": {"enable": False},
                }
                path = private / "config.yaml"
                path.write_text(yaml.safe_dump(isolated, allow_unicode=True))
                path.chmod(0o600)
                binary = str(self.base / "bin/mihomo")
                env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
                check = subprocess.run(
                    [binary, "-t", "-d", str(private), "-f", str(path)],
                    capture_output=True,
                    timeout=15,
                    env=env,
                )
                if check.returncode:
                    raise ContractNotVerified("RABISU_ISOLATED_CONFIG_INVALID")
            # Logs may contain node secrets: discard them, including validation text.
            self.process = subprocess.Popen(
                [binary, "-d", str(private), "-f", str(path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=env,
            )
            deadline = time.monotonic() + 8
            while self.process.poll() is None and time.monotonic() < deadline:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        self.url = f"http://127.0.0.1:{port}"
                        self.evidence["listener"] = self.url
                        return self
                except OSError:
                    time.sleep(0.05)
            raise TransportUnknown("RABISU_ISOLATED_LISTENER_UNAVAILABLE")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.evidence["temporary_process_stopped"] = (
            self.process is None or self.process.poll() is not None
        )
        if self.temp is not None:
            name = self.temp.name
            self.temp.cleanup()
            self.evidence["temporary_config_removed"] = not Path(name).exists()
        after = self.snapshot()
        before = self.evidence["main_before"]
        self.evidence.update(
            main_after=after,
            main_unchanged=before == after and before.get("selectors_status") == "OBSERVED",
        )
