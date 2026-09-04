"""mDNS advertisement for iii-runtime-api."""

from __future__ import annotations

import socket
import ipaddress
from collections.abc import Callable
from typing import Any

from iii_drone_contracts import API_VERSION


RUNTIME_API_SERVICE_TYPE = "_iii-runtime-api._tcp.local."


def runtime_api_service_name(instance_name: str, service_type: str = RUNTIME_API_SERVICE_TYPE) -> str:
    normalized_instance = instance_name.strip().rstrip(".") or "III Runtime API"
    normalized_type = service_type if service_type.endswith(".") else f"{service_type}."
    if normalized_instance.endswith(normalized_type.rstrip(".")):
        return f"{normalized_instance.rstrip('.')}."
    return f"{normalized_instance}.{normalized_type}"


def runtime_api_advertisement_properties(
    *,
    runtime_id: str,
    runtime_name: str,
    profile: str | None,
    system_id: str,
    host: str,
    port: int,
    api_version: str = API_VERSION,
) -> dict[str, str]:
    properties = {
        "runtime_id": runtime_id,
        "runtime_name": runtime_name,
        "name": runtime_name,
        "system_id": system_id,
        "host": host,
        "port": str(port),
        "api_version": api_version,
        "scheme": "http",
    }
    if profile:
        properties["profile"] = profile
    return properties


class RuntimeApiAdvertiser:
    """Register the runtime API service in mDNS while the API process is alive."""

    def __init__(
        self,
        *,
        runtime_id: str,
        runtime_name: str,
        profile: str | None,
        bind_host: str,
        port: int,
        instance_name: str,
        system_id: str,
        advertise_host: str | None = None,
        service_type: str = RUNTIME_API_SERVICE_TYPE,
        zeroconf_factory: Callable[[], Any] | None = None,
        service_info_factory: Callable[..., Any] | None = None,
        host_resolver: Callable[[str, str | None], str] | None = None,
        server_name_resolver: Callable[[], str] | None = None,
    ):
        self.runtime_id = runtime_id
        self.runtime_name = runtime_name
        self.profile = profile
        self.bind_host = bind_host
        self.port = port
        self.instance_name = instance_name
        self.system_id = system_id
        self.advertise_host = advertise_host
        self.service_type = service_type
        self.zeroconf_factory = zeroconf_factory or _default_zeroconf_factory
        self.service_info_factory = service_info_factory or _default_service_info_factory
        self.host_resolver = host_resolver or _default_advertise_host
        self.server_name_resolver = server_name_resolver or _default_server_name
        self._zeroconf: Any | None = None
        self._service_info: Any | None = None

    def start(self) -> None:
        if self._service_info is not None:
            return
        host = self.host_resolver(self.bind_host, self.advertise_host)
        properties = runtime_api_advertisement_properties(
            runtime_id=self.runtime_id,
            runtime_name=self.runtime_name,
            profile=self.profile,
            system_id=self.system_id,
            host=host,
            port=self.port,
        )
        service_info = self.service_info_factory(
            self.service_type,
            runtime_api_service_name(self.instance_name, self.service_type),
            addresses=_inet_addresses(host),
            port=self.port,
            properties=properties,
            server=self.server_name_resolver(),
        )
        zeroconf = self.zeroconf_factory()
        try:
            zeroconf.register_service(service_info)
        except Exception:
            zeroconf.close()
            raise
        self._zeroconf = zeroconf
        self._service_info = service_info

    def stop(self) -> None:
        service_info = self._service_info
        zeroconf = self._zeroconf
        self._service_info = None
        self._zeroconf = None
        if zeroconf is None or service_info is None:
            return
        try:
            zeroconf.unregister_service(service_info)
        finally:
            zeroconf.close()


def _default_zeroconf_factory() -> Any:
    from zeroconf import Zeroconf

    return Zeroconf()


def _default_service_info_factory(
    service_type: str,
    name: str,
    *,
    addresses: list[bytes],
    port: int,
    properties: dict[str, str],
    server: str,
) -> Any:
    from zeroconf import ServiceInfo

    return ServiceInfo(
        service_type,
        name,
        addresses=addresses,
        port=port,
        properties=properties,
        server=server,
    )


def _default_advertise_host(bind_host: str, advertise_host: str | None) -> str:
    if advertise_host:
        return advertise_host
    if bind_host and bind_host not in {"0.0.0.0", "::", "*"}:
        return bind_host
    # ``gethostbyname(gethostname())`` commonly resolves to 127.0.1.1 on
    # Debian/Ubuntu hosts.  Advertising that address makes a perfectly healthy
    # remote runtime appear to live on the operator computer's loopback device.
    # A connected UDP socket performs only a route lookup (no packet is sent)
    # and gives us the address the kernel would use to reach another host.
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("192.0.2.1", 9))
        routed = str(probe.getsockname()[0])
        if not ipaddress.ip_address(routed).is_loopback:
            return routed
    except OSError:
        pass
    finally:
        probe.close()
    try:
        resolved = socket.gethostbyname(socket.gethostname())
        if not ipaddress.ip_address(resolved).is_loopback:
            return resolved
    except (OSError, ValueError):
        pass
    raise RuntimeError(
        "cannot advertise a wildcard-bound runtime API without a routable IPv4 address"
    )


def _default_server_name() -> str:
    hostname = socket.gethostname().split(".", maxsplit=1)[0] or "iii-runtime-api"
    return f"{hostname}.local."


def _inet_addresses(host: str) -> list[bytes]:
    try:
        return [socket.inet_aton(host)]
    except OSError:
        return []
