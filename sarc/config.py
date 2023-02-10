import json
import os
from contextlib import contextmanager
from contextvars import ContextVar
from functools import cached_property
from pathlib import Path
from typing import Union

from pydantic import BaseModel as _BaseModel
from pydantic import Extra, validator


class BaseModel(_BaseModel):
    class Config:
        # Forbid extra fields that are not explicitly defined
        extra = Extra.forbid
        # Ignore cached_property, this avoids errors with serialization
        keep_untouched = (cached_property,)


class ClusterConfig(BaseModel):
    host: str
    prometheus_url: str = None
    name: str = None
    sshconfig: Path = None

    @cached_property
    def ssh(self):
        from fabric import Config as FabricConfig
        from fabric import Connection
        from paramiko import SSHConfig

        if self.sshconfig is None:
            return Connection(self.host)
        else:
            fconfig = FabricConfig(ssh_config=SSHConfig.from_path(self.sshconfig))
            return Connection(self.host, config=fconfig)

    @cached_property
    def prometheus(self):
        from prometheus_api_client import PrometheusConnect

        if self.prometheus_url is None:
            raise Exception(f"No prometheus URL provided for cluster '{self.name}'")
        return PrometheusConnect(url=self.prometheus_url)


class MongoConfig(BaseModel):
    url: str
    database: str

    @cached_property
    def instance(self):
        from pymongo import MongoClient

        client = MongoClient(self.url)
        return client.get_database(self.database)


class Config(BaseModel):
    mongo: MongoConfig
    sshconfig: Path = None
    cache: Path = None
    clusters: dict[str, ClusterConfig]

    @validator("cache", "sshconfig")
    def __absolute_path(cls, value):
        return value and value.expanduser().absolute()

    @validator("clusters")
    def __complete_cluster_fields(cls, value, values):
        for name, cluster in value.items():
            if not cluster.name:
                cluster.name = name
            if not cluster.sshconfig and "sshconfig" in values:
                cluster.sshconfig = values["sshconfig"]
        return value


config_var = ContextVar("config", default=None)


def parse_config(config_path):
    config_path = Path(config_path)

    if not config_path.exists():
        raise Exception(
            f"Cannot read SARC configuration file: '{config_path}'"
            " Use the $SARC_CONFIG environment variable to choose the config file."
        )

    try:
        cfg = Config.parse_file(config_path)
    except json.JSONDecodeError as exc:
        raise Exception(f"'{config_path}' contains malformed JSON") from exc

    return cfg


def config():
    if (current := config_var.get()) is not None:
        return current
    cfg = parse_config(os.environ.get("SARC_CONFIG", "config/sarc-dev.json"))
    config_var.set(cfg)
    return cfg


@contextmanager
def using_config(cfg: Union[str, Path, Config]):
    if isinstance(cfg, (str, Path)):
        cfg = parse_config(cfg)
    token = config_var.set(cfg)
    yield cfg
    config_var.reset(token)