import dataclasses
import json
import os
import typing

import docker.client
import docker.models.containers

from mailpy.db import EntryData, GroupData

RESOURCES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "./resources")


@dataclasses.dataclass(frozen=True)
class MongoContainerSettings:
    resources_path: str = RESOURCES_PATH
    name: str = "MONGODB_TEST_CONTAINER"
    port: int = 27017
    host: str = "localhost"
    database: str = "mailpy"
    root_username: str = "admin"
    root_password: str = "admin"
    username: str = "test"
    password: str = "test"
    image: str = "mongo:4.4.3-bionic"


def _join_path(*files):
    path = os.path.join(*files)
    if not os.path.exists(path):
        raise ValueError(f"Path {path} does not exist")
    return path


class MongoContainerManager:
    def __init__(self, config: typing.Optional[MongoContainerSettings] = None):
        if not config:
            self.config = MongoContainerSettings()
        else:
            self.config = config

        self.docker_client = docker.client.DockerClient()
        self._container: typing.Optional[docker.models.containers.Container] = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop()

    def stop(self):
        if self._container:
            self._container.stop()
            self._container.remove()
        self.docker_client.close()

    def start(self):
        self.remove_previous_mongodb_containers()
        self._container = self.create_mongodb_container()
        self._container.start()

    def _volumes(self):
        return [
            "{}:/docker-entrypoint-initdb.d/00-create-db-users.sh:ro".format(
                _join_path(self.config.resources_path, "00-create-db-users.sh")
            ),
            "{}:/docker-entrypoint-initdb.d/01-create-collections.js:ro".format(
                _join_path(self.config.resources_path, "01-create-collections.js")
            ),
            "{}:/docker-entrypoint-initdb.d/02-insert-data.sh:ro".format(
                _join_path(self.config.resources_path, "02-insert-data.sh")
            ),
            "{}:/mailpy-db-init-data:ro".format(
                _join_path(self.config.resources_path, "mailpy-db-2021-11-29")
            ),
        ]

    def create_mongodb_container(self) -> docker.models.containers.Container:
        if not self.check_image_exists(self.config.image):
            self.docker_client.images.pull(self.config.image)

        self.docker_client.images.pull(self.config.image)
        return self.docker_client.containers.create(
            self.config.image,
            name=self.config.name,
            environment={
                "MONGO_INITDB_DATABASE": self.config.database,
                "MONGO_INITDB_ROOT_USERNAME": self.config.root_username,
                "MONGO_INITDB_ROOT_PASSWORD": self.config.root_password,
                "MONGO_INITDB_USERNAME": self.config.username,
                "MONGO_INITDB_PASSWORD": self.config.password,
            },
            volumes=self._volumes(),
            ports={"27017/tcp": (self.config.host, self.config.port)},
            detach=True,
        )

    def check_image_exists(self, name):
        tags = []
        for str_l in [i.tags for i in self.docker_client.images.list()]:
            for i in str_l:
                tags.append(i)
        return name in tags

    def remove_previous_mongodb_containers(self):
        container: docker.models.containers.Container
        for container in self.docker_client.containers.list(all=True):
            if (
                type(container) == docker.models.containers.Container
                and container.name == self.config.name
            ):
                container.stop()
                container.remove()


class MongoJsonLoader:
    def __init__(
        self,
        dirname: typing.Optional[str] = None,
        entries_filename: str = "entries.json",
        groups_filename: str = "groups.json",
    ):
        if not dirname:
            dirname = dirname = os.path.join(RESOURCES_PATH, "mailpy-db-2021-11-29")

        self.entries_filename = _join_path(dirname, entries_filename)
        self.groups_filename = _join_path(dirname, groups_filename)

    def load_groups(self):
        with open(self.groups_filename, "r") as file:
            data = json.load(file)
            if type(data) != list:
                raise RuntimeError(
                    f"Expected {data} to be a list, received {type(data)}"
                )

            groups = [self._create_group(d) for d in data]
            groups.sort(key=lambda x: x.id)
            return groups

    def _create_group(self, d):
        return GroupData(
            id=d["_id"]["$oid"],
            name=d["name"],
            enabled=d["enabled"],
            description=d.get("description", ""),
        )

    def load_entries(self):
        with open(self.entries_filename, "r") as file:
            data = json.load(file)
            if type(data) != list:
                raise RuntimeError(
                    f"Expected {data} to be a list, received {type(data)}"
                )
            entries = [self._create_entry(d) for d in data]
            entries.sort(key=lambda x: x.id)
            return entries

    def _create_entry(self, d):
        return EntryData(
            id=d["_id"]["$oid"],
            pvname=d["pvname"].strip(),
            emails=d["emails"].split(";"),
            condition=d["condition"].strip(),
            alarm_values=d["alarm_values"].strip(),
            unit=d["unit"].strip(),
            warning_message=d["warning_message"].strip(),
            subject=d["subject"].strip(),
            email_timeout=d["email_timeout"],
            group=d["group"].strip(),
        )
