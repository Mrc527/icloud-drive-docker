__author__ = "Mandar Patil (mandarons@pm.me)"

import os
import re
import time
from pathlib import Path
from shutil import copyfileobj, rmtree

import more_itertools
from icloudpy import exceptions
import asyncio

from src import config_parser, LOGGER
from src.tools import background, gather_with_concurrency


def wanted_file(filters, file_path):
    if not file_path:
        return False
    if not filters or len(filters) == 0:
        return True
    for file_extension in filters:
        if re.search(f"{file_extension}$", file_path, re.IGNORECASE):
            return True
    LOGGER.debug(f"Skipping the unwanted file {file_path}")
    return False


def wanted_folder(filters, root, folder_path):
    if not filters or not folder_path or not root or len(filters) == 0:
        # Nothing to filter, return True
        return True
        # Something to filter
    folder_path = Path(folder_path)
    for folder in filters:
        child_path = Path(
            os.path.join(
                os.path.abspath(root), str(folder).removeprefix("/").removesuffix("/")
            )
        )
        if (
                folder_path in child_path.parents
                or child_path in folder_path.parents
                or folder_path == child_path
        ):
            return True
    return False


def wanted_parent_folder(filters, root, folder_path):
    if not filters or not folder_path or not root or len(filters) == 0:
        return True
    folder_path = Path(folder_path)
    for folder in filters:
        child_path = Path(
            os.path.join(
                os.path.abspath(root), folder.removeprefix("/").removesuffix("/")
            )
        )
        if child_path in folder_path.parents or folder_path == child_path:
            return True
    return False


def process_folder(item, destination_path, filters, root):
    if not (item and destination_path and root):
        return None
    new_directory = os.path.join(destination_path, item.name)
    if not wanted_folder(filters=filters, folder_path=new_directory, root=root):
        LOGGER.debug(f"Skipping the unwanted folder {new_directory} ...")
        return None
    os.makedirs(new_directory, exist_ok=True)
    return new_directory


def file_exists(item, local_file):
    if item and local_file and os.path.isfile(local_file):
        local_file_modified_time = int(os.path.getmtime(local_file))
        remote_file_modified_time = int(item.date_modified.timestamp())
        local_file_size = os.path.getsize(local_file)
        remote_file_size = item.size
        if (
                local_file_modified_time == remote_file_modified_time
                and (local_file_size == remote_file_size or (local_file_size == 0 and remote_file_size is None) or (
                local_file_size is None and remote_file_size == 0))
        ):
            LOGGER.debug(f"No changes detected. Skipping the file {local_file} ...")
            return True
        else:
            LOGGER.debug(
                f"Changes detected: local_modified_time is {local_file_modified_time}, "
                + f"remote_modified_time is {remote_file_modified_time}, "
                + f"local_file_size is {local_file_size} and remote_file_size is {remote_file_size}."
            )
    else:
        LOGGER.debug(f"File {local_file} does not exist locally.")
    return False


def download_file(item, local_file):
    if not (item and local_file):
        return False
    LOGGER.info(f"Downloading {local_file} ...")
    try:
        with item.open(stream=True) as response:
            with open(local_file, "wb") as file_out:
                copyfileobj(response.raw, file_out)
        item_modified_time = time.mktime(item.date_modified.timetuple())
        os.utime(local_file, (item_modified_time, item_modified_time))
    except (exceptions.ICloudPyAPIResponseException, FileNotFoundError, Exception) as e:
        LOGGER.error(f"Failed to download {local_file}: {str(e)}")
        return False
    return True


def process_file(item, destination_path, filters, files):
    if not (item and destination_path and files is not None):
        return False
    local_file = os.path.join(destination_path, item.name)
    if not wanted_file(filters=filters, file_path=local_file):
        return False
    files.add(local_file)
    if file_exists(item=item, local_file=local_file):
        return False
    download_file(item=item, local_file=local_file)
    return True


def remove_obsolete(destination_path, files):
    removed_paths = set()
    if not (destination_path and files is not None):
        return removed_paths
    for path in Path(destination_path).rglob("*"):
        local_file = str(path.absolute())
        if local_file not in files:
            LOGGER.info(f"Removing {local_file} ...")
            if path.is_file():
                path.unlink(missing_ok=True)
                removed_paths.add(local_file)
            elif path.is_dir():
                rmtree(local_file)
                removed_paths.add(local_file)
    return removed_paths


def sync_directory(
        drive,
        destination_path,
        items,
        root,
        top=True,
        filters=None,
        remove=False,
        config=None
):
    files = set()
    if drive and destination_path and items and root:
        # for i in items:
        #    sync_items(i, drive, destination_path, filters, root, files)

        concurrent_workers = 10
        if config is not None and "drive" in config.keys() and "workers" in config["drive"].keys():
            concurrent_workers = config["drive"]["workers"]

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            if str(e).startswith("There is no current event loop in thread"):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:  # pragma: no cover
                raise

        __total = len(items)
        for chunk in more_itertools.chunked(items, 100):
            __tasks = []
            for __item in chunk:
                __tasks.append(sync_items(__item, drive, destination_path, filters, root, files, config))
            LOGGER.info(f"Executing {len(__tasks)} tasks in current chunk")
            __looper = gather_with_concurrency(concurrent_workers, __total, __tasks)
            loop.run_until_complete(__looper)
            LOGGER.info(f"Chunk completed, moving to the next")


        # looper = gather_with_concurrency(concurrent_workers, *[sync_items(i, drive, destination_path, filters, root,
        #                                                                   files, config)
        #                                                        for i in items])
        #
        # loop.run_until_complete(looper)

        if top and remove:
            remove_obsolete(destination_path=destination_path, files=files)
    return files


@background
def sync_items(i, drive, destination_path, filters, root, files, config):
    item = drive[i]
    if item.type in ("folder", "app_library"):
        new_folder = process_folder(
            item=item,
            destination_path=destination_path,
            filters=filters["folders"]
            if filters and "folders" in filters
            else None,
            root=root,
        )
        if not new_folder:
            return
        files.add(new_folder)
        files.update(
            sync_directory(
                drive=item,
                destination_path=new_folder,
                items=item.dir(),
                root=root,
                top=False,
                filters=filters,
                config=config
            )
        )
    elif item.type == "file":
        if wanted_parent_folder(
                filters=filters["folders"]
                if filters and "folders" in filters
                else None,
                root=root,
                folder_path=destination_path,
        ):
            process_file(
                item=item,
                destination_path=destination_path,
                filters=filters["file_extensions"]
                if filters and "file_extensions" in filters
                else None,
                files=files,
            )
    return files


def sync_drive(config, drive):
    destination_path = config_parser.prepare_drive_destination(config=config)
    result = sync_directory(
        drive=drive,
        destination_path=destination_path,
        root=destination_path,
        items=drive.dir(),
        top=True,
        filters=config["drive"]["filters"]
        if "drive" in config and "filters" in config["drive"]
        else None,
        remove=config_parser.get_drive_remove_obsolete(config=config),
        config=config,
    )
    LOGGER.info(f"Drive sync completed")
    return result
