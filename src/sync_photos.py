import time
import os
import shutil

import more_itertools
from icloudpy import exceptions
import asyncio


from src import config_parser, LOGGER
from src.tools import gather_with_concurrency, background


def generate_file_name(photo, file_size, destination_path):
    full_path = os.path.join(destination_path, str(photo.added_date.year))
    os.makedirs(full_path, exist_ok=True)
    full_path = os.path.join(full_path, str(photo.added_date.month))
    os.makedirs(full_path, exist_ok=True)
    filename = photo.filename
    if file_size != "original":
        tokens = photo.filename.rsplit(".", 1)
        tokens.insert(len(tokens) - 1, file_size)
        filename = "__".join(tokens[:-1]) + "." + tokens[-1]
    else:
        tokens = photo.filename.rsplit(".", 1)
        tokens.insert(len(tokens) - 1, file_size)
        original_filename = "__".join(tokens[:-1]) + "." + tokens[-1]
        original_file_path = os.path.join(full_path, original_filename)
        if os.path.isfile(original_file_path):
            os.rename(original_file_path, os.path.join(full_path, filename))
    return os.path.abspath(os.path.join(full_path, filename))


def photo_exists(photo, file_size, local_path):
    if photo and local_path and os.path.isfile(local_path):
        local_size = os.path.getsize(local_path)
        remote_size = int(photo.versions[file_size]["size"])
        LOGGER.info(
            f"(Exists: {local_size == remote_size}) Local mTime -> {os.path.getmtime(local_path)}\nLocalSize -> {os.path.getsize(local_path)}\nRemoteSize ->{remote_size}\nRemoteDate -> {time.mktime(photo.added_date.timetuple())}")

        if local_size == remote_size:
            LOGGER.debug(f"No changes detected. Skipping the file {local_path} ...")
            return True
        else:
            LOGGER.debug(
                f"Change detected: local_file_size is {local_size} and remote_file_size is {remote_size}."
            )
        return False


def download_photo(photo, file_size, destination_path):
    if not (photo and file_size and destination_path):
        return False
    LOGGER.info(f"Downloading {destination_path} ...")
    try:
        download = photo.download(file_size)
        with open(destination_path, "wb") as file_out:
            shutil.copyfileobj(download.raw, file_out)
        LOGGER.info(f"Setting modified date to {photo.added_date}")
        local_modified_time = time.mktime(photo.added_date.timetuple())
        os.utime(destination_path, (local_modified_time, local_modified_time))
        LOGGER.info(
            f"{destination_path}: Remote date -> {photo.added_date} Converted Date -> {local_modified_time} Local mTime -> {os.path.getmtime(destination_path)}")
    except (exceptions.ICloudPyAPIResponseException, FileNotFoundError, Exception) as e:
        LOGGER.error(f"Failed to download {destination_path}: {str(e)}")
        return False
    return True


@background
def process_photos(photo, file_sizes, destination_path):
    for file_size in file_sizes:
        process_photo(photo, file_size, destination_path)
    return True


def process_photo(photo, file_size, destination_path):
    photo_path = generate_file_name(
        photo=photo, file_size=file_size, destination_path=destination_path
    )
    if file_size not in photo.versions:
        LOGGER.warning(
            f"File size {file_size} not found on server. Skipping the photo {photo_path} ..."
        )
        return False
    if photo_exists(photo, file_size, photo_path):
        return False
    download_photo(photo, file_size, photo_path)
    return True


def sync_album(album, destination_path, file_sizes, config):
    concurrent_workers = 10
    if config is not None and "photos" in config.keys() and "workers" in config["photos"].keys():
        concurrent_workers = config["photos"]["workers"]

    if not (album and destination_path and file_sizes):
        return None
    os.makedirs(destination_path, exist_ok=True)
    LOGGER.info(f"Starting parallel download with {concurrent_workers} workers")

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:  # pragma: no cover
        if str(e).startswith("There is no current event loop in thread"):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        else:  # pragma: no cover
            raise
    __total = len(album)
    for chunk in more_itertools.chunked(album, 100):
        __tasks = []
        for __photo in chunk:
            __tasks.append(process_photos(__photo, file_sizes, destination_path))
        LOGGER.info(f"Executing {len(__tasks)} tasks in current chunk")
        __looper = gather_with_concurrency(concurrent_workers, __total, __tasks)
        loop.run_until_complete(__looper)
        LOGGER.info(f"Chunk completed, moving to the next")

    # tasks = [process_photos(photo, file_sizes, destination_path) for photo in album]


def sync_photos(config, photos):
    destination_path = config_parser.prepare_photos_destination(config=config)
    filters = config_parser.get_photos_filters(config=config)
    if filters["albums"]:
        for album in iter(filters["albums"]):
            sync_album(
                album=photos.albums[album],
                destination_path=os.path.join(destination_path, album),
                file_sizes=filters["file_sizes"],
                config=config,
            )
    else:
        sync_album(
            album=photos.all,
            destination_path=os.path.join(destination_path, ""),
            file_sizes=filters["file_sizes"],
            config=config,
        )
    LOGGER.info(f"Photo sync completed")

# def enable_debug():
#     import contextlib
#     import http.client
#     import logging
#     import requests
#     import warnings

#     # from pprint import pprint
#     # from icloudpy import ICloudPyService
#     from urllib3.exceptions import InsecureRequestWarning

#     # Handle certificate warnings by ignoring them
#     old_merge_environment_settings = requests.Session.merge_environment_settings

#     @contextlib.contextmanager
#     def no_ssl_verification():
#         opened_adapters = set()

#         def merge_environment_settings(self, url, proxies, stream, verify, cert):
#             # Verification happens only once per connection so we need to close
#             # all the opened adapters once we're done. Otherwise, the effects of
#             # verify=False persist beyond the end of this context manager.
#             opened_adapters.add(self.get_adapter(url))

#             settings = old_merge_environment_settings(
#                 self, url, proxies, stream, verify, cert
#             )
#             settings["verify"] = False

#             return settings

#         requests.Session.merge_environment_settings = merge_environment_settings

#         try:
#             with warnings.catch_warnings():
#                 warnings.simplefilter("ignore", InsecureRequestWarning)
#                 yield
#         finally:
#             requests.Session.merge_environment_settings = old_merge_environment_settings

#             for adapter in opened_adapters:
#                 try:
#                     adapter.close()
#                 except Exception as e:
#                     pass

#     # Monkeypatch the http client for full debugging output
#     httpclient_logger = logging.getLogger("http.client")

#     def httpclient_logging_patch(level=logging.DEBUG):
#         """Enable HTTPConnection debug logging to the logging framework"""

#         def httpclient_log(*args):
#             httpclient_logger.log(level, " ".join(args))

#         # mask the print() built-in in the http.client module to use
#         # logging instead
#         http.client.print = httpclient_log
#         # enable debugging
#         http.client.HTTPConnection.debuglevel = 1

#     # Enable general debug logging
#     logging.basicConfig(filename="log1.txt", encoding="utf-8", level=logging.DEBUG)

#     httpclient_logging_patch()


# if __name__ == "__main__":
#     # enable_debug()
#     sync_photos()
