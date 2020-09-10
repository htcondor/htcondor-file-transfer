#!/usr/bin/env python3

"""
Utilize HTCondor to transfer / synchronize a directory from a source on an
execute host to a local destination on the submit host.
"""

import abc
import argparse
import contextlib
import enum
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple, Type, TypeVar

import classad
import htcondor

T_JSON = Dict[str, Any]
T_CMD_INFO = List[Mapping[str, Path]]

KB = 2 ** 10
MB = 2 ** 20
GB = 2 ** 30
TB = 2 ** 40

METADATA_FILE_SIZE_LIMIT = 16 * KB

SANDBOX_FILE_NAME = "file-for-transfer"
REQUIREMENTS_FILE_NAME = "requirements.txt"
METADATA_FILE_NAME = "metadata"
LOCAL_MANIFEST_FILE_NAME = "local_manifest.txt"
REMOTE_MANIFEST_FILE_NAME = "remote_manifest.txt"
TRANSFER_MANIFEST_FILE_NAME = "transfer_manifest.txt"
TRANSFER_COMMANDS_FILE_NAME = "transfer_commands.json"
VERIFY_COMMANDS_FILE_NAME = "verify_commands.json"

OUTER_DAG_NAME = "outer.dag"
INNER_DAG_NAME = "inner.dag"
DAG_ARGS = {"force": 1}

THIS_FILE = Path(__file__).resolve()


class TransferError(Exception):
    pass


class InvalidManifestEntry(TransferError):
    pass


class InconsistentManifest(TransferError):
    pass


class TransferAlreadyRunning(TransferError):
    pass


class VerificationFailed(TransferError):
    pass


class NotACondorJob(TransferError):
    pass


class StrEnum(str, enum.Enum):
    def __repr__(self):
        return repr(self.value)

    def __str__(self):
        return self.value


class Commands(StrEnum):
    SYNC = "sync"
    MAKE_REMOTE_FILE_MANIFEST = "make_remote_file_manifest"
    WRITE_INNER_DAG = "write_inner_dag"
    PULL_FILE = "pull_file"
    PUSH_FILE = "push_file"
    GET_REMOTE_METADATA = "get_remote_metadata"
    VERIFY_METADATA = "verify_metadata"
    FINALIZE_TRANSFER_MANIFEST = "finalize_transfer_manifest"


class TransferDirection(StrEnum):
    PULL = "pull"
    PUSH = "push"


DIRECTION_TO_COMMAND = {
    TransferDirection.PULL: Commands.PULL_FILE,
    TransferDirection.PUSH: Commands.PUSH_FILE,
}


def timestamp() -> float:
    return time.time()


def write_requirements_file(working_dir: Path, requirements: str) -> None:
    (working_dir / REQUIREMENTS_FILE_NAME).write_text(requirements)


def read_requirements_file(requirements_file: Optional[Path]) -> Optional[str]:
    if requirements_file is None:
        return None

    return requirements_file.read_text().strip()


RE_SPLIT_CAMEL = re.compile(r".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)")


def camel_to_upper_snake(text: str) -> str:
    return "_".join(m.group(0).upper() for m in RE_SPLIT_CAMEL.finditer(text))


class ManifestEntry(metaclass=abc.ABCMeta):
    def __init__(self, **info):
        expected_keys = set(self.keys)

        given_keys = set(info.keys())

        if given_keys < expected_keys:
            raise InvalidManifestEntry(
                "Info {} for {} is missing keys: {}".format(
                    info, type(self).__name__, expected_keys - given_keys
                )
            )
        if given_keys > expected_keys:
            logging.warning(
                "Info {} for {} has extra keys: {}".format(
                    info, type(self).__name__, given_keys - expected_keys
                )
            )

        self._info = {k: info[k] for k in self.keys}

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented

        return self._info == other._info

    def __repr__(self):
        return "{}({})".format(
            type(self).__name__, ", ".join("{} = {!r}".format(k, v) for k, v in self._info.items())
        )

    def __str__(self):
        return "{} {}".format(self.type, json.dumps(self.to_json()))

    def to_json(self) -> T_JSON:
        return path_values_to_strings(self._info)

    def to_entry(self):
        return "{}\n".format(self)

    def write_entry_to(self, file):
        file.write(self.to_entry())

    @property
    def type(self) -> str:
        return camel_to_upper_snake(type(self).__name__)

    @property
    @abc.abstractmethod
    def keys(self) -> Tuple[str, ...]:
        raise NotImplementedError


class Name(ManifestEntry, metaclass=abc.ABCMeta):
    def __init__(self, **info):
        super().__init__(**info)

        self._info["name"] = Path(self._info["name"])

    @property
    def name(self):
        return self._info["name"]


class Size(ManifestEntry, metaclass=abc.ABCMeta):
    def __init__(self, **info):
        super().__init__(**info)

        self._info["size"] = int(self._info["size"])

    @property
    def size(self):
        return self._info["size"]


class Digest(Name, Size, metaclass=abc.ABCMeta):
    @property
    def digest(self):
        return self._info["digest"]


class Timestamp(ManifestEntry, metaclass=abc.ABCMeta):
    def __init__(self, **info):
        super().__init__(**info)

        self._info["timestamp"] = float(self._info["timestamp"])

    @property
    def timestamp(self):
        return self._info["timestamp"]


class TransferRequest(Name, Size):
    keys = ("name", "size")


class VerifyRequest(Name, Size):
    keys = ("name", "size")


class TransferVerified(Digest, Timestamp):
    keys = ("name", "size", "digest", "timestamp")


class SyncRequest(Timestamp):
    keys = (
        "direction",
        "remote_prefix",
        "files_at_source",
        "files_to_transfer",
        "bytes_to_transfer",
        "files_to_verify",
        "bytes_to_verify",
        "timestamp",
    )

    def __init__(self, **info):
        super().__init__(**info)

        self._info["remote_prefix"] = Path(self._info["remote_prefix"])


class SyncDone(Timestamp):
    keys = ("timestamp",)


class File(Name, Size):
    keys = ("name", "size")


class Metadata(Digest):
    keys = ("name", "size", "digest")


def descendants(cls):
    for c in cls.__subclasses__():
        yield c
        yield from descendants(c)


ENTRY_TYPE_TO_CLASS = {
    camel_to_upper_snake(cls.__name__): cls for cls in descendants(ManifestEntry)
}


def read_manifest(path: Path) -> Iterator[Tuple[ManifestEntry, int]]:
    with path.open(mode="r") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            try:
                yield parse_manifest_entry(line), line_number
            except Exception:
                logging.exception(
                    'Failed to parse manifest entry at {}:{} ("{}")'.format(path, line_number, line)
                )
                raise


def parse_manifest_entry(entry: str) -> ManifestEntry:
    entry = entry.strip()
    type, info = entry.split(maxsplit=1)

    cls = ENTRY_TYPE_TO_CLASS[type]
    info = json.loads(info)

    return cls(**info)


def create_file_manifest(root_path: Path, manifest_path: Path, test_mode: bool = False) -> None:
    logging.info("Generating file listing for %s", root_path)

    with manifest_path.open(mode="w") as f:
        if not root_path.exists():
            return

        for entry in walk(root_path):
            size = entry.stat().st_size

            if test_mode and size > 50 * MB:
                continue

            File(name=entry.path, size=size).write_entry_to(f)


def parse_file_manifest(prefix: Path, file_manifest_path: Path) -> Dict[Path, int]:
    files = {}
    for entry, _ in read_manifest(file_manifest_path):
        entry = check_entry_type(entry, File)

        fname = entry.name
        size = entry.size

        if prefix not in fname.parents:
            logging.error("%s file (%s) does not start with specified prefix", fname)
        if fname == prefix:
            logging.warning("%s file, stripped of prefix (%s), is empty", prefix)
            continue
        files[fname.relative_to(prefix)] = size

    return files


def walk(path):
    for entry in os.scandir(str(path)):
        if entry.is_dir():
            yield from walk(entry.path)
        elif entry.is_file():
            yield entry


def write_metadata_file(path: Path, hasher, size: int) -> None:
    metadata = Metadata(name=path, digest=hasher.hexdigest(), size=size)

    logging.info("File metadata: {}".format(metadata))

    with Path(METADATA_FILE_NAME).open(mode="w") as f:
        metadata.write_entry_to(f)

    logging.info("Wrote metadata file")


def read_metadata_file(path: Path) -> Metadata:
    if path.stat().st_size > METADATA_FILE_SIZE_LIMIT:
        raise InvalidManifestEntry("Metadata file is too large")

    entry, _ = tuple(read_manifest(path))[0]

    return check_entry_type(entry, Metadata)


T = TypeVar("T", bound=ManifestEntry)


def check_entry_type(entry: ManifestEntry, expected_type: Type[T]) -> T:
    if not isinstance(entry, expected_type):
        raise InvalidManifestEntry(
            "Expected a {}, but got a {}".format(expected_type.__name__, type(entry).__name__)
        )

    return entry


def write_json(j: T_JSON, path: Path) -> None:
    with path.open(mode="w") as f:
        json.dump(j, f)


def load_json(path: Path) -> T_JSON:
    with path.open(mode="r") as f:
        return json.load(f)


def make_hasher():
    return hashlib.sha1()


def shared_submit_descriptors(
    executable: Optional[Path] = None,
    unique_id: Optional[str] = None,
    requirements: Optional[str] = None,
) -> Dict[str, str]:
    if executable is None:
        executable = THIS_FILE

    return {
        "executable": executable.as_posix(),
        "keep_claim_idle": "300",
        "request_disk": "1GB",
        "requirements": requirements or "true",
        "My.Is_Transfer_Job": "true",
        "My.WantFlocking": "true",  # special attribute for the CHTC pool, not necessary at other sites
        "My.UniqueID": classad.quote(unique_id) if unique_id else "",
    }


def submit_outer_dag(
    direction: TransferDirection,
    working_dir: Path,
    local_dir: Path,
    remote_dir: Path,
    requirements: Optional[str] = None,
    unique_id: Optional[str] = None,
    test_mode: bool = False,
) -> int:
    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    working_dir = working_dir.resolve()
    local_dir = local_dir.resolve()

    working_dir.mkdir(parents=True, exist_ok=True)
    local_dir.mkdir(parents=True, exist_ok=True)

    outer_dag = make_outer_dag(
        direction=direction,
        local_dir=local_dir,
        remote_dir=remote_dir,
        working_dir=working_dir,
        requirements=requirements,
        unique_id=unique_id,
        test_mode=test_mode,
    )

    outer_dag_file = dags.write_dag(outer_dag, dag_dir=working_dir, dag_file_name=OUTER_DAG_NAME)

    sub = htcondor.Submit.from_dag(str(outer_dag_file), DAG_ARGS)

    with change_dir(working_dir):
        schedd = htcondor.Schedd()
        with schedd.transaction() as txn:
            return sub.queue(txn)


def make_outer_dag(
    direction: TransferDirection,
    local_dir: Path,
    remote_dir: Path,
    working_dir: Path,
    requirements: Optional[str],
    unique_id: Optional[str],
    test_mode: bool,
):
    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    outer_dag = dags.DAG()

    transfer_manifest_path = local_dir / TRANSFER_MANIFEST_FILE_NAME

    if requirements:
        write_requirements_file(working_dir, requirements)

    # copy this script into the working dir for all further use
    executable = working_dir / THIS_FILE.name
    shutil.copy2(str(THIS_FILE), str(executable))

    outer_dag.layer(
        name="make_remote_file_manifest",
        submit_description=htcondor.Submit(
            {
                "output": "make_remote_file_manifest.out",
                "error": "make_remote_file_manifest.err",
                "log": "make_remote_file_manifest.log",
                "arguments": "{} {} {}".format(
                    Commands.MAKE_REMOTE_FILE_MANIFEST,
                    remote_dir,
                    "--test-mode" if test_mode else "",
                ),
                "should_transfer_files": "yes",
                **shared_submit_descriptors(
                    executable=executable, unique_id=unique_id, requirements=requirements,
                ),
            }
        ),
        post=dags.Script(
            executable=executable,
            arguments=[
                Commands.WRITE_INNER_DAG,
                direction,
                remote_dir,
                REMOTE_MANIFEST_FILE_NAME,
                local_dir,
                "--requirements_file={}".format(REQUIREMENTS_FILE_NAME)
                if requirements is not None
                else "",
                "--unique_id={}".format(unique_id) if unique_id is not None else "",
                "--test-mode" if test_mode else "",
            ],
        ),
    ).child_subdag(
        name="inner",
        dag_file=working_dir / INNER_DAG_NAME,
        post=dags.Script(
            executable=executable,
            arguments=[Commands.FINALIZE_TRANSFER_MANIFEST, transfer_manifest_path],
        ),
    )

    logging.info("Outer DAG shape:\n{}".format(outer_dag.describe()))

    return outer_dag


def write_inner_dag(
    direction: TransferDirection,
    remote_prefix: Path,
    remote_manifest: Path,
    local_prefix: Path,
    requirements=None,
    test_mode: bool = False,
    unique_id=None,
):
    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    logging.info(
        "Generating SUBGDAG for transfer of %s->%s", remote_prefix, local_prefix,
    )

    logging.info("Parsing remote file manifest...")

    remote_files = parse_file_manifest(remote_prefix, remote_manifest)

    logging.info("Generating local file manifest...")

    local_manifest_path = Path(LOCAL_MANIFEST_FILE_NAME)
    create_file_manifest(local_prefix, local_manifest_path)
    local_files = parse_file_manifest(local_prefix, local_manifest_path)

    transfer_manifest_path = local_prefix / TRANSFER_MANIFEST_FILE_NAME
    transfer_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    transfer_manifest_path.touch(exist_ok=True)

    # Never transfer the transfer manifest
    local_files.pop(transfer_manifest_path.relative_to(local_prefix), None)

    if direction is TransferDirection.PULL:
        src_files, dest_files = remote_files, local_files
    else:  # This is a PUSH
        src_files, dest_files = local_files, remote_files

    files_to_transfer = {
        fname for fname, size in src_files.items() if size != dest_files.get(fname, -1)
    }

    # TODO: rethink this logic for push vs. pull
    # Check for files that we have already verified, and do not verify them again.
    files_verified = set()
    for entry, _ in read_manifest(transfer_manifest_path):
        if not isinstance(entry, TransferVerified):
            continue

        files_verified.add(entry.name)

    files_to_verify = set()
    for fname in remote_files:
        if fname in files_to_transfer:
            continue

        if fname not in files_verified:
            files_to_verify.add(fname)

    files_to_transfer = sorted(files_to_transfer)
    files_to_verify = sorted(files_to_verify)

    if direction is TransferDirection.PULL:
        ensure_local_dirs_exist(local_prefix, files_to_transfer)

    transfer_cmd_info = make_cmd_info(
        files_to_transfer, remote_prefix, local_prefix, transfer_manifest_path
    )
    verify_cmd_info = make_cmd_info(
        files_to_verify, remote_prefix, local_prefix, transfer_manifest_path
    )

    write_cmd_info(transfer_cmd_info, Path(TRANSFER_COMMANDS_FILE_NAME))
    write_cmd_info(verify_cmd_info, Path(VERIFY_COMMANDS_FILE_NAME))

    dags.write_dag(
        make_inner_dag(
            direction=direction,
            requirements=requirements,
            transfer_cmd_info=transfer_cmd_info,
            verify_cmd_info=verify_cmd_info,
            unique_id=unique_id,
            test_mode=test_mode,
        ),
        dag_dir=Path.cwd(),  # this will be the working dir of the outer DAG
        dag_file_name=INNER_DAG_NAME,
    )

    bytes_to_transfer = sum(src_files[fname] for fname in files_to_transfer)
    bytes_to_verify = sum(src_files[fname] for fname in files_to_verify)

    with transfer_manifest_path.open(mode="a") as f:
        SyncRequest(
            direction=direction,
            remote_prefix=remote_prefix,
            files_at_source=len(src_files),
            files_to_transfer=len(files_to_transfer),
            bytes_to_transfer=bytes_to_transfer,
            files_to_verify=len(files_to_verify),
            bytes_to_verify=bytes_to_verify,
            timestamp=timestamp(),
        ).write_entry_to(f)

        for fname in files_to_transfer:
            TransferRequest(name=fname, size=src_files[fname]).write_entry_to(f)

        for fname in files_to_verify:
            VerifyRequest(name=fname, size=src_files[fname]).write_entry_to(f)


def make_inner_dag(
    direction: TransferDirection,
    requirements: Optional[str],
    transfer_cmd_info: T_CMD_INFO,
    verify_cmd_info: T_CMD_INFO,
    unique_id: Optional[str] = None,
    test_mode: bool = False,
):
    # Only import htcondor.dags submit-side
    import htcondor.dags as dags

    inner_dag = dags.DAG(max_jobs_by_category={"TRANSFER_JOBS": 1} if test_mode else None)

    tof = [METADATA_FILE_NAME]
    tor = {METADATA_FILE_NAME: "$(flattened_name).metadata"}

    pull_tof = [SANDBOX_FILE_NAME]
    pull_tor = {SANDBOX_FILE_NAME: "$(local_file)"}

    shared_descriptors = shared_submit_descriptors(unique_id=unique_id, requirements=requirements)

    inner_dag.layer(
        name=direction,
        submit_description=htcondor.Submit(
            {
                "output": "$(flattened_name).out",
                "error": "$(flattened_name).err",
                "log": "transfer_file.log",
                "arguments": classad.quote(
                    "{} '$(remote_file)'".format(DIRECTION_TO_COMMAND[direction])
                ),
                "should_transfer_files": "yes",
                "transfer_input_files": "$(local_file)"
                if direction is TransferDirection.PUSH
                else "",
                "transfer_output_files": ", ".join(
                    tof + (pull_tof if direction is TransferDirection.PULL else [])
                ),
                "transfer_output_remaps": classad.quote(
                    " ; ".join(
                        "{} = {}".format(k, v)
                        for k, v in {**tor, **(pull_tor if TransferDirection.PULL else {}),}.items()
                    )
                ),
                **shared_descriptors,
            }
        ),
        vars=transfer_cmd_info,
        post=dags.Script(
            executable=THIS_FILE,
            arguments=[
                Commands.VERIFY_METADATA,
                "--cmd-info",
                TRANSFER_COMMANDS_FILE_NAME,
                "--key",
                "$JOB",
            ],
        ),
    )

    inner_dag.layer(
        name="verify",
        submit_description=htcondor.Submit(
            {
                "output": "$(flattened_name).out",
                "error": "$(flattened_name).err",
                "log": "verify_file.log",
                "arguments": classad.quote(
                    "{} '$(remote_file)'".format(Commands.GET_REMOTE_METADATA)
                ),
                "should_transfer_files": "yes",
                "transfer_output_files": ", ".join(tof),
                "transfer_output_remaps": classad.quote(
                    " ; ".join("{} = {}".format(k, v) for k, v in tor.items())
                ),
                **shared_descriptors,
            }
        ),
        vars=verify_cmd_info,
        post=dags.Script(
            executable=THIS_FILE,
            arguments=[
                Commands.VERIFY_METADATA,
                "--cmd-info",
                VERIFY_COMMANDS_FILE_NAME,
                "--key",
                "$JOB",
            ],
        ),
    )

    logging.info("Inner DAG shape:\n{}".format(inner_dag.describe()))

    return inner_dag


@contextlib.contextmanager
def change_dir(dir):
    original = os.getcwd()
    os.chdir(dir)
    yield
    os.chdir(original)


def ensure_local_dirs_exist(prefix: Path, relative_paths: Iterable[Path]) -> None:
    for d in {(prefix / relative_path).parent for relative_path in relative_paths}:
        d.mkdir(exist_ok=True, parents=True)


def make_cmd_info(files, remote_prefix, local_prefix, transfer_manifest_path):
    cmd_info = []

    for fname in files:
        remote_file = remote_prefix / fname
        local_file = local_prefix / fname
        flattened_name = flatten_path(fname)

        info = {
            "remote_file": remote_file,
            "local_file": local_file,
            "local_prefix": local_prefix,
            "flattened_name": flattened_name,
            "transfer_manifest": transfer_manifest_path,
        }
        cmd_info.append(info)

    return cmd_info


def write_cmd_info(cmd_info: T_CMD_INFO, path: Path) -> None:
    write_json(dict(enumerate(map(path_values_to_strings, cmd_info))), path)


def flatten_path(path: Path) -> str:
    return str(path).replace("/", "_SLASH_").replace(" ", "_SPACE_")


def path_values_to_strings(mapping):
    return {k: str(v) if isinstance(v, Path) else v for k, v in mapping.items()}


def pull_file(path: Path) -> None:
    sandbox_path = Path(os.environ["_CONDOR_SCRATCH_DIR"]) / SANDBOX_FILE_NAME

    hash, byte_count = copy_with_hash(src_path=path, dest_path=sandbox_path)

    write_metadata_file(path, hash, byte_count)


def push_file(path: Path) -> None:
    sandbox_path = Path(os.environ["_CONDOR_SCRATCH_DIR"]) / path.name

    hash, byte_count = copy_with_hash(src_path=sandbox_path, dest_path=path)

    write_metadata_file(path, hash, byte_count)


def get_remote_metadata(path: Path) -> None:
    hash, byte_count = hash_file(path)

    write_metadata_file(path, hash, byte_count)


def verify_metadata(
    local_prefix: Path, local_name: Path, metadata_path: Path, transfer_manifest_path: Path,
) -> None:
    entry = read_metadata_file(metadata_path)

    remote_name = entry.name
    remote_digest = entry.digest
    remote_size = entry.size

    logging.info("About to verify contents of %s", local_name)

    local_size = local_name.stat().st_size

    if remote_size != local_size:
        raise VerificationFailed(
            "Local file size ({} bytes) does not match remote file size ({} bytes)".format(
                local_size, remote_size,
            )
        )

    hasher, byte_count = hash_file(local_name)

    local_digest = hasher.hexdigest()
    if remote_digest != local_digest:
        raise VerificationFailed(
            "Local file {} has digest of {}, which does not match remote file {} (digest {})".format(
                local_name, local_digest, remote_name, remote_digest
            )
        )

    logging.info(
        "File verification successful: local file (%s) and remote file (%s) have matching digest (%s)",
        local_name,
        remote_name,
        remote_digest,
    )

    with transfer_manifest_path.open(mode="a") as f:
        TransferVerified(
            name=local_name.relative_to(local_prefix),
            digest=remote_digest,
            size=remote_size,
            timestamp=timestamp(),
        ).write_entry_to(f)

        f.flush()
        os.fsync(f.fileno())

    for path in (
        metadata_path,
        metadata_path.with_suffix(".out"),
        metadata_path.with_suffix(".err"),
    ):
        if path.exists():
            path.unlink()


def copy_with_hash(src_path: Path, dest_path: Path) -> Tuple[Any, int]:
    tmp_path = dest_path.with_suffix(".tmp")
    logging.info("About to copy %s to %s", src_path, tmp_path)

    size = src_path.stat().st_size

    logging.info("There are %.2f MB to copy", size / MB)
    last_log = time.time()

    hasher = make_hasher()

    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    with src_path.open(mode="rb") as src, tmp_path.open(mode="wb") as dest:
        buf = src.read(MB)
        byte_count = len(buf)

        while len(buf) > 0:
            hasher.update(buf)
            dest.write(buf)

            buf = src.read(MB)

            now = time.time()
            if now - last_log > 5:
                logging.info(
                    "Copied %.2f of %.2f MB; %.1f%% done",
                    byte_count / MB,
                    size / MB,
                    (byte_count / size) * 100,
                )
                last_log = now

            byte_count += len(buf)

        logging.info("Copy complete; about to synchronize file to disk")

        dest.flush()
        os.fsync(dest.fileno())

        logging.info("File synchronized to disk")

        logging.info("Copying file metadata from {} to {}".format(src_path, tmp_path))

        # py3.5 compat; copystat did not take Paths yet
        shutil.copystat(str(src_path), str(tmp_path))

        logging.info("Copied file metadata")

    logging.info("Renaming {} to {}".format(tmp_path, dest_path))

    tmp_path.rename(dest_path)

    logging.info("Renamed {} to {}".format(tmp_path, dest_path))

    return hasher, byte_count


def hash_file(path: Path) -> Tuple[Any, int]:
    logging.info("About to hash %s", path)

    size = path.stat().st_size

    logging.info("There are %.2f MB to hash", size / MB)
    last_log = time.time()

    hasher = make_hasher()

    with path.open(mode="rb") as f:
        buf = f.read(MB)
        byte_count = len(buf)

        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(MB)

            now = time.time()
            if now - last_log > 5:
                logging.info(
                    "Hashed %.2f of %.2f MB; %.1f%% done",
                    byte_count / MB,
                    size / MB,
                    (byte_count / size) * 100,
                )
                last_log = now

            byte_count += len(buf)

    return hasher, byte_count


def analyze(transfer_manifest_path: Path) -> None:
    sync_request_start = None
    sync_request = {"files": {}, "transfer_files": set(), "verified_files": {}}
    local_dir = transfer_manifest_path.parent.resolve()
    sync_count = 0
    mismatched_filesizes = {}

    for entry, line_number in read_manifest(transfer_manifest_path):
        if isinstance(entry, SyncRequest):
            sync_count += 1
            # if sync_request_start is not None:
            #    logging.error("Sync request started at line %d but never finished; inconsistent log",
            #        sync_request_start)
            #    sys.exit(4)
            sync_request_start = line_number
            sync_request.update(entry._info)
        elif isinstance(entry, (TransferRequest, VerifyRequest)):
            if sync_request_start is None:
                raise InconsistentManifest(
                    "Transfer request found at line {} before sync started; inconsistent log".format(
                        line_number
                    )
                )

            size = entry.size
            fname = entry.name

            # File was previously verified.
            if sync_request["verified_files"].get(fname, None) == size:
                continue

            sync_request["files"][fname] = size

            if isinstance(entry, TransferRequest):
                sync_request["transfer_files"].add(entry.name)
        elif isinstance(entry, TransferVerified):
            if sync_request_start is None:
                raise InconsistentManifest(
                    "Transfer verification found at line {} before sync started; inconsistent log".format(
                        line_number
                    )
                )

            fname = entry.name
            size = entry.size

            if sync_request["verified_files"].get(fname, None) == size:
                continue

            if fname not in sync_request["files"]:
                raise InconsistentManifest("File {} verified but was not requested.".format(fname))

            if sync_request["files"][fname] != size:
                raise InconsistentManifest(
                    "Verified file size {} of {} is different than anticipated {}".format(
                        size, fname, sync_request["files"][fname]
                    ),
                )

            local_size = (local_dir / fname).stat().st_size
            if local_size != size:
                mismatched_filesizes[fname] = {
                    "expected": size,
                    "got": local_size,
                    "line_number": line_number,
                }
            else:
                mismatched_filesizes.pop(fname, None)

            if fname in sync_request["transfer_files"]:
                sync_request["files_to_transfer"] -= 1
                sync_request["bytes_to_transfer"] -= size
            else:
                sync_request["files_to_verify"] -= 1
                sync_request["bytes_to_verify"] -= size

            del sync_request["files"][fname]

            sync_request["verified_files"][fname] = size
        elif isinstance(entry, SyncDone):
            if sync_request_start is None:
                raise InconsistentManifest(
                    "Transfer request found at line {} before sync started; inconsistent log".format(
                        line_number,
                    )
                )

            if (
                sync_request["files_to_verify"]
                or sync_request["bytes_to_verify"]
                or sync_request["files"]
                or sync_request["files_to_transfer"]
                or sync_request["bytes_to_transfer"]
            ):
                raise InconsistentManifest(
                    "SYNC_DONE but there is work remaining: {}".format(sync_request)
                )

            sync_request_start = None
            sync_request = {"files": {}, "transfer_files": set(), "verified_files": {}}

    if len(mismatched_filesizes) > 0:
        for fname, size in mismatched_filesizes.items():
            logging.error(
                "- Mismatched file size for %s (line %d): expected %d, got %d on disk",
                fname,
                size["line_number"],
                size["expected"],
                size["got"],
            )
        raise InconsistentManifest(
            "Local sizes of {} files did match anticipated sizes.".format(len(mismatched_filesizes))
        )

    if sync_request_start is not None and (
        sync_request["files_to_verify"]
        or sync_request["bytes_to_verify"]
        or sync_request["files"]
        or sync_request["files_to_transfer"]
        or sync_request["bytes_to_transfer"]
    ):
        logging.error("Sync not done! Work remaining.")
        logging.error(
            "- Files to transfer: %s (bytes %d)",
            sync_request["files_to_transfer"],
            sync_request["bytes_to_transfer"],
        )
        logging.error(
            "- Files to verify: %s (bytes %d)",
            sync_request["files_to_verify"],
            sync_request["bytes_to_verify"],
        )
        logging.error("Inconsistent files: {}".format(sync_request["files"]))
        raise InconsistentManifest("There was work remaining!")

    if sync_request_start is not None:
        with transfer_manifest_path.open(mode="a") as f:
            SyncDone(timestamp=timestamp()).write_entry_to(f)
        print("Synchronization done; verification complete.")
    elif sync_count > 0:
        print("All synchronizations done; verification complete")
    else:
        raise InconsistentManifest("No synchronization found in manifest.")


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    sync = subparsers.add_parser(Commands.SYNC)
    sync.add_argument("direction", type=TransferDirection)
    sync.add_argument("local", type=Path)
    sync.add_argument("remote", type=Path)
    default_working_dir = Path.cwd() / "transfer_working_dir"
    sync.add_argument(
        "--working-dir",
        help="Directory to place working HTCondor files.",
        type=Path,
        default=default_working_dir,
    )
    add_requirements_args(sync)
    add_unique_id_arg(sync)
    add_test_mode_arg(sync)

    make_remote_file_manifest = subparsers.add_parser(Commands.MAKE_REMOTE_FILE_MANIFEST)
    make_remote_file_manifest.add_argument("src", type=Path)
    add_test_mode_arg(make_remote_file_manifest)

    write_inner_dag = subparsers.add_parser(Commands.WRITE_INNER_DAG)
    write_inner_dag.add_argument(
        "direction", type=TransferDirection, choices=list(TransferDirection)
    )
    write_inner_dag.add_argument("remote_prefix", type=Path)
    write_inner_dag.add_argument("remote_manifest", type=Path)
    write_inner_dag.add_argument("local_prefix", type=Path)
    add_requirements_args(write_inner_dag)
    add_unique_id_arg(write_inner_dag)
    add_test_mode_arg(write_inner_dag)

    pull_file = subparsers.add_parser(Commands.PULL_FILE)
    pull_file.add_argument("src", type=Path)

    push_file = subparsers.add_parser(Commands.PUSH_FILE)
    push_file.add_argument("dest", type=Path)

    get_remote_metadata = subparsers.add_parser(Commands.GET_REMOTE_METADATA)
    get_remote_metadata.add_argument("src", type=Path)

    verify_metadata = subparsers.add_parser(Commands.VERIFY_METADATA)
    verify_metadata.add_argument("--cmd-info", type=Path)
    verify_metadata.add_argument("--key")

    analyze = subparsers.add_parser(Commands.FINALIZE_TRANSFER_MANIFEST)
    analyze.add_argument("transfer_manifest", type=Path)

    args = parser.parse_args()

    args.cmd = Commands(args.cmd)

    return args


def add_test_mode_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--test-mode",
        help="Testing mode (only transfers small files)",
        default=False,
        action="store_true",
    )


def add_requirements_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--requirements", help="Submit file requirements (e.g. 'UniqueName == \"MyLab0001\"')",
    )
    parser.add_argument(
        "--requirements_file", help="File containing submit file requirements", type=Path,
    )


def add_unique_id_arg(parser: argparse.ArgumentParser):
    parser.add_argument("--unique_id", help="Set UniqueId in submitted jobs")


def main():
    args = parse_args()

    logging.debug(
        "{} called with args:\n\t{}".format(
            sys.argv[0], "\n\t".join("{} = {!r}".format(k, v) for k, v in vars(args).items()),
        )
    )

    if args.cmd is Commands.SYNC:
        check_already_running(args.unique_id)

        cluster_id = submit_outer_dag(
            direction=args.direction,
            working_dir=args.working_dir,
            local_dir=args.local,
            remote_dir=args.remote,
            requirements=read_requirements_file(args.requirements_file) or args.requirements,
            unique_id=args.unique_id,
            test_mode=args.test_mode,
        )

        print("Outer DAG is running in cluster {}".format(cluster_id))
    elif args.cmd is Commands.MAKE_REMOTE_FILE_MANIFEST:
        check_running_as_job()
        create_file_manifest(args.src, Path(REMOTE_MANIFEST_FILE_NAME), test_mode=args.test_mode)
    elif args.cmd is Commands.WRITE_INNER_DAG:
        write_inner_dag(
            direction=args.direction,
            remote_prefix=args.remote_prefix,
            remote_manifest=args.remote_manifest,
            local_prefix=args.local_prefix,
            requirements=read_requirements_file(args.requirements_file) or args.requirements,
            test_mode=args.test_mode,
            unique_id=args.unique_id,
        )
    elif args.cmd is Commands.PULL_FILE:
        check_running_as_job()
        pull_file(path=args.src)
    elif args.cmd is Commands.PUSH_FILE:
        check_running_as_job()
        push_file(path=args.dest)
    elif args.cmd is Commands.GET_REMOTE_METADATA:
        check_running_as_job()
        get_remote_metadata(path=args.src)
    elif args.cmd is Commands.VERIFY_METADATA:
        cmd_info_path = load_json(args.cmd_info)
        # Split the DAG job name (which is passed as fileid) to get the cmd_info key
        info = cmd_info_path[args.key.split(":")[-1]]
        verify_metadata(
            local_prefix=Path(info["local_prefix"]),
            local_name=Path(info["local_file"]),
            metadata_path=Path("{}.metadata".format(info["flattened_name"])),
            transfer_manifest_path=Path(info["transfer_manifest"]),
        )
    elif args.cmd is Commands.FINALIZE_TRANSFER_MANIFEST:
        analyze(args.transfer_manifest)


def check_already_running(unique_id: Optional[str]) -> None:
    if not unique_id:
        return

    schedd = htcondor.Schedd()
    existing_job = schedd.query(
        constraint="UniqueId == {} && JobStatus =!= 4".format(classad.quote(unique_id)),
        attr_list=[],
        limit=1,
    )
    if len(existing_job) > 0:
        raise TransferAlreadyRunning(
            'Jobs already found in queue with UniqueId == "{}"'.format(unique_id,)
        )


def check_running_as_job() -> None:
    if "_CONDOR_JOB_AD" not in os.environ:
        raise NotACondorJob("This step must be run as an HTCondor job.")


if __name__ == "__main__":
    try:
        logging.basicConfig(format="%(asctime)s ~ %(message)s", level=logging.DEBUG)
        main()
    except Exception as e:
        logging.exception("Error: {}".format(e))
        sys.exit(1)
