import enum, json, time, os

from typing import List, Literal
from fnmatch import fnmatch
from uuid import uuid4
from datetime import datetime

from constants.etl import LOCKFILE_FILENAME


class EnumManifestStatus(str, enum.Enum):
    Pending = "pending"
    IntegrityCheckFailed = "integrity_check_failed"
    Active = "active"
    Completed = "completed"
    Failed = "failed"

class EnumPhase(str, enum.Enum):
    Ingress = "ingress"
    Transform = "transform"
    Egress = "egress"

class ManifestModel:
    def __init__(
        self,
        filename,
        path,
        remote_files=None,
        transfers=None,
        local_files=None,
        jobs=None,
        status: EnumManifestStatus=EnumManifestStatus.Pending,
        phase: EnumPhase=EnumPhase.Ingress,
        logs=None,
        created_at=None,
        last_modified=None,
        metadata=None
    ):
        self.filename = filename
        self.path = path
        self.status = status
        self.phase = phase
        self.remote_files = []
        self.transfers = []
        self.local_files = []
        self.jobs = []
        self.logs = logs if logs != None else []
        self.created_at = created_at
        self.last_modified = last_modified
        self.metadata = metadata if metadata != None else {}

        local_files = local_files if local_files != None else []
        for local_file in local_files:
            if type(local_file) == dict:
                self.local_files.append(local_file)
                continue

            self.local_files.append(local_file.__dict__)

        remote_files = remote_files if remote_files != None else []
        for remote_file in remote_files:
            if type(remote_file) == dict:
                self.remote_files.append(remote_file)
                continue

            self.remote_files.append(remote_file.__dict__)

        transfers = transfers if transfers != None else []
        for transfer in transfers:
            if type(transfer) == dict:
                self.transfers.append(transfer)
                continue

            self.transfers.append(transfer.__dict__)

        jobs = jobs if jobs != None else []
        for job in jobs:
            if type(job) == dict:
                self.jobs.append(job)
                continue

            self.jobs.append(job.__dict__)

    def _to_json(self):
        return json.dumps(
            {
                "status": self.status,
                "phase": self.phase,
                "local_files": self.local_files,
                "transfers": self.transfers,
                "remote_files": self.remote_files,
                "jobs": self.jobs,
                "logs": self.logs,
                "created_at": self.created_at,
                "last_modified": self.last_modified,
                "metadata": self.metadata
            },
            indent=4
        )
    
    def add_metadata(self, metadata):
        self.metadata = {
            **self.metadata,
            **metadata
        }
    
    def set_status(self, status):
        self.status = status
        self.log(f"Status change: {status}")

    def set_phase(self, phase: EnumPhase, status=EnumManifestStatus.Pending):
        self.phase = phase
        self.log(f"Phase change: {phase}")
        self.status = status
        self.log(f"Status change: {status}")

    def log(self, message, prefix_datetime=True):
        if prefix_datetime:
            message = f"{str(datetime.now())} {message}"
        self.logs.append(message)

    def create(self, system_id, client):
        self.created_at = time.time()
        self.last_modified = self.created_at
        self.log("Created")
        self._persist(system_id, client)

    def save(self, system_id, client):
        self.last_modified = time.time()
        self._persist(system_id, client)
        

    def _persist(self, system_id, client):
        client.files.insert(
            systemId=system_id,
            path=self.path,
            file=self._to_json()
        )



def get_tapis_file_contents_json(client, system_id, path):
    return client.files.getContents(systemId=system_id, path=path)

def get_manifest_files(client, system_id, path):
    """Fetches the manifest files while ignoring the etl lockfile"""
    files = client.files.listFiles(systemId=system_id, path=path)
    return [file for file in files if file.name != LOCKFILE_FILENAME]

def match_patterns(target, include_patterns, exclude_patterns):
    print("TARGET", target, "   ")
    inclusions = [] if len(include_patterns) > 0 else [True]
    for include_pattern in include_patterns:
        inclusion = fnmatch(target, include_pattern)
        inclusions.append(inclusion)
        if inclusion: break

    exclusions = []
    for exclude_pattern in exclude_patterns:
        exclusion = fnmatch(target, exclude_pattern)
        exclusions.append(exclusion)
        if exclusion: break

    return any(inclusions) and not any(exclusions)

class DataIntegrityProfile:
    def __init__(
        self,
        type_,
        done_files_path=None,
        include_patterns=[],
        exclude_patterns=[]
    ):
        self.type = type_
        self.done_files_path = done_files_path
        self.include_patterns = include_patterns
        self.exclude_patterns = exclude_patterns

        if self.type == "checksum" or self.type == "byte_check":
            pass
        elif self.type == "done_file" and self.done_files_path == None:
            raise TypeError(f"Missing required properties for data integrity profile with type {self.type} | Values required: [done_files_path]")

class DataIntegrityValidator:
    def __init__(self, client):
        self.client = client
        self.errors = []

    def _done_file_validation(self, files_in_manifest, system_id, profile):
        # Fetch the done files
        all_files = self.client.files.listFiles(
            systemId=system_id,
            path=profile.done_files_path
        )

        done_files = []
        for file in all_files:
            if match_patterns(file.name, profile.include_patterns, profile.exclude_patterns):
                done_files.append(file)

        for file_in_manifest in files_in_manifest:
            validated = False
            for done_file in done_files:
                if file_in_manifest.get("name") in done_file.name:
                    validated = True
                    break

            if not validated:
                self.errors.append(f"Failed to find done file for file '{file_in_manifest.get('name')}'")
        
        # Not really sure if this check is necessary. Perhaps just return false
        return len(self.errors) < 1

    def _byte_check_validation(self, files_in_manifest, system_id):
        validations = []
        for file_in_manifest in files_in_manifest:
            file_on_system = self.client.flies.listFiles(
                systemId=system_id,
                path=file_in_manifest.path
            )[0]

            # Validation fails if the size of any given file in the manifest
            # is not the same as the size of the file at that path on the
            # system
            if file_on_system.size != file_in_manifest.size:
                validations.append(False)
                self.errors.append(f"Byte check failed for file {file_in_manifest}")
                break

            validations.append(True)

        return all(validations)

    def _checksum_validation(sefl, files_in_manifest, system_id):
        raise NotImplementedError("Data integrity profile type 'checksum' is not implemented")

    def validate(
        self,
        files_in_manifest: List[object],
        system_id: str,
        profile: DataIntegrityProfile
    ):
        validated = False
        try:
            if profile.type == "done_file":
                validated = self._done_file_validation(files_in_manifest, system_id, profile)
            elif profile.type == "checksum":
                validated = self._checksum_validation(files_in_manifest, system_id)
            elif profile.type == "byte_check":
                validated = self._byte_check_validation(files_in_manifest, system_id, profile)
        except Exception as e:
            self.errors.append(str(e))
        
        # Covert array of errors to a sting
        err = ""
        error_count = len(self.errors)
        if error_count > 0:
            i = 1
            for error in self.errors:
                err = err + f"{i}. {error} "
                i+=1

        return (
            validated,
            err if error_count > 0 else None
        )
    
class ManifestsLock:
    def __init__(self, client, system):
        self._client = client
        self._system = system
        self._locked = True

    def acquire(self, max_wait_sec=300):
        self._await_lockfile(max_wait_sec=max_wait_sec)
        self._create_lockfile()

    def release(self):
        self._delete_lockfile()

    def _await_lockfile(self, max_wait_sec):
        start_time = time.time()
        while self._locked:
            # Check if the total wait time was exceeded. If so, throw exception
            if time.time() - start_time >= max_wait_sec:
                raise Exception(f"Max Wait Time Reached: {max_wait_sec}")
        
            # Fetch all manifest files
            files = self._client.files.listFiles(
                systemId=self._system.get("manifests").get("system_id"),
                path=self._system.get("manifests").get("path")
            )

            self._locked = LOCKFILE_FILENAME in [file.name for file in files]
            if not self._locked:
                break
                
            time.sleep(5)

    def _create_lockfile(self):
        try:
            # Create the lockfile
            self._client.files.insert(
                systemId=self._system.get("manifests").get("system_id"),
                path=os.path.join(self._system.get("manifests").get("path"), LOCKFILE_FILENAME),
                file=b""
            )
        except Exception as e:
            raise Exception(f"Failed to create lockfile: {e}")
        
    def _delete_lockfile(self):
        # Delete the lock file
        try:
            self._client.files.delete(
                systemId=self._system.get("manifests").get("system_id"),
                path=os.path.join(self._system.get("manifests").get("path"), LOCKFILE_FILENAME)
            )
        except Exception as e:
            raise Exception(f"Failed to delete lockfile: {e}")
        
def poll_transfer_task(client, task, interval_sec=5):
    while task.status not in ["COMPLETED", "FAILED", "FAILED_OPT", "CANCELLED"]:
        time.sleep(interval_sec)
        task = client.files.getTransferTask(
            transferTaskId=task.uuid
        )
    return task
    
    
def generate_manifests(system, client, phase: EnumPhase):
    # Fetch manifest files
    try:
        manifest_files = fetch_system_files(
            system_id=system.get("manifests").get("system_id"),
            path=system.get("manifests").get("path"),
            client=client,
            include_patterns=system.get("manifests").get("include_patterns"),
            exclude_patterns=[
                *system.get("manifests").get("exclude_patterns"),
                LOCKFILE_FILENAME # Ignore the lockfile.
            ]
        )
    except Exception as e:
        raise Exception(f"Failed to fetch manifest files: {e}")

    try:
        # Get all of the contents of each manifest file and create an instance
        # of a manifest model
        manifests = []
        for manifest_file in manifest_files:
            manifests.append(
                ManifestModel(
                    filename=manifest_file.get("name"),
                    path=manifest_file.get("path"),
                    **json.loads(
                        get_tapis_file_contents_json(
                            client,
                            system.get("manifests").get("system_id"),
                            manifest_file.get("path")
                        )
                    )
                )
            )
    except Exception as e:
        raise Exception(f"Critical: Failed to create manifest model for manifest file at path {manifest_file.get('path')} | {e}")

    # Fetch the data files
    try:
        data_files = fetch_system_files(
            system_id=system.get("data").get("system_id"),
            path=system.get("data").get("path"),
            client=client,
            include_patterns=system.get("data").get("include_patterns"),
            exclude_patterns=system.get("data").get("exclude_patterns")
        )
    except Exception as e:
        raise Exception(f"Failed to fetch data files: {e}")
    
    # Create a list of all data files that have already been registered in a
    # manifest
    registered_data_file_paths = []
    files_property = "local_files" if phase == EnumPhase.Ingress else "remote_files"
    for manifest in manifests:
        for manifest_data_file in getattr(manifest, files_property):
            registered_data_file_paths.append(manifest_data_file["path"])

    # Create a list all data files that have not yet been registered with a
    # manifest
    unregistered_data_files = [
        data_file for data_file in data_files
        if data_file.path not in registered_data_file_paths
    ]

    # Check the manifest generation policy to determine whether all new
    # data files should be added to a single manifest, or a manifest
    # should be generated for each new data file
    # TODO consider querying for the file(s) sizes 2 times in a row at some interval and if
    # the size is different, keep polling until the last 2 sizes(for the same file(s)) are the same
    new_manifests = []
    manifest_generation_policy = system.get("manifests").get("generation_policy")
    if manifest_generation_policy == "auto_one_per_file":
        for unregistered_data_file in unregistered_data_files:
            manifest_filename = f"{str(uuid4())}.json"
            new_manifests.append(
                ManifestModel(
                    filename=manifest_filename,
                    phase=phase,
                    path=os.path.join(
                        system.get("manifests").get("path"),
                        manifest_filename
                    ),
                    files=[unregistered_data_file]
                )
            )
    elif (
        manifest_generation_policy == "auto_one_for_all"
        and len(unregistered_data_files) > 0
    ):
        manifest_filename = f"{str(uuid4())}.json" 
        new_manifests.append(
            ManifestModel(
                filename=manifest_filename,
                path=os.path.join(
                    system.get("manifests").get("path"),
                    manifest_filename
                ),
                files=unregistered_data_files
            )
        )

    try:
        # Persist all of the new manifests
        for new_manifest in new_manifests:
            new_manifest.create(system.get("manifests").get("path"), client)
    except Exception as e:
        raise Exception(f"Failed to create manifests: {e}")

    return new_manifests

def requires_manifest_generation(system):
    return system.get("manifests").get("generation_policy") in [
        "auto_one_for_all",
        "auto_one_per_file"
    ]

def fetch_system_files(
    system_id,
    path,
    client,
    include_patterns=[],
    exclude_patterns=[]
):
    try:
        # Fetch the all files
        unfiltered_files = client.files.listFiles(
            systemId=system_id,
            path=path
        )

        print("UNFILTERED", unfiltered_files, "   ")

        print("INCLUDE", include_patterns, "   ")

        print("EXCLUDE", include_patterns, "   ")


        if len(include_patterns) == 0 and len(exclude_patterns) == 0:
            return unfiltered_files
        
        filtered_files = []
        for file in unfiltered_files:
            if match_patterns(file.name, include_patterns, exclude_patterns):
                filtered_files.append(file)

        return filtered_files
    except Exception as e:
        raise Exception(f"Failed to fetch data files: {str(e)}")

def cleanup(ctx, stdout_message=""):
    # Calling stdout calls clean up hooks that were regsitered in the
    # beginning of the script
    ctx.stdout(stdout_message)

def validate_manifest_data_files(system, files_in_manifest, client):
    # This step ensures that the file(s) in the manifest are ready for the current
    # operation (data processing or transfer) to be performed against them.
    validated = True
    if system.get("data").get("integrity_profile") == None:
        return validated
    
    try: 
        data_integrity_profile = DataIntegrityProfile(
            **system.get("data").get("integrity_profile")
        )
    except Exception as e:
        raise Exception(f"Failed to create DataIntegrityProfile | {e}")

    # Check the integrity of each data file in the manifests based on the data
    # integrity profile
    data_integrity_validator = DataIntegrityValidator(client)
    return data_integrity_validator.validate(
        files_in_manifest,
        system.get("data").get("system_id"),
        data_integrity_profile
    )
