import enum, json, time, os, re

from uuid import uuid4

from tapipy.tapis import Tapis


class EnumManifestStatus(str, enum.Enum):
    Pending = "pending"
    Active = "active"
    Completed = "completed"
    Failed = "failed"

class EnumPhase(str, enum.Enum):
    Inbound = "inbound"
    Outbound = "outbound"

class ManifestModel:
    def __init__(
        self,
        filename,
        path,
        files=[],
        status: EnumManifestStatus=EnumManifestStatus.Pending,
        created_at = None,
        last_modified = None
    ):
        self.filename = filename
        self.path = path
        self.files = []
        for file in files:
            if type(file) == dict:
                self.files.append(file)
                continue

            self.files.append(file.__dict__)
        self.status = status
        self.created_at = created_at
        self.last_modified = last_modified

    def create(self, system_id, client):
        self.created_at = time.time()
        self.last_modified = self.created_at
        # Upload the contents of the manifest file to the tapis system
        client.files.insert(
            systemId=system_id,
            path=self.path,
            file=json.dumps({
                "status": self.status,
                "files": self.files,
                "created_at": self.created_at,
                "last_modified": self.last_modified
            })
        )

    def update(self, system_id, client):
        self.last_modified = time.time()
        client.files.insert(
            systemId=system_id,
            path=self.path,
            file=json.dumps({
                "status": self.status,
                "files": self.files,
                "created_at": self.created_at,
                "last_modified": self.last_modified
            })
        )

def get_tapis_file_contents_json(client, system_id, path):
    return client.files.getContents(systemId=system_id, path=path)

def get_client(ctx):
    username = ctx.get_input("TAPIS_USERNAME")
    password = ctx.get_input("TAPIS_PASSWORD")
    jwt = ctx.get_input("TAPIS_JWT")

    if (username == None or password == None) and jwt == None:
        ctx.stderr(1, "Unable to authenticate with tapis: Must provide either a username with a password or a JWT")

    kwargs = {
        "username": username,
        "password": password,
        "jwt": jwt
    }

    try:
        t = Tapis(
            base_url=ctx.get_input("TAPIS_BASE_URL"),
            **kwargs
        )
        
        if username and password and not jwt:
            t.get_tokens()
    except Exception as e:
        ctx.stderr(1, f"Failed to authenticate: {e}")

def match_patterns(target, include_pattern, exclude_pattern):
    print(target, include_pattern, exclude_pattern)
    matches_include = True
    if include_pattern != None:
        print("MATCH PATTERNS INCLUDE", include_pattern, type(include_pattern))
        matches_include = re.match(include_pattern, target) != None

    matches_exclude = True
    if exclude_pattern != None:
        print("MATCH PATTERNS EXCLUDE", exclude_pattern, type(exclude_pattern))
        matches_exclude = re.match(exclude_pattern, target) != None

    return matches_include and not matches_exclude

class DataIntegrityProfile:
    def __init__(
        self,
        type_,
        done_files_path=None,
        include_pattern=None,
        exclude_pattern=None
    ):
        self.type = type_
        self.done_files_path = done_files_path
        self.include_pattern = include_pattern
        self.exclude_pattern = exclude_pattern

        if self.type == "checksum" or self.type == "byte_check":
            pass
        elif self.type == "done_file" and (
            self.done_files_path == None or self.pattern == None
        ):
            raise TypeError(f"Missing required properties for data integrity profile with type {self.type} | Values required: [done_files_path, pattern]")

class DataIntegrityValidator:
    def __init__(self, client):
        self.client = client
        self.errors = []

    def _done_file_validation(self, manifest, system_id, profile):
        # Fetch the done files
        done_files = self.client.files.listFiles(
            system_id=system_id,
            path=profile.done_files_path
        )
        filtered_done_files = []
        for done_file in done_files:
            print("CHECKING DONE_FILE", done_file)
            # Add files to the done files list if it complies with the include
            # and exclude patterns
            if match_patterns(
                done_file.name, 
                profile.include_pattern, profile.exclude_pattern
            ):
                filtered_done_files.append(done_file)
        validated_files = []
        for file_in_manifest in manifest.files:
            validated = False
            for done_file in done_files:
                if file_in_manifest.name in done_file.name:
                    validated = True
                    validated_files.append(file_in_manifest)
                    break

            if not validated:
                self.errors.append(f"Failed to find done file for file '{file_in_manifest.name}'")
                break

        return len(validated_files) == len(done_files)

    def _byte_check_validation(self, manifest, system_id):
        validations = []
        for file_in_manifest in manifest.files:
            file_on_system = self.client.flies.listFiles(
                system_id=system_id,
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

    def _checksum_validation(sefl, manifest, system_id):
        raise NotImplementedError("Data integrity profile type 'checksum' is not implemented")

    def validate(
        self,
        manifest: ManifestModel,
        system_id: str,
        profile: DataIntegrityProfile
    ):
        validated = False
        try:
            if profile.type == "done_file":
                validated = self._done_file_validation(manifest, system_id)
            elif profile.type == "checksum":
                validated = self._checksum_validation(manifest, system_id)
            elif profile.type == "byte_check":
                validated = self._byte_check_validation(manifest, system_id, profile)
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

def delete_lockfile(client, system_id, manifests_path, lockfile_filename):
    # Delete the lock file
    try:
        client.files.delete(
            systemId=system_id,
            path=os.path.join(manifests_path, lockfile_filename),
        )
    except Exception as e:
        raise Exception(f"Failed to delete lockfile: {e}")
    
def generate_new_manfifests(
    system_id,
    data_path,
    include_pattern,
    exclude_pattern,
    manifests_path,
    manifest_generation_policy,
    manifests,
    client
):
    try:
        # Fetch the all data files
        unfiltered_data_files = client.files.listFiles(
            systemId=system_id,
            path=data_path
        )
    except Exception as e:
        raise Exception(f"Failed to fetch data files: {str(e)}")
    
    try:
        data_files = unfiltered_data_files
        if include_pattern != None or exclude_pattern != None:
            data_files = [
                data_file for data_file in unfiltered_data_files
                if match_patterns(data_file.name, include_pattern, exclude_pattern)
            ]
    except Exception as e:
        raise Exception(f"Error while filtering data files using provided include and exclude patterns: {str(e)}")

    # Create a list of all registered files
    registered_data_file_paths = []
    for manifest in manifests:
        for manifest_data_file in manifest.files:
            registered_data_file_paths.append(manifest_data_file["path"])

    # Find all data files that have not yet been registered with a manifest
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
    if manifest_generation_policy == "auto_one_per_file":
        for unregistered_data_file in unregistered_data_files:
            manifest_filename = f"{str(uuid4())}.json"
            new_manifests.append(
                ManifestModel(
                    filename=manifest_filename,
                    path=os.path.join(manifests_path, manifest_filename),
                    files=[unregistered_data_file]
                )
            )
    elif manifest_generation_policy == "auto_one_for_all":
        manifest_filename = f"{str(uuid4())}.json" 
        new_manifests.append(
            ManifestModel(
                filename=manifest_filename,
                path=os.path.join(manifests_path, manifest_filename),
                files=unregistered_data_files
            )
        )

    try:
        # Persist all of the new manifests
        for new_manifest in new_manifests:
            new_manifest.create(system_id, client)
    except Exception as e:
        raise Exception(f"Failed to create manifests: {e}")

    return new_manifests