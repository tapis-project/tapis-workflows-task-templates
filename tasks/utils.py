import enum, json, time, os


class EnumManifestStatus(str, enum.Enum):
    Pending = "pending"
    Active = "active"
    Completed = "completed"
    Failed = "failed"

class EnumETLPhase(str, enum.Enum):
    DataProcessing = "data_processing_prep"
    Transfer = "transfer_prep"

class ETLManifestModel:
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