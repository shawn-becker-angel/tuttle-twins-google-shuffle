class ManifestKey():

    self.s3_manifest_bucket = "nft-tuttle-twins"

    def __init__(self, manifest_folder: str, season: int, episode: int):
        '''
        manifest_folder must be like "season-manifests"
        season must be an int between 1 and 99
        episode must be an int between 1 and 99
        '''
        assert isinstance(manifest_folder, str) and 1 <= len(manifest_folder) <= 32, "'manifest_folder' must be a string with length between 1 and 32"
        assert isinstance(season, int) and 1 <= session <= 99, "'session' must be an int between 1 and 99"
        assert isinstance(episode, int) and 1 <= episode <= 99, "'episode' must be an int between 1 and 99"
        self.manifest_folder = manifest_folder
        self.season  = f"{int(season):02}"
        self.episode = f"{int(episode):02}"

    def get_current_utc_datetime_iso() -> str:
        '''
        return a string like this:
        "2022-04-28T16:12:34.123456"
        '''
        return datetime.now().isoformat()

    def get_current_s3_manifest_file_name(self) -> str:
        '''
        return a string like this:
        "S01_E01-manifest-2022-04-28T16:12:34.123456.jl"
        '''
        return f"S_{self.season}_E{self.episode}-manifest-{get_current_utc_datetime_iso()}.jl"
    
    def get_current_s3_manifest_file_key(self) -> str:
        '''
        return a string like this:
        "tuttle-twins/season-manifests/S01_E01-manifest-2022-04-28T16:12:34.123456.jl"
        '''
        return f"{self.manifest_folder}/{self.get_current_s3_manifest_file_name()}"

    def validate_local_manifest_file(local_manifest_file):
        assert is_instance(local_manifest_file, str) and local_manifest_file.endswith(".jl") and len(local_manifest_file) > 4, "'local_manifest_file' must be a non-zero length filename with a 'jl' extension"

    def s3_upload_manifest_file(self, local_manifest_file: str) -> None:
        validate_local_manifest_file(local_manifest_file)
        s3_manifest_file_key = self.get_current_s3_manifest_file_key()
        s3_upload_file(local_manifest_file, self.s3_manifest_bucket, s3_manifest_file_key)

    def s3_download_manifest_file(self, s3_manifest_bucket, s3_manifest_file_key, local_manifest_file):
        '''
        <s3_manifest_bucket> should be like 
        local_manifest_file should have extension ".jl"
        <utc_datetime_iso> should have format YYYY-MM-DDTHH:MM:SS.ssssss
        <s3_manifest_file_name> should have format Sdd_Edd-manifest-<utc_datetime_iso>.jl
        <s3_manifest_folder> should be like "\w+/season-manifests"
        <s3_manifest_file_key> should have format <manifest_folder>/<s3_manifest_file_name>
        '''
        assert local_manifest_file.endswith(".jl"), "local_manifest_file lacks '.jl' extension"
        s3_manifest_file_key = f"{manifest_folder}/S{season.upper()}_E{episode.upper()}"


        s3_manifest_key = f"{manifest_folder}/S{season.upper()}_E{episode.upper()}"
        s3.upload_file(local_manifest_file, s3_manifest_bucket, s3_manifest_key)

    def s3_download_manifest_file(local_manifest_file):
        s3_download_file(s3_manifest_bucket, s3_manifest_key, local_manifest_filee)

    def s3_find_manifest_files():
        '''
        returns a list of all manifest files for the current season and episode
        '''
        s3_manifest_prefix = get_s3_manifest_prefix()
        s3_manifest_filter = f"{s3_manifest_prefix}"

        all_files = s3_list(s3_manifest_bucket).filter(s3_manifest_prefix)
        