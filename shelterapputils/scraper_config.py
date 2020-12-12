from typing import List


class ScraperConfig:
    def __init__(self,
                 source: str,
                 data_url: str,
                 data_format: str,
                 extract_usecols: List[str],
                 drop_duplicates_columns: List[str],
                 rename_columns: dict,
                 service_summary: str,
                 check_collection: str,
                 dump_collection: str,
                 dupe_collection: str,
                 data_source_collection_name: str,
                 collection_dupe_field: str):
        self._source: str = source
        self._data_url: str = data_url
        self._data_format: str = data_format
        self._extract_usecols: List[str] = extract_usecols
        self._drop_duplicates_columns: List[str] = drop_duplicates_columns
        self._rename_columns: dict = rename_columns
        self._service_summary: str = service_summary
        self._check_collection: str = check_collection
        self._dump_collection: str = dump_collection
        self._dupe_collection: str = dupe_collection
        self._data_source_collection_name: str = data_source_collection_name
        self._collection_dupe_field: str = collection_dupe_field

    @property
    def source(self) -> str:
        return self._source

    @property
    def data_url(self) -> str:
        return self._data_url

    @property
    def data_format(self) -> str:
        return self._data_format

    @property
    def extract_usecols(self) -> List[str]:
        return self._extract_usecols

    @property
    def drop_duplicates_columns(self) -> List[str]:
        return self._drop_duplicates_columns

    @property
    def rename_columns(self) -> dict:
        return self._rename_columns

    @property
    def service_summary(self) -> str:
        return self._service_summary

    @property
    def check_collection(self) -> str:
        return self._check_collection

    @property
    def dump_collection(self) -> str:
        return self._dump_collection

    @property
    def dupe_collection(self) -> str:
        return self._dupe_collection

    @property
    def data_source_collection_name(self) -> str:
        return self._data_source_collection_name

    @property
    def collection_dupe_field(self) -> str:
        return self._collection_dupe_field
