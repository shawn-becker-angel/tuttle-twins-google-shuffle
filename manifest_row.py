from typing import TypedDict, Literal
import enum

class ManifestRow(TypedDict):
   src_url: str
   dst_key: str

class ManifestRowAction(enum.Enum):
    COPY_SRC_TO_DST: int = 1 
    REMOVE_DST: int = 2

class ManifestActionRow(TypedDict):
   src_url: str
   dst_url: str
   action: ManifestRowAction



