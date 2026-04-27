from datetime import datetime

from pydantic import BaseModel
from pydantic_mongo import PydanticObjectId

from sarc.client.job import SlurmJob

from .users import UserData


class SlurmJobList(BaseModel):
    jobs: list[SlurmJob]
    page: int
    per_page: int


class UserList(BaseModel):
    users: list[UserData]
    page: int
    per_page: int


class Cursor(BaseModel):
    """Keyset pagination cursor anchored on (submit_time, _id) of the last item
    of the previous page. Ordering matches the index (submit_time DESC, _id ASC).
    """

    after_submit_time: datetime
    after_id: PydanticObjectId


class SlurmJobListByCursor(BaseModel):
    jobs: list[SlurmJob]
    per_page: int
    next_cursor: Cursor | None = None
