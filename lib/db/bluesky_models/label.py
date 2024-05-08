"""Bluesky model for labels.

Based on https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/com/atproto/label/defs.py
"""  # noqa
from typing import Optional, Union
import typing_extensions as te

from pydantic import BaseModel, Field


class LabelModel(BaseModel):
    cts: str = Field(..., description="Timestamp at which this label was created.")  # noqa
    src: str = Field(..., description="The DID of the user who created this label.")  # noqa
    uri: str = Field(..., description="AT URI of the record, repository (account), or other resource that this label applies to.")  # noqa
    val: str = Field(..., max_length=128, description="The short string name of the value or type of this label.")  # noqa
    cid: Optional[str] = Field(default=None, description="The CID of the record that this label applies to.")  # noqa
    exp: Optional[str] = Field(default=None, description="The timestamp at which this label expires.")  # noqa
    neg: Optional[bool] = Field(default=None, description="Whether this label is negated.")  # noqa
    sig: Optional[Union[str, bytes]] = Field(default=None, description="The signature of the label.")  # noqa
    ver: Optional[int] = Field(default=None, description="The version of the label.")  # noqa
    py_type: te.Literal['com.atproto.label.defs#label'] = Field(
        default='com.atproto.label.defs#label', alias='$type', frozen=True
    )
