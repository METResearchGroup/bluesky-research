from pydantic import BaseModel


class EnqueueServicePayload(BaseModel):
    record_type: str
    integrations: list[str]
    start_date: str
    end_date: str
