from pydantic import BaseModel

class History(BaseModel):
    query: str
    result: str
