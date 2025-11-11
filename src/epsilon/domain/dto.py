from pydantic import BaseModel

class RawHTML(BaseModel):
    url: str
    content: str