from __future__ import annotations
import html, re, unicodedata
from html.parser import HTMLParser
class _Text(HTMLParser):
    def __init__(self): super().__init__(); self.out=[]
    def handle_data(self, data): self.out.append(data)
def canonical_body(value: str) -> str:
    p=_Text(); p.feed(value or ""); text=html.unescape("".join(p.out))
    text=unicodedata.normalize("NFKC", text).replace("\r\n","\n").replace("\r","\n")
    return re.sub(r"[ \t\f\v]+", " ", re.sub(r"\n{3,}", "\n\n", text)).strip()
