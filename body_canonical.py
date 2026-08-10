from __future__ import annotations
import html, re, unicodedata
from html.parser import HTMLParser
BLOCK_TAGS={"p","div","li","h1","h2","h3","h4","h5","h6","br","tr"}
class _Text(HTMLParser):
    def __init__(self): super().__init__(); self.out=[]
    def handle_starttag(self, tag, attrs):
        if tag in BLOCK_TAGS: self.out.append("\n")
    def handle_endtag(self, tag):
        if tag in BLOCK_TAGS: self.out.append("\n")
    def handle_data(self, data): self.out.append(data)
def canonical_body(value: str) -> str:
    p=_Text(); p.feed(value or ""); text=html.unescape("".join(p.out))
    text=unicodedata.normalize("NFKC", text).replace("\r\n","\n").replace("\r","\n")
    return re.sub(r"[ \t\f\v]+", " ", re.sub(r"\n+", "\n", text)).strip()
