from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass(frozen=True)
class DataFingerprint:
    path: str
    size_bytes: int
    head_sha256: str
    tail_sha256: str

    def to_dict(self) -> Dict[str, object]:
        return {
            "path": self.path,
            "size_bytes": self.size_bytes,
            "head_sha256": self.head_sha256,
            "tail_sha256": self.tail_sha256,
        }


def _sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def fingerprint_file(path: Path, block_bytes: int = 4096) -> DataFingerprint:
    """
    Fast fingerprint: sha256(first block) + sha256(last block) + file size.

    This is intentionally not a full-file hash (too slow for large snaps).
    """
    p = path.resolve()
    st = p.stat()
    size = int(st.st_size)

    with p.open("rb") as f:
        head = f.read(block_bytes)
        if size > block_bytes:
            f.seek(max(0, size - block_bytes))
        tail = f.read(block_bytes)

    return DataFingerprint(
        path=str(p),
        size_bytes=size,
        head_sha256=_sha256_bytes(head),
        tail_sha256=_sha256_bytes(tail),
    )
