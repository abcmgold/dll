import json
from functools import lru_cache
from pathlib import Path

_LOCATIONS_FILE = Path(__file__).with_name("locations.json")


@lru_cache(maxsize=1)
def provinces() -> list[dict]:
    """15 tỉnh mới, mỗi tỉnh 1 toạ độ đại diện (province_id, province_name, merged_from, center_name, lat, lon)."""
    with _LOCATIONS_FILE.open(encoding="utf-8") as f:
        return json.load(f)["provinces"]
