import json
from pathlib import Path

RESOURCES_DIR = Path(__file__).parent.parent.parent / "data" / "resources"

class ResourceDiscovery:

    def __init__(self, resources_dir=RESOURCES_DIR):
        self.resources = []
        for f in sorted(resources_dir.glob("*.json")):
            with open(f) as fp:
                self.resources.extend(json.load(fp))

    def get(self, country: str, risk_tier: str, disease: str = "dengue", k: int = 6) -> dict:
        region_map = {
            "INDONESIA": ["INDONESIA", "GLOBAL"],
            "PERU":      ["PERU", "SOUTH_AMERICA", "GLOBAL"],
            "BRAZIL":    ["BRAZIL", "SOUTH_AMERICA", "GLOBAL"],
        }
        allowed   = region_map.get(country.upper(), ["GLOBAL"])
        retrieved = [r for r in self.resources
                     if r["country"].upper() in allowed][:k]

        result = {
            "clinics": [], "hotlines": [], "ngos": [],
            "food": [], "programs": [], "vaccines": [], "surveillance": []
        }
        type_map = {
            "clinic":      "clinics",
            "hotline":     "hotlines",
            "ngo":         "ngos",
            "food":        "food",
            "program":     "programs",
            "vaccine":     "vaccines",
            "surveillance":"surveillance",
        }
        for r in retrieved:
            key = type_map.get(r["type"])
            if key:
                result[key].append(f"{r['name']} — {r['contact']}")

        urgency_note = ""
        if risk_tier in ("Critical", "High"):
            urgency_note = (
                "⚠ Outbreak risk is elevated. "
                "Seek care at first symptoms — do not wait."
            )

        return {"resources": result, "urgency_note": urgency_note}