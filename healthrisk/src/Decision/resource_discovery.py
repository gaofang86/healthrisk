import json
import numpy as np
from pathlib import Path
from sentence_transformers import SentenceTransformer
import faiss

RESOURCES_DIR = Path(__file__).parent.parent.parent / "data" / "resources"

class ResourceDiscovery:

    def __init__(self, resources_dir=RESOURCES_DIR):
        # 加载所有区域文件
        self.resources = []
        for f in resources_dir.glob("*.json"):
            with open(f) as fp:
                self.resources.extend(json.load(fp))

        self.encoder = SentenceTransformer("all-MiniLM-L6-v2")

        texts = [r["tags"] for r in self.resources]
        embeddings = self.encoder.encode(texts, show_progress_bar=False)
        embeddings = np.array(embeddings).astype("float32")
        faiss.normalize_L2(embeddings)

        self.index = faiss.IndexFlatIP(embeddings.shape[1])
        self.index.add(embeddings)

    def get(self, country: str, risk_tier: str, disease: str = "dengue", k: int = 8) -> dict:
        query     = f"{disease} {country.lower()} {risk_tier.lower()} clinic hotline food ngo"
        query_vec = self.encoder.encode([query], show_progress_bar=False)
        query_vec = np.array(query_vec).astype("float32")
        faiss.normalize_L2(query_vec)

        _, indices = self.index.search(query_vec, k=20)
        retrieved = [self.resources[i] for i in indices[0]]

        region_map = {
            "INDONESIA": ["INDONESIA", "GLOBAL"],
            "PERU":      ["PERU", "SOUTH_AMERICA", "GLOBAL"],
            "BRAZIL":    ["BRAZIL", "SOUTH_AMERICA", "GLOBAL"],
        }
        allowed = region_map.get(country.upper(), ["GLOBAL"])
        retrieved = [r for r in retrieved if r["country"].upper() in allowed][:k]
        
        result = {
            "clinics":      [],
            "hotlines":     [],
            "ngos":         [],
            "food":         [],
            "programs":     [],
            "vaccines":     [],
            "surveillance": [],
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
                "⚠ Outbreak risk is elevated in your district. "
                "Seek care at first symptoms — do not wait."
            )

        return {"resources": result, "urgency_note": urgency_note}