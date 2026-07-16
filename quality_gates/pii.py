# quality_gates/pii.py
import re

def mask_pii(text: str) -> str:
    text = re.sub(r"[\w.+-]+@[\w-]+\.[\w.-]+", "[EMAIL]", text)
    text = re.sub(r"\b\d{9,12}\b", "[PHONE]", text)
    return text