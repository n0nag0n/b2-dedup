"""Persistent GUI configuration (bucket name, DB backup state, etc.)."""
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import b2_dedup

GUI_CONFIG_PATH = b2_dedup._DATA_DIR / "b2_gui_config.json"


def load_gui_config() -> dict:
    if GUI_CONFIG_PATH.exists():
        try:
            with open(GUI_CONFIG_PATH, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_gui_config(config: dict) -> None:
    try:
        with open(GUI_CONFIG_PATH, 'w') as f:
            json.dump(config, f)
    except Exception:
        pass
