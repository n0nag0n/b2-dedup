"""Optional LAN password helpers."""
import os


def vault_password() -> str:
    return (os.environ.get("VAULT_PASSWORD") or "").strip()


def password_required() -> bool:
    return bool(vault_password())
