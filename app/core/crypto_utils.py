"""Payload encryption utilities for the Orchestrator.

Uses Fernet (AES-128-CBC + HMAC-SHA256) from the ``cryptography`` library.

The shared secret key is read from:
  - PAYLOAD_ENCRYPTION_KEY env var   (preferred, no prefix)
  - ENCRYPTION_KEY env var           (fallback)

Generate a key once and put it in BOTH .env files:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_fernet = None


def _get_fernet():
    """Lazily initialize the Fernet cipher."""
    global _fernet
    if _fernet is not None:
        return _fernet

    from cryptography.fernet import Fernet

    key = (
        os.environ.get("PAYLOAD_ENCRYPTION_KEY", "")
        or os.environ.get("ENCRYPTION_KEY", "")
        or os.environ.get("WORKER_ENCRYPTION_KEY", "")  # compat with worker .env
    ).strip()

    if not key:
        raise RuntimeError(
            "Encryption key not set. Add PAYLOAD_ENCRYPTION_KEY to your .env file.\n"
            "Generate a key with:\n"
            "  python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\""
        )

    _fernet = Fernet(key.encode())
    logger.debug("Fernet cipher initialized.")
    return _fernet


def reset_fernet() -> None:
    """Reset cached cipher (useful for testing or key rotation)."""
    global _fernet
    _fernet = None


def is_encryption_enabled() -> bool:
    """Return True if an encryption key is configured."""
    key = (
        os.environ.get("PAYLOAD_ENCRYPTION_KEY", "")
        or os.environ.get("ENCRYPTION_KEY", "")
        or os.environ.get("WORKER_ENCRYPTION_KEY", "")
    ).strip()
    return bool(key)


# ── Image encryption ──────────────────────────────────────────────────────────


def encrypt_image_bytes(image_bytes: bytes) -> str:
    """Encrypt raw image bytes → encrypted base64 string for JSON transport.

    Args:
        image_bytes: Raw fingerprint image data (e.g. TIFF bytes).

    Returns:
        URL-safe base64 string containing the encrypted ciphertext.
    """
    fernet = _get_fernet()
    encrypted = fernet.encrypt(image_bytes)
    result = encrypted.decode("utf-8")
    logger.debug(
        "encrypt_image_bytes: %d bytes → %d chars", len(image_bytes), len(result)
    )
    return result


def decrypt_image_bytes(encrypted_b64: str) -> bytes:
    """Decrypt an encrypted base64 string → raw image bytes.

    Args:
        encrypted_b64: The string produced by :func:`encrypt_image_bytes`.

    Returns:
        Original raw image bytes.

    Raises:
        cryptography.fernet.InvalidToken: If the token is tampered or the key is wrong.
    """
    fernet = _get_fernet()
    result = fernet.decrypt(encrypted_b64.encode("utf-8"))
    logger.debug(
        "decrypt_image_bytes: %d chars → %d bytes", len(encrypted_b64), len(result)
    )
    return result


# ── Generic field encryption ──────────────────────────────────────────────────


def encrypt_field(text: str) -> str:
    """Encrypt a plain-text string (e.g. a JSON-serialised embedding vector).

    Args:
        text: Plain text to encrypt.

    Returns:
        Encrypted string safe for JSON transport.
    """
    return _get_fernet().encrypt(text.encode("utf-8")).decode("utf-8")


def decrypt_field(encrypted_text: str) -> str:
    """Decrypt a field encrypted by :func:`encrypt_field`.

    Args:
        encrypted_text: Encrypted string.

    Returns:
        Original plain text.
    """
    return _get_fernet().decrypt(encrypted_text.encode("utf-8")).decode("utf-8")
