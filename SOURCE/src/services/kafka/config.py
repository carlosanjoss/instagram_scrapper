import os
from typing import Any, Dict


def kafka_connection_options(bootstrap_servers: str) -> Dict[str, Any]:
    """Build shared kafka-python connection options from environment variables."""
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
    options: Dict[str, Any] = {
        "bootstrap_servers": [bootstrap_servers],
        "security_protocol": security_protocol,
    }

    if security_protocol.startswith("SASL_"):
        mechanism = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip().upper()
        username = os.getenv("KAFKA_SASL_USERNAME")
        password = os.getenv("KAFKA_SASL_PASSWORD")
        if not username or not password:
            raise ValueError(
                "KAFKA_SASL_USERNAME e KAFKA_SASL_PASSWORD sao obrigatorios "
                f"quando KAFKA_SECURITY_PROTOCOL={security_protocol}"
            )
        options.update(
            {
                "sasl_mechanism": mechanism,
                "sasl_plain_username": username,
                "sasl_plain_password": password,
            }
        )

    return options
