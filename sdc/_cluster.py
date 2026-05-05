import os
from draco import start_slurm_cluster

from typing import Any


DEFAULT_SLURM_CONFIG = {
    "processes": 3,
    "cores": 12,
    "memory": "18 GiB",
    "queues_to_try": ['short', 'standard'],
}


def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _normalize_memory(value: str | int) -> str:
    if isinstance(value, int):
        return f"{value} GiB"

    value = value.strip()

    if value.isdigit():
        return f"{value} GiB"

    return value


def _get_memory_env(name: str, default: str | int) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return _normalize_memory(default)
    return _normalize_memory(value)


def _get_list_env(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


def get_slurm_config() -> dict[str, Any]:
    return {
        "processes": _get_int_env(
            "SDC_SLURM_PROCESSES",
            DEFAULT_SLURM_CONFIG["processes"],
        ),
        "cores": _get_int_env(
            "SDC_SLURM_CORES",
            DEFAULT_SLURM_CONFIG["cores"],
        ),
        "memory": _get_memory_env(
            "SDC_SLURM_MEMORY",
            DEFAULT_SLURM_CONFIG["memory"],
        ),
        "queues_to_try": _get_list_env(
            "SDC_SLURM_QUEUES",
            DEFAULT_SLURM_CONFIG["queues_to_try"],
        ),
    }

def start_cluster():
    return start_slurm_cluster(**get_slurm_config())
