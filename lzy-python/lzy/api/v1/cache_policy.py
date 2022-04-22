from enum import Enum


class CachePolicy(Enum):
    SAVE = "SAVE"  # Save cached ops, but not restore them
    RESTORE = "RESTORE"  # Get cached ops, but not save new ones
    SAVE_AND_RESTORE = "SAVE_AND_RESTORE"
    IGNORE = "IGNORE"

    def save(self) -> bool:
        return self == CachePolicy.SAVE or self == CachePolicy.SAVE_AND_RESTORE

    def restore(self) -> bool:
        return self == CachePolicy.RESTORE or self == CachePolicy.SAVE_AND_RESTORE

    def from_last_snapshot(self) -> bool:
        return self != CachePolicy.IGNORE
