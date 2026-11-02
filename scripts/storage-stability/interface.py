"""Typed boundary for a source-pinned storage operator; no effects on import.

The Linux backend owns fixed-action host commands and their actual OS/physical
closure evidence. The workflow owns authority, shared lock and dual journals.
A completed JSON alone never establishes an actual command exit or recovery.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol, Mapping
from types import MappingProxyType

SCHEMA_VERSION = 1
INSTALL_ROOT = Path('/usr/local/lib/openlegal-storage-stability')
CONFIG_ROOT = Path('/etc/openlegal-storage-stability')
CHECKPOINT_PHASES = (
    'qualify', 'stop', 'drain_writers', 'stop_postgres',
    'snapshot_same_pool', 'copy_ssd', 'resume_owned_hold',
    'export_snapshots', 'verify_isolated_restore', 'retention',
)
FAILOVER_PHASES = ('fence_old', 'verify_latest', 'promote', 'reseed', 'strict_resume')
BACKEND_PHASES = tuple(p for p in CHECKPOINT_PHASES if p not in ('copy_ssd', 'retention')) + FAILOVER_PHASES
SOURCE_FILES = frozenset({
    'interface.py', 'inventory.py', 'journal.py', 'copy_tree.py',
    'retention.py', 'checkpoint.py', 'linux_backend.py', 'restore_backend.py', 'manual_backend.py', 'provider_backend.py', 'bootstrap_backend.py', 'command_process.py', 'canary_native.py',
    'preservation_backend.py', 'preservation_capture.py',
})
HOLD_PHASES = frozenset({'qualify', 'stop', 'drain_writers', 'resume_owned_hold', 'strict_resume', 'fence_old'})
HOLD_FIELDS = frozenset({'generation', 'owner_operation_id', 'clean', 'recovery_hold', 'accounting_sha256'})
TREE_ROLES = frozenset({'cache', 'corpus', 'postgres', 'config', 'assets', 'trust', 'wal'})

class Refused(RuntimeError):
    """Static error code only; no credential, SQL, URL or payload reflection."""

def need(value: bool, code: str) -> None:
    if not value:
        raise Refused(code)

class PhaseJournal(Protocol):
    def read_phase_bytes(self, phase: str, name: str, pin: str) -> bytes:
        """Read previously ACKed private artifact only after both media revalidate."""
        ...
    def save(self, name: str, value: Any) -> str:
        """O_EXCL canonical+LF JSON on BOTH media, fsync and raw readback. Return SHA."""
        ...
    def save_bytes(self, name: str, value: bytes) -> str:
        """Same durability contract for bounded private technical artifacts."""
        ...

def freeze_authority(value):
    """Freeze a validated detached authority copy; not a path or command input."""
    if type(value)is dict:
        return MappingProxyType({key:freeze_authority(item)for key,item in value.items()})
    if type(value)is list:
        return tuple(freeze_authority(item)for item in value)
    need(value is None or type(value)in(str,int,float,bool),'authority_value_type')
    return value

@dataclass(frozen=True)
class Operation:
    operation_id: str
    action: str                    # checkpoint or manual_failover
    phase: str
    inventory: dict[str, Any]
    inventory_sha256: str
    source_graph_sha256: str
    authority_sha256: str
    journal: PhaseJournal
    chain_root: Path               # new exclusive backup_root/chains/<operation UUID>
    previous: dict[str, dict]      # previously VERIFIED receipts, never a failed phase
    seal: Callable[[], None]       # source/input/authority/lock/journal verifier, not a command hook
    authority: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    # Backend permission checks MUST precede effects; missing/default keys deny.

# Exact receipt envelope returned by EVERY backend method. All artifacts are
# emitted through Operation.journal, with relative basenames and their raw SHA.
RECEIPT_FIELDS = frozenset({
    'schema_version', 'kind', 'operation_id', 'action', 'phase',
    'inventory_sha256', 'source_graph_sha256', 'authority_sha256',
    'status', 'actual_exit_code', 'owned_processes_closed', 'facts', 'artifacts',
})
RECEIPT_KIND = 'openlegal_storage_stability_backend_receipt'

# Minimum true facts are necessary, not sufficient: the backend must prove the
# inventory's exact resource IDs/specs/devices, actual termination and SQL caps.
# "MCP" in stop/drain means application serving/scheduler/collection/document
# workers. PostgreSQL is stopped in the following phase. Historical completed
# Jobs are immutable and never patched; only live/unfinished Jobs are suspended.
REQUIRED_TRUE = {
    'qualify': ('inventory_devices_pv_images_match', 'host_identity_match', 'source_seal_match'),
    'stop': ('serving_desired_zero', 'scheduler_desired_zero', 'all_mcp_jobs_suspended', 'provider_accounting_preserved', 'all_mcp_processes_absent', 'all_mcp_cgroups_empty', 'all_mcp_cri_containers_terminated', 'sql_application_sessions_zero'),
    'drain_writers': ('all_mcp_processes_absent', 'all_mcp_cgroups_empty', 'all_mcp_cri_containers_terminated', 'sql_application_sessions_zero', 'provider_accounting_preserved'),
    'stop_postgres': ('primary_actual_exit_zero', 'standby_actual_exit_zero', 'postgres_processes_absent', 'postgres_cgroups_empty', 'sql_endpoints_unreachable', 'wal_archive_workers_absent'),
    'snapshot_same_pool': ('original_datasets_exact', 'active_datasets_exact', 'one_coordinated_snapshot_group', 'all_writers_still_stopped', 'latest_wal_and_index_sealed'),
    'resume_owned_hold': ('same_images_config_epoch', 'provider_accounting_preserved', 'lookup_ready', 'strict_durability_ready'),
    'export_snapshots': ('all_snapshot_guids_match', 'export_bytes_verified', 'chain_inputs_complete', 'full_self_contained_export'),
    'verify_isolated_restore': ('isolated_no_network', 'original_sources_unmodified', 'all_retained_rows_equal', 'all_blob_hashes_equal', 'wal_and_index_verified', 'restore_processes_closed', 'restore_resources_removed', 'full_export_restore_verified'),
    'fence_old': ('old_primary_physically_fenced', 'all_old_writers_physically_fenced', 'no_automatic_promotion', 'all_mcp_processes_absent', 'all_mcp_cgroups_empty', 'all_mcp_cri_containers_terminated', 'sql_application_sessions_zero', 'provider_accounting_preserved'),
    'verify_latest': ('latest_acknowledged_wal_present', 'latest_index_verified', 'retained_inventory_verified', 'no_old_snapshot_rollback'),
    'promote': ('old_fence_reverified', 'promoted_exact_owned_standby', 'new_epoch_intent_bound', 'application_writers_inhibited', 'no_old_snapshot_rollback'),
    'reseed': ('second_copy_durably_seeded', 'both_pg_identities_verified', 'both_blob_generations_verified', 'new_epoch_bound'),
    'strict_resume': ('old_fence_reverified', 'both_durable_copies_ready', 'new_epoch_bound', 'lookup_ready', 'provider_accounting_preserved'),
}

class LinuxBackend(Protocol):
    """Source-pinned sibling class. Methods accept NO arbitrary commands/hooks.

    All methods return RECEIPT_FIELDS exactly, actual_exit_code typed int0,
    owned_processes_closed true, status passed, identity fields copied exactly
    from Operation, artifacts mapping basename->raw SHA produced by its journal.

    snapshot_same_pool facts additionally contain snapshots (exact original seven plus all active extra datasets in one group; list of exact
    {dataset,guid,snapshot,snapshot_guid}) and latest_wal_lsn/index_sha256.
    qualify/stop/drain/resume facts carry provider_hold_before/after, the EXACT
    HOLD_FIELDS projection, hold_created_by_operation bool, collectors_resumed
    bool, and provider_state_before_artifact/provider_state_after_artifact names.
    The named private artifacts are canonical wrappers with schema_version=1,
    kind=openlegal_checkpoint_provider_observation, operation_id,phase,stage,
    projection,state,admission_ownership,hold_control. state is EXACT schema29
    state(); projection.accounting_sha256 hashes its canonical bytes. Ownership
    is {legacy_locked:bool,slots:[{owner,slot,locked:bool}]} covering every ledger
    row. hold_control is the full provider-control row. Unknown/unlocked evidence
    cannot be cleared. Changed charged accounting is accepted only with a bound
    actual provider_settlement_closure_artifact, typed same-operation full
    process/SQL/runtime-guard closure, same-day monotonic counters and unchanged
    provider/canary policy. STOP must already establish this full closure.
    The named private artifacts contain full raw provider/accounting readbacks;
    they are not substituted for the five-field projection. Qualify establishes
    this operation's fresh baseline; inventory.provider_hold is initial metadata,
    not a standing daily count. Each later before links to the latest verified after; only source-reviewed
    monotonic settlement with actual closure may advance accounting.
    A changed hold additionally carries provider_checkpoint_readback, the actual
    schema29 readback object {hold,release,created_at,released_at}, and its canonical
    journal name provider_checkpoint_readback_artifact. Its SHA is bound as an
    artifact. Only drain_writers (or fence_old in explicit manual_failover) may create a clean owned hold; release is allowed
    only in resume phases, after physical closure, with unchanged accounting and
    explicit authority. A foreign or uncertain hold stays unchanged/collectors0.
    Both install and release mint new generations; old receipt replay is refused.

    Runtime receipts must preserve original whole metadata, exact actor/resource
    UID pins and actual process identity/OS exit+closure; absence of a Pod or
    desired replicas0 alone NEVER proves physical drainage. Command receipts and
    raw finite readbacks must be saved before returning a passed receipt.

    Manual failover is never automatic. If an original journal medium is lost,
    promotion/reseed/resume require a SECOND independent durable journal root on
    replacement media, pinned by a NEW inventory/epoch/authority. No SSD-only
    journal fallback exists. No old snapshot rollback or original deletion.
    """
    def qualify(self, operation: Operation) -> dict: ...
    def stop(self, operation: Operation) -> dict: ...
    def drain_writers(self, operation: Operation) -> dict: ...
    def stop_postgres(self, operation: Operation) -> dict: ...
    def snapshot_same_pool(self, operation: Operation) -> dict: ...
    def resume_owned_hold(self, operation: Operation) -> dict: ...
    def export_snapshots(self, operation: Operation) -> dict: ...
    def verify_isolated_restore(self, operation: Operation) -> dict: ...
    def fence_old(self, operation: Operation) -> dict: ...
    def verify_latest(self, operation: Operation) -> dict: ...
    def promote(self, operation: Operation) -> dict: ...
    def reseed(self, operation: Operation) -> dict: ...
    def strict_resume(self, operation: Operation) -> dict: ...
