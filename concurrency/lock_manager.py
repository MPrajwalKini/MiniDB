"""
MiniDB Lock Manager
===================
Strict Two-Phase Locking (2PL) with deadlock detection.

Design rules (Phase 7):
  - Table-level granularity (row-level structurally supported, deferred)
  - Lock hierarchy: TABLE > PAGE > ROW (future multi-granularity ready)
  - FIFO wait queue per resource (starvation prevention)
  - Wait-for graph derived dynamically from wait queues (no duplication)
  - Deadlock detection: DFS cycle check on every wait
  - Victim selection: highest txn_id (youngest) — deterministic
  - Upgrade: allowed only if txn is sole SHARED holder, else wait
  - Abort wakes waiting threads immediately via Event
  - Atomic release_all under global mutex

Thread safety: all state guarded by threading.Lock.
"""

import threading
from enum import Enum
from typing import Dict, Set, Optional, List, Tuple
from dataclasses import dataclass, field


class LockType(Enum):
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


class LockResult(Enum):
    GRANTED = "GRANTED"
    TIMEOUT = "TIMEOUT"
    DEADLOCK = "DEADLOCK"
    ABORTED = "ABORTED"


# Lock hierarchy levels (for future multi-granularity)
class LockGranularity(Enum):
    TABLE = 0
    PAGE = 1
    ROW = 2


# ─── Resource key helpers ────────────────────────────────────────────────

def table_resource(table_name: str) -> Tuple[str, str]:
    """Resource key for a table-level lock."""
    return ("table", table_name)


def row_resource(table_name: str, page_id: int, slot_id: int) -> Tuple:
    """Resource key for a row-level lock (future use)."""
    return ("row", table_name, page_id, slot_id)


# ─── Lock request ────────────────────────────────────────────────────────

@dataclass
class LockRequest:
    """A pending or granted lock request."""
    txn_id: int
    lock_type: LockType
    granted: bool = False
    event: threading.Event = field(default_factory=threading.Event)
    aborted: bool = False  # Set True when txn is deadlock victim


# ─── Per-resource lock state ─────────────────────────────────────────────

class _ResourceLock:
    """
    Tracks lock state for a single resource.
    grant_group: set of (txn_id, lock_type) currently holding the lock.
    wait_queue: FIFO list of LockRequest waiting to be granted.
    """
    __slots__ = ('grant_group', 'wait_queue')

    def __init__(self):
        self.grant_group: Dict[int, LockType] = {}  # txn_id → held lock type
        self.wait_queue: List[LockRequest] = []      # FIFO

    def is_compatible(self, lock_type: LockType, requesting_txn: int) -> bool:
        """
        Check if lock_type is compatible with current grant group,
        excluding the requesting txn itself (for upgrade check).
        """
        for txn_id, held_type in self.grant_group.items():
            if txn_id == requesting_txn:
                continue
            # SHARED-SHARED is the only compatible combination
            if held_type == LockType.EXCLUSIVE or lock_type == LockType.EXCLUSIVE:
                return False
        return True

    def has_waiters(self) -> bool:
        return len(self.wait_queue) > 0

    def is_sole_holder(self, txn_id: int) -> bool:
        """True if txn_id is the ONLY holder in grant_group."""
        return list(self.grant_group.keys()) == [txn_id]


# ─── Lock Manager ────────────────────────────────────────────────────────

class LockManager:
    """
    Central lock manager implementing strict 2PL.

    All public methods are thread-safe.
    Locks are released only via release_all() at commit/abort time.
    """

    def __init__(self):
        self._mutex = threading.Lock()  # Global mutex for all state
        self._resources: Dict[tuple, _ResourceLock] = {}
        self._txn_locks: Dict[int, Set[tuple]] = {}  # txn_id → held resources
        self._txn_waiting: Dict[int, tuple] = {}      # txn_id → resource waiting for

    # ─── Public API ──────────────────────────────────────────────────────

    def acquire(self, txn_id: int, resource: tuple,
                lock_type: LockType, timeout: float = 5.0) -> LockResult:
        """
        Acquire a lock. Blocks if incompatible lock is held.

        Returns:
          GRANTED  — lock acquired
          TIMEOUT  — timed out waiting
          DEADLOCK — txn was chosen as deadlock victim
          ABORTED  — txn was aborted while waiting

        Rules:
          - If txn already holds same or stronger lock, return GRANTED (no-op)
          - Upgrade from SHARED→EXCLUSIVE allowed only if sole holder
          - Deadlock detection runs immediately after enqueueing wait
          - FIFO wait queue prevents starvation
        """
        with self._mutex:
            res = self._get_or_create_resource(resource)

            # Already holding this resource?
            if txn_id in res.grant_group:
                held = res.grant_group[txn_id]
                if held == lock_type or held == LockType.EXCLUSIVE:
                    return LockResult.GRANTED  # Already have equal or stronger
                # Upgrade: SHARED → EXCLUSIVE
                if res.is_sole_holder(txn_id):
                    # Safe upgrade — we're the only holder
                    res.grant_group[txn_id] = LockType.EXCLUSIVE
                    return LockResult.GRANTED
                # Not sole holder — must wait for upgrade
                # Fall through to wait logic

            # Check compatibility (considering FIFO: must also check no waiters)
            if not res.has_waiters() and res.is_compatible(lock_type, txn_id):
                # Grant immediately
                res.grant_group[txn_id] = lock_type
                self._txn_locks.setdefault(txn_id, set()).add(resource)
                return LockResult.GRANTED

            # Must wait — enqueue request
            request = LockRequest(txn_id=txn_id, lock_type=lock_type)
            res.wait_queue.append(request)
            self._txn_waiting[txn_id] = resource

            # Deadlock detection (immediate, before blocking)
            if self._detect_deadlock_cycle(txn_id):
                # This txn is part of a cycle — abort youngest in cycle
                victim = self._select_victim(txn_id)
                if victim == txn_id:
                    # We are the victim
                    res.wait_queue.remove(request)
                    self._txn_waiting.pop(txn_id, None)
                    return LockResult.DEADLOCK
                else:
                    # Abort the other txn (wake it up)
                    self._abort_waiting_txn(victim)

        # Block outside of mutex (wait for grant or abort)
        granted = request.event.wait(timeout=timeout)

        with self._mutex:
            self._txn_waiting.pop(txn_id, None)

            if request.aborted:
                # We were aborted while waiting
                return LockResult.ABORTED

            if not granted:
                # Timeout — remove from wait queue
                res = self._resources.get(resource)
                if res and request in res.wait_queue:
                    res.wait_queue.remove(request)
                return LockResult.TIMEOUT

            # Successfully granted (event was set by _try_grant_waiters)
            return LockResult.GRANTED

    def release_all(self, txn_id: int) -> int:
        """
        Release all locks held by a transaction.
        Called at commit/abort time AFTER WAL is durable.
        Returns number of locks released.

        MUST be atomic — holds global mutex during entire operation.
        """
        with self._mutex:
            resources = self._txn_locks.pop(txn_id, set())
            count = 0

            for resource in resources:
                res = self._resources.get(resource)
                if res is None:
                    continue

                # Remove from grant group
                if txn_id in res.grant_group:
                    del res.grant_group[txn_id]
                    count += 1

                # Try to grant waiting requests
                self._try_grant_waiters(res, resource)

                # Clean up empty resource entries
                if not res.grant_group and not res.wait_queue:
                    del self._resources[resource]

            # Also abort any pending waits for this txn
            waiting_resource = self._txn_waiting.pop(txn_id, None)
            if waiting_resource:
                res = self._resources.get(waiting_resource)
                if res:
                    for req in list(res.wait_queue):
                        if req.txn_id == txn_id:
                            req.aborted = True
                            req.event.set()
                            res.wait_queue.remove(req)

            return count

    def abort_waiting(self, txn_id: int) -> None:
        """
        Wake a transaction that is blocked waiting for a lock.
        Used by deadlock detection to notify the victim.
        """
        with self._mutex:
            self._abort_waiting_txn(txn_id)

    # ─── Introspection API ───────────────────────────────────────────────

    def get_locks(self, txn_id: int) -> List[Tuple[tuple, LockType]]:
        """Return list of (resource, lock_type) held by a transaction."""
        with self._mutex:
            result = []
            for resource in self._txn_locks.get(txn_id, set()):
                res = self._resources.get(resource)
                if res and txn_id in res.grant_group:
                    result.append((resource, res.grant_group[txn_id]))
            return result

    def get_waiting(self, txn_id: int) -> Optional[tuple]:
        """Return the resource a transaction is waiting for, or None."""
        with self._mutex:
            return self._txn_waiting.get(txn_id)

    def get_holders(self, resource: tuple) -> Dict[int, LockType]:
        """Return dict of txn_id → lock_type for a resource."""
        with self._mutex:
            res = self._resources.get(resource)
            if res is None:
                return {}
            return dict(res.grant_group)

    def get_wait_queue(self, resource: tuple) -> List[Tuple[int, LockType]]:
        """Return list of (txn_id, lock_type) waiting for a resource."""
        with self._mutex:
            res = self._resources.get(resource)
            if res is None:
                return []
            return [(r.txn_id, r.lock_type) for r in res.wait_queue]

    # ─── Internal ────────────────────────────────────────────────────────

    def _get_or_create_resource(self, resource: tuple) -> _ResourceLock:
        """Get or create resource lock entry. Must hold _mutex."""
        if resource not in self._resources:
            self._resources[resource] = _ResourceLock()
        return self._resources[resource]

    def _try_grant_waiters(self, res: _ResourceLock, resource: tuple) -> None:
        """
        Try to grant requests from the FIFO wait queue.
        Must hold _mutex. Grants in queue order while compatible.
        """
        granted_indices = []
        for i, request in enumerate(res.wait_queue):
            if request.aborted:
                granted_indices.append(i)
                continue

            if res.is_compatible(request.lock_type, request.txn_id):
                # Grant this request
                res.grant_group[request.txn_id] = request.lock_type
                self._txn_locks.setdefault(request.txn_id, set()).add(resource)
                request.granted = True
                request.event.set()  # Wake the waiting thread
                granted_indices.append(i)
            else:
                # FIFO: stop at first incompatible to prevent starvation
                break

        # Remove granted requests from queue (reverse order to preserve indices)
        for i in reversed(granted_indices):
            res.wait_queue.pop(i)

    def _detect_deadlock_cycle(self, start_txn: int) -> bool:
        """
        DFS cycle detection on wait-for graph.
        Graph is derived dynamically from wait queues (no separate state).
        Must hold _mutex.

        Returns True if start_txn is in a cycle.
        """
        visited = set()
        stack = [start_txn]

        while stack:
            txn = stack.pop()
            if txn in visited:
                if txn == start_txn:
                    return True  # Cycle back to start
                continue
            visited.add(txn)

            # Find what this txn is waiting for
            waiting_resource = self._txn_waiting.get(txn)
            if waiting_resource is None:
                continue

            # Find who holds conflicting locks on that resource
            res = self._resources.get(waiting_resource)
            if res is None:
                continue

            for holder_txn in res.grant_group:
                if holder_txn != txn:
                    stack.append(holder_txn)

        return False

    def _select_victim(self, start_txn: int) -> int:
        """
        Select deadlock victim: highest txn_id (youngest) in cycle.
        Must hold _mutex.
        """
        # Collect all txns in the cycle by following the wait-for chain
        cycle_txns = set()
        txn = start_txn
        max_iterations = 100  # Safety limit

        for _ in range(max_iterations):
            if txn in cycle_txns:
                break  # Completed the cycle
            cycle_txns.add(txn)

            waiting_resource = self._txn_waiting.get(txn)
            if waiting_resource is None:
                break
            res = self._resources.get(waiting_resource)
            if res is None:
                break

            # Follow to a holder that is part of the wait chain
            found_next = False
            for holder in res.grant_group:
                if holder != txn and holder in self._txn_waiting:
                    txn = holder
                    found_next = True
                    break
            if not found_next:
                break

        # Victim = highest txn_id in cycle (youngest)
        return max(cycle_txns) if cycle_txns else start_txn

    def _abort_waiting_txn(self, txn_id: int) -> None:
        """
        Abort a txn that is waiting on a lock.
        Sets aborted flag and wakes the thread. Must hold _mutex.
        """
        resource = self._txn_waiting.get(txn_id)
        if resource is None:
            return

        res = self._resources.get(resource)
        if res is None:
            return

        for req in list(res.wait_queue):
            if req.txn_id == txn_id:
                req.aborted = True
                req.event.set()  # Wake the blocked thread
                res.wait_queue.remove(req)
                break

        self._txn_waiting.pop(txn_id, None)
