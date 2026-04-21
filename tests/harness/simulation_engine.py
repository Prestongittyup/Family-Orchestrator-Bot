"""
Simulation Engine - Models concurrent family member interactions

Simulates realistic household scenarios with:
  - Multiple family members
  - Concurrent plan/task/event creation
  - Conflicting updates on same resources
  - Idempotency key tracking
  - Artificial delays and response reordering
"""

import asyncio
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
import copy
import hashlib
import json

from apps.api.xai.causal_mapper import CausalContext, CausalMapper
from apps.api.xai.schema import (
    EntityType as XAIEntityType,
    ExplanationSchema,
    InitiatedBy,
)


class CommandType(Enum):
    """HPAL command types that family members can issue"""
    CREATE_PLAN = "create_plan"
    UPDATE_PLAN = "update_plan"
    CREATE_TASK = "create_task"
    UPDATE_TASK = "update_task"
    RECOMPUTE_PLAN = "recompute_plan"
    MARK_TASK_COMPLETE = "mark_task_complete"
    CREATE_EVENT = "create_event"
    CANCEL_EVENT = "cancel_event"


class PersonRole(Enum):
    """Family member roles"""
    PARENT = "parent"
    TEENAGER = "teenager"
    CHILD = "child"
    CAREGIVER = "caregiver"


@dataclass
class SimulatedCommand:
    """A command issued by a family member"""
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    command_type: CommandType = CommandType.CREATE_PLAN
    issued_by: str = ""  # person_id
    issued_at: datetime = field(default_factory=datetime.utcnow)
    target_entity_id: Optional[str] = None  # plan_id, task_id, etc.
    target_entity_type: str = ""  # "plan", "task", "event"
    payload: Dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))
    execution_count: int = 0
    last_execution_time: Optional[datetime] = None
    succeeded: bool = False
    error: Optional[str] = None
    delay_ms: int = 0  # Artificial delay before execution
    should_fail: bool = False  # For failure injection


@dataclass
class SimulatedEntity:
    """Base class for simulated plans, tasks, events"""
    entity_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    entity_type: str = ""  # "plan", "task", "event"
    family_id: str = ""
    created_by: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    version: int = 1
    watermark_epoch: int = 0
    deleted: bool = False
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SimulationTrace:
    """Execution trace of a command"""
    command_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    phase: str = ""  # "queued", "executing", "committed", "failed", "retried"
    error: Optional[str] = None
    state_mutations: List[Dict[str, Any]] = field(default_factory=list)


class FamilyMember:
    """Simulates a family member with independent behavior"""
    
    def __init__(self, person_id: str, name: str, role: PersonRole, family_id: str):
        self.person_id = person_id
        self.name = name
        self.role = role
        self.family_id = family_id
        self.issued_commands: List[SimulatedCommand] = []
        self.pending_commands: List[SimulatedCommand] = []
        self.recent_entity_contexts: Dict[str, Dict[str, Any]] = {}  # entity_id -> context
    
    def issue_command(
        self,
        command_type: CommandType,
        target_entity_id: Optional[str] = None,
        target_entity_type: str = "",
        payload: Optional[Dict[str, Any]] = None,
        delay_ms: int = 0,
    ) -> SimulatedCommand:
        """Issue a new command"""
        cmd = SimulatedCommand(
            command_type=command_type,
            issued_by=self.person_id,
            target_entity_id=target_entity_id,
            target_entity_type=target_entity_type,
            payload=payload or {},
            delay_ms=delay_ms,
        )
        self.issued_commands.append(cmd)
        self.pending_commands.append(cmd)
        return cmd
    
    def retry_command(self, command: SimulatedCommand) -> SimulatedCommand:
        """Retry a failed command with same idempotency key"""
        cmd = copy.copy(command)
        cmd.command_id = str(uuid.uuid4())  # New execution ID
        cmd.execution_count += 1
        self.pending_commands.append(cmd)
        return cmd


class HouseholdSimulationState:
    """Mutable state of the simulated household"""
    
    def __init__(self, family_id: str):
        self.family_id = family_id
        self.entities: Dict[str, SimulatedEntity] = {}  # entity_id -> entity
        self.command_history: List[SimulatedCommand] = []
        self.traces: Dict[str, SimulationTrace] = {}  # command_id -> trace
        self.idempotency_cache: Dict[str, Any] = {}  # idempotency_key -> result
        self.task_execution_count: Dict[str, int] = defaultdict(int)  # task_id -> count
        self.concurrent_commands: Set[str] = set()  # In-flight command IDs
        self.state_mutations: List[Dict[str, Any]] = []  # Audit trail
        self.xai_explanations: List[ExplanationSchema] = []  # one per successful mutation
        self.xai_contexts: List[CausalContext] = []  # parallel causal contexts
        self.watermark_epoch: int = 0
        self.created_at = datetime.utcnow()
        self.quarantine_mode: bool = False  # If True, system is in fail-safe mode
        self.quarantine_reason: Optional[str] = None
    
    def add_entity(self, entity: SimulatedEntity) -> None:
        """Add entity to state"""
        self.entities[entity.entity_id] = copy.deepcopy(entity)
        self.state_mutations.append({
            "timestamp": datetime.utcnow().isoformat(),
            "type": "entity_created",
            "entity_id": entity.entity_id,
            "entity_type": entity.entity_type,
        })
    
    def update_entity(self, entity_id: str, updates: Dict[str, Any]) -> None:
        """Update entity attributes"""
        if entity_id not in self.entities:
            raise ValueError(f"Entity {entity_id} not found")
        
        entity = self.entities[entity_id]
        entity.attributes.update(updates)
        entity.updated_at = datetime.utcnow()
        entity.version += 1
        
        self.state_mutations.append({
            "timestamp": datetime.utcnow().isoformat(),
            "type": "entity_updated",
            "entity_id": entity_id,
            "entity_type": entity.entity_type,
            "updates": updates,
            "new_version": entity.version,
        })
    
    def get_entity(self, entity_id: str) -> Optional[SimulatedEntity]:
        """Get entity by ID"""
        return self.entities.get(entity_id)
    
    def get_task_execution_count(self, task_id: str) -> int:
        """Get number of times a task was executed"""
        return self.task_execution_count.get(task_id, 0)
    
    def increment_task_execution(self, task_id: str) -> None:
        """Track task execution (for duplicate detection)"""
        self.task_execution_count[task_id] += 1
    
    def record_idempotent_result(self, idempotency_key: str, result: Any) -> None:
        """Cache result for idempotency"""
        self.idempotency_cache[idempotency_key] = copy.deepcopy(result)
    
    def get_idempotent_result(self, idempotency_key: str) -> Optional[Any]:
        """Check if idempotent operation already executed"""
        return self.idempotency_cache.get(idempotency_key)
    
    def state_hash(self) -> str:
        """Compute deterministic hash of state for convergence verification"""
        state_dict = {
            "family_id": self.family_id,
            "entity_count": len(self.entities),
            "entities": sorted([
                {
                    "id": eid,
                    "type": e.entity_type,
                    "version": e.version,
                    "attrs_hash": hashlib.md5(
                        json.dumps(e.attributes, sort_keys=True, default=str).encode()
                    ).hexdigest(),
                }
                for eid, e in self.entities.items()
                if not e.deleted
            ], key=lambda x: x["id"]),
            "watermark_epoch": self.watermark_epoch,
            "task_execution_counts": dict(self.task_execution_count),
            "quarantine_mode": self.quarantine_mode,
        }
        state_json = json.dumps(state_dict, sort_keys=True, default=str)
        return hashlib.sha256(state_json.encode()).hexdigest()


class SimulationEngine:
    """Core simulation orchestrator"""
    
    def __init__(self, family_id: str, random_seed: int = 42):
        self.family_id = family_id
        self.random_seed = random_seed
        random.seed(random_seed)
        
        self.state = HouseholdSimulationState(family_id)
        self.family_members: Dict[str, FamilyMember] = {}
        self.event_log: List[Dict[str, Any]] = []
        self.command_queue: asyncio.Queue = None
        self._xai_mapper = CausalMapper()
        self.execution_stats = {
            "total_commands": 0,
            "successful_commands": 0,
            "failed_commands": 0,
            "retried_commands": 0,
            "duplicate_detections": 0,
        }
    
    def add_family_member(
        self,
        person_id: str,
        name: str,
        role: PersonRole,
    ) -> FamilyMember:
        """Add a family member to simulation"""
        member = FamilyMember(person_id, name, role, self.family_id)
        self.family_members[person_id] = member
        
        self._log_event({
            "type": "member_registered",
            "person_id": person_id,
            "name": name,
            "role": role.value,
        })
        
        return member
    
    def _log_event(self, event: Dict[str, Any]) -> None:
        """Log simulation event"""
        event["timestamp"] = datetime.utcnow().isoformat()
        self.event_log.append(event)

    # ------------------------------------------------------------------
    # XAI integration: command_type + entity_type routing tables
    # ------------------------------------------------------------------

    _CMD_TYPE_MAP: Dict = {
        CommandType.CREATE_PLAN:       "create_or_merge_plan",
        CommandType.UPDATE_PLAN:       "update_plan",
        CommandType.RECOMPUTE_PLAN:    "recompute_plan",
        CommandType.CREATE_TASK:       "create_task",
        CommandType.UPDATE_TASK:       "update_task",
        CommandType.MARK_TASK_COMPLETE: "update_task",
        CommandType.CREATE_EVENT:      "create_event",
        CommandType.CANCEL_EVENT:      "update_event",
    }

    _ENTITY_TYPE_MAP: Dict = {
        "plan":  XAIEntityType.PLAN,
        "task":  XAIEntityType.TASK,
        "event": XAIEntityType.EVENT,
    }

    def _generate_explanation(
        self,
        command: "SimulatedCommand",
        entity_id: str,
        entity_type: str,
        entity_name: str,
        **extra,
    ) -> None:
        """
        Generate and record a deterministic explanation for one successful
        state mutation.  Called exactly once per non-cached execution —
        idempotency is enforced upstream (cache hits skip _execute_command_logic
        entirely, so this is never invoked twice for the same idempotency_key).
        """
        xai_cmd_type   = self._CMD_TYPE_MAP.get(command.command_type, "update_plan")
        xai_entity_type = self._ENTITY_TYPE_MAP.get(entity_type, XAIEntityType.PLAN)
        task_status    = extra.pop("task_status", None)

        ctx = CausalContext(
            command_type=xai_cmd_type,
            idempotency_key=command.idempotency_key,
            family_id=self.family_id,
            initiated_by=InitiatedBy.USER,
            entity_type=xai_entity_type,
            entity_id=entity_id,
            entity_name=entity_name,
            task_status=task_status,
            **extra,
        )
        explanation = self._xai_mapper.map(ctx)
        self.state.xai_explanations.append(explanation)
        self.state.xai_contexts.append(ctx)

        self._log_event({
            "type": "explanation_generated",
            "explanation_id": explanation.explanation_id,
            "reason_code": explanation.reason_code.value,
            "entity_id": entity_id,
            "idempotency_key": command.idempotency_key,
        })

    async def execute_command(
        self,
        command: SimulatedCommand,
        failure_injector: Optional['FailureInjector'] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Execute a command with optional failure injection
        
        Returns: (success, error_message)
        """
        self.execution_stats["total_commands"] += 1
        
        # Check idempotency cache
        cached_result = self.state.get_idempotent_result(command.idempotency_key)
        if cached_result is not None:
            self.execution_stats["duplicate_detections"] += 1
            self._log_event({
                "type": "duplicate_execution_blocked",
                "command_id": command.command_id,
                "idempotency_key": command.idempotency_key,
                "original_result": cached_result,
            })
            return True, None  # Return cached result as success
        
        # Apply artificial delay
        if command.delay_ms > 0:
            await asyncio.sleep(command.delay_ms / 1000.0)
        
        # Failure injection
        if failure_injector:
            should_fail, failure_reason = await failure_injector.should_inject_failure(
                command,
                self.state,
            )
            if should_fail:
                command.error = failure_reason
                self.execution_stats["failed_commands"] += 1
                self._log_event({
                    "type": "command_failed",
                    "command_id": command.command_id,
                    "reason": failure_reason,
                    "injection": True,
                })
                return False, failure_reason
        
        # Simulate command execution
        try:
            result = await self._execute_command_logic(command)
            
            # Cache result for idempotency
            self.state.record_idempotent_result(command.idempotency_key, result)
            
            command.succeeded = True
            command.last_execution_time = datetime.utcnow()
            self.execution_stats["successful_commands"] += 1
            
            self._log_event({
                "type": "command_succeeded",
                "command_id": command.command_id,
                "command_type": command.command_type.value,
                "entity_id": command.target_entity_id,
            })
            
            return True, None
            
        except Exception as e:
            command.error = str(e)
            self.execution_stats["failed_commands"] += 1
            
            self._log_event({
                "type": "command_error",
                "command_id": command.command_id,
                "error": str(e),
            })
            
            return False, str(e)
    
    async def _execute_command_logic(self, command: SimulatedCommand) -> Dict[str, Any]:
        """Execute command-specific logic"""
        
        if command.command_type == CommandType.CREATE_PLAN:
            entity = SimulatedEntity(
                entity_type="plan",
                family_id=self.family_id,
                created_by=command.issued_by,
                attributes={
                    "title": command.payload.get("title", "Unnamed Plan"),
                    "status": "draft",
                },
            )
            self.state.add_entity(entity)
            command.target_entity_id = entity.entity_id
            self._generate_explanation(
                command, entity.entity_id, "plan",
                entity.attributes.get("title", entity.entity_id),
            )
            return {"entity_id": entity.entity_id}

        elif command.command_type == CommandType.CREATE_TASK:
            entity = SimulatedEntity(
                entity_type="task",
                family_id=self.family_id,
                created_by=command.issued_by,
                attributes={
                    "title": command.payload.get("title", "Unnamed Task"),
                    "status": "pending",
                    "plan_id": command.payload.get("plan_id"),
                },
            )
            self.state.add_entity(entity)
            self.state.increment_task_execution(entity.entity_id)
            command.target_entity_id = entity.entity_id
            self._generate_explanation(
                command, entity.entity_id, "task",
                entity.attributes.get("title", entity.entity_id),
            )
            return {"entity_id": entity.entity_id}

        elif command.command_type == CommandType.UPDATE_PLAN:
            if not command.target_entity_id:
                raise ValueError("UPDATE_PLAN requires target_entity_id")
            self.state.update_entity(command.target_entity_id, command.payload)
            _ent = self.state.get_entity(command.target_entity_id)
            _name = _ent.attributes.get("title", command.target_entity_id) if _ent else command.target_entity_id
            self._generate_explanation(command, command.target_entity_id, "plan", _name)
            return {"updated": True, "entity_id": command.target_entity_id}

        elif command.command_type == CommandType.MARK_TASK_COMPLETE:
            if not command.target_entity_id:
                raise ValueError("MARK_TASK_COMPLETE requires target_entity_id")
            # Track task execution to detect duplicates
            self.state.increment_task_execution(command.target_entity_id)
            self.state.update_entity(command.target_entity_id, {"status": "completed"})
            _ent = self.state.get_entity(command.target_entity_id)
            _name = _ent.attributes.get("title", command.target_entity_id) if _ent else command.target_entity_id
            self._generate_explanation(
                command, command.target_entity_id, "task", _name,
                task_status="completed",
            )
            return {"completed": True, "entity_id": command.target_entity_id}

        elif command.command_type == CommandType.CREATE_EVENT:
            entity = SimulatedEntity(
                entity_type="event",
                family_id=self.family_id,
                created_by=command.issued_by,
                attributes={
                    "title": command.payload.get("title", "Unnamed Event"),
                    "start_time": datetime.utcnow().isoformat(),
                },
            )
            self.state.add_entity(entity)
            command.target_entity_id = entity.entity_id
            self._generate_explanation(
                command, entity.entity_id, "event",
                entity.attributes.get("title", entity.entity_id),
            )
            return {"entity_id": entity.entity_id}

        else:
            raise NotImplementedError(f"Command type not implemented: {command.command_type}")
    
    async def run_scenario(
        self,
        scenario_name: str,
        scenario_generator,  # Async generator that yields commands
        failure_injector: Optional['FailureInjector'] = None,
        max_duration_seconds: float = 30.0,
    ) -> Dict[str, Any]:
        """
        Run a simulation scenario end-to-end
        
        Returns: simulation results
        """
        self._log_event({
            "type": "scenario_started",
            "name": scenario_name,
        })
        
        start_time = datetime.utcnow()
        timeout = timedelta(seconds=max_duration_seconds)
        
        async def execute_with_timeout():
            async for command in scenario_generator():
                elapsed = datetime.utcnow() - start_time
                if elapsed > timeout:
                    self._log_event({
                        "type": "scenario_timeout",
                        "elapsed_seconds": elapsed.total_seconds(),
                    })
                    break
                
                # Execute command concurrently
                await self.execute_command(command, failure_injector)
        
        try:
            await execute_with_timeout()
        except asyncio.TimeoutError:
            self._log_event({
                "type": "scenario_timeout_error",
            })
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        results = {
            "scenario_name": scenario_name,
            "family_id": self.family_id,
            "duration_seconds": duration,
            "state_hash": self.state.state_hash(),
            "event_count": len(self.event_log),
            "entity_count": len(self.state.entities),
            "stats": self.execution_stats,
            "quarantine_mode": self.state.quarantine_mode,
            "quarantine_reason": self.state.quarantine_reason,
        }
        
        self._log_event({
            "type": "scenario_completed",
            "name": scenario_name,
            "duration_seconds": duration,
            "results": results,
        })
        
        return results
