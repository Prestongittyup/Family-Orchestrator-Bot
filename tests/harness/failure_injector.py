"""
Failure Injector - Controlled fault injection for robustness testing

Injects deterministic failures:
  - Orchestrator crash mid-transaction
  - Partial persistence failure
  - Out-of-order event delivery
  - Duplicate execution attempts
  - Lease expiration during execution
  - Network retry storms
"""

import asyncio
import random
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple
import math


class FailureMode(Enum):
    """Types of failures that can be injected"""
    ORCHESTRATOR_CRASH = "orchestrator_crash"
    PARTIAL_PERSISTENCE = "partial_persistence"
    DELAYED_RESPONSE = "delayed_response"
    DUPLICATE_EXECUTION = "duplicate_execution"
    LEASE_EXPIRATION = "lease_expiration"
    NETWORK_TIMEOUT = "network_timeout"
    RATE_LIMITING = "rate_limiting"
    TRANSIENT_ERROR = "transient_error"
    OUT_OF_ORDER_DELIVERY = "out_of_order_delivery"


@dataclass
class FailureInjectionProfile:
    """Configuration for failure injection"""
    name: str
    failure_modes: list  # List of (FailureMode, probability, can_retry)
    delay_range_ms: Tuple[int, int] = (10, 100)
    retry_attempts: int = 3
    exponential_backoff: bool = True
    cascade_failures: bool = False  # Can one failure trigger others
    deterministic: bool = True


class FailureInjector:
    """Orchestrates controlled fault injection"""
    
    # Pre-defined failure profiles
    PROFILES = {
        "no_failures": FailureInjectionProfile(
            name="no_failures",
            failure_modes=[],
        ),
        "light_transient": FailureInjectionProfile(
            name="light_transient",
            failure_modes=[
                (FailureMode.TRANSIENT_ERROR, 0.05, True),
                (FailureMode.DELAYED_RESPONSE, 0.10, True),
            ],
        ),
        "moderate_network": FailureInjectionProfile(
            name="moderate_network",
            failure_modes=[
                (FailureMode.NETWORK_TIMEOUT, 0.10, True),
                (FailureMode.DELAYED_RESPONSE, 0.15, True),
                (FailureMode.DUPLICATE_EXECUTION, 0.05, False),
            ],
        ),
        "high_chaos": FailureInjectionProfile(
            name="high_chaos",
            failure_modes=[
                (FailureMode.ORCHESTRATOR_CRASH, 0.05, False),
                (FailureMode.PARTIAL_PERSISTENCE, 0.08, False),
                (FailureMode.LEASE_EXPIRATION, 0.10, True),
                (FailureMode.NETWORK_TIMEOUT, 0.15, True),
                (FailureMode.RATE_LIMITING, 0.12, True),
                (FailureMode.OUT_OF_ORDER_DELIVERY, 0.05, False),
            ],
            cascade_failures=True,
        ),
        "byzantine": FailureInjectionProfile(
            name="byzantine",
            failure_modes=[
                (FailureMode.ORCHESTRATOR_CRASH, 0.10, False),
                (FailureMode.PARTIAL_PERSISTENCE, 0.15, False),
                (FailureMode.DUPLICATE_EXECUTION, 0.20, False),
                (FailureMode.OUT_OF_ORDER_DELIVERY, 0.10, False),
                (FailureMode.LEASE_EXPIRATION, 0.08, False),
            ],
            cascade_failures=True,
            exponential_backoff=False,
        ),
    }
    
    def __init__(
        self,
        profile: str = "no_failures",
        random_seed: int = 42,
        verbose: bool = False,
    ):
        if profile not in self.PROFILES:
            raise ValueError(f"Unknown profile: {profile}")
        
        self.profile = self.PROFILES[profile]
        self.random_seed = random_seed
        self.verbose = verbose
        random.seed(random_seed)
        
        self.injected_failures = []
        self.retry_counts = {}  # command_id -> retry count
        self.cascade_state = {}  # Track cascading failures
    
    async def should_inject_failure(
        self,
        command,
        state,
    ) -> Tuple[bool, Optional[str]]:
        """
        Determine if a failure should be injected for this command
        
        Returns: (should_fail, failure_reason)
        """
        # Check if in quarantine mode (system failed)
        if state.quarantine_mode:
            return False, None  # Don't inject more failures in quarantine
        
        for failure_mode, probability, can_retry in self.profile.failure_modes:
            if random.random() < probability:
                # Failure selected
                command_id = command.command_id
                
                if command_id not in self.retry_counts:
                    self.retry_counts[command_id] = 0
                
                # Check retry limit
                if not can_retry and self.retry_counts[command_id] > 0:
                    continue  # Don't inject non-retryable failure twice
                
                # Inject the failure
                failure_reason = self._generate_failure_message(
                    failure_mode,
                    command,
                    self.retry_counts[command_id],
                )
                
                self.injected_failures.append({
                    "command_id": command_id,
                    "mode": failure_mode.value,
                    "reason": failure_reason,
                    "retryable": can_retry,
                })
                
                self.retry_counts[command_id] += 1
                
                if self.verbose:
                    print(f"[INJECTED] {failure_mode.value}: {failure_reason}")
                
                # Check for cascade failures
                if self.profile.cascade_failures:
                    await self._maybe_cascade_failure(failure_mode, state)
                
                return True, failure_reason
        
        return False, None
    
    def _generate_failure_message(
        self,
        failure_mode: FailureMode,
        command,
        retry_count: int,
    ) -> str:
        """Generate descriptive failure message"""
        
        messages = {
            FailureMode.ORCHESTRATOR_CRASH: (
                f"Orchestrator crashed mid-transaction for {command.command_type.value}"
            ),
            FailureMode.PARTIAL_PERSISTENCE: (
                f"Partial persistence failure: entity created but watermark not updated"
            ),
            FailureMode.DELAYED_RESPONSE: (
                f"Network delay injected (attempt {retry_count + 1})"
            ),
            FailureMode.DUPLICATE_EXECUTION: (
                f"Duplicate execution detected (idempotency_key: {command.idempotency_key[:8]}...)"
            ),
            FailureMode.LEASE_EXPIRATION: (
                f"Lease expired during command execution"
            ),
            FailureMode.NETWORK_TIMEOUT: (
                f"Network timeout after {retry_count + 1} attempt(s)"
            ),
            FailureMode.RATE_LIMITING: (
                f"Rate limit exceeded (HTTP 429)"
            ),
            FailureMode.TRANSIENT_ERROR: (
                f"Transient error, retry suggested"
            ),
            FailureMode.OUT_OF_ORDER_DELIVERY: (
                f"Message delivered out of order, causality violation"
            ),
        }
        
        return messages.get(failure_mode, "Unknown failure")
    
    async def _maybe_cascade_failure(
        self,
        failure_mode: FailureMode,
        state,
    ) -> None:
        """Potentially trigger cascading failures"""
        
        cascade_probabilities = {
            FailureMode.ORCHESTRATOR_CRASH: [
                (FailureMode.PARTIAL_PERSISTENCE, 0.8),
                (FailureMode.LEASE_EXPIRATION, 0.6),
            ],
            FailureMode.PARTIAL_PERSISTENCE: [
                (FailureMode.OUT_OF_ORDER_DELIVERY, 0.5),
                (FailureMode.DUPLICATE_EXECUTION, 0.4),
            ],
        }
        
        if failure_mode in cascade_probabilities:
            for cascaded_mode, prob in cascade_probabilities[failure_mode]:
                if random.random() < prob:
                    if self.verbose:
                        print(f"[CASCADE] {cascaded_mode.value} triggered by {failure_mode.value}")
                    self.cascade_state[cascaded_mode.value] = True
    
    def get_retry_delay(self, command_id: str) -> float:
        """Get retry delay in seconds (exponential backoff)"""
        retry_count = self.retry_counts.get(command_id, 0)
        
        if not self.profile.exponential_backoff:
            return 0.1  # Fixed delay
        
        # Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
        base_delay = 0.1
        max_delay = 10.0
        delay = base_delay * math.pow(2, retry_count)
        return min(delay, max_delay)
    
    def should_retry(self, command_id: str) -> bool:
        """Check if command should be retried"""
        retry_count = self.retry_counts.get(command_id, 0)
        return retry_count < self.profile.retry_attempts
    
    def get_injection_summary(self) -> dict:
        """Get summary of injected failures"""
        by_mode = {}
        for failure in self.injected_failures:
            mode = failure["mode"]
            by_mode[mode] = by_mode.get(mode, 0) + 1
        
        return {
            "total_injected": len(self.injected_failures),
            "by_mode": by_mode,
            "cascade_count": len(self.cascade_state),
        }


class FailureScenarioBuilder:
    """Builder for custom failure scenarios"""
    
    def __init__(self):
        self.failure_modes = []
    
    def add_failure(
        self,
        mode: FailureMode,
        probability: float,
        retryable: bool = True,
    ):
        """Add a failure mode to scenario"""
        self.failure_modes.append((mode, probability, retryable))
        return self
    
    def with_cascade(self):
        """Enable cascading failures"""
        self.cascade_failures = True
        return self
    
    def build(self, name: str) -> FailureInjectionProfile:
        """Build custom profile"""
        return FailureInjectionProfile(
            name=name,
            failure_modes=self.failure_modes,
        )
