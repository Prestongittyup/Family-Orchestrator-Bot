from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DAGNode:
    node_id: str
    node_type: str
    operation: str
    dependencies: list[str] = field(default_factory=list)
    service_type: str | None = None
    inputs: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "operation": self.operation,
            "dependencies": list(self.dependencies),
            "service_type": self.service_type,
            "inputs": dict(self.inputs),
            "metadata": dict(self.metadata),
        }


@dataclass
class DAG:
    dag_id: str
    intent_id: str
    nodes: dict[str, DAGNode]
    entry_node: str | None
    exit_nodes: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "intent_id": self.intent_id,
            "nodes": {node_id: node.to_dict() for node_id, node in self.nodes.items()},
            "entry_node": self.entry_node,
            "exit_nodes": list(self.exit_nodes),
            "metadata": dict(self.metadata),
        }
