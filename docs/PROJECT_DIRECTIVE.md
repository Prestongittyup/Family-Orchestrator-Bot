# Family Orchestration Bot — Execution Directive

## System Purpose
This repository is a modular household orchestration backend built in Python (FastAPI + SQLite).

## Current Phase: Phase 1 (Execution Spine)

We are NOT building features yet. We are building a working event pipeline.

---

## Phase 1 Goal
Build a minimal working system that can:

1. Accept a SystemEvent via HTTP POST
2. Validate event using Pydantic schema
3. Route event based on type
4. Execute module handler (Task module only for now)
5. Persist output to SQLite
6. Return created entity response

---

## Architecture Rules

- FastAPI backend only
- Python only (no Node, no JS backend logic)
- SQLite only for persistence (no external DB)
- No AI/agents yet
- No workflows yet
- No email/calendar ingestion yet

---

## Core Flow

API → SystemEvent → Router → Module Service → SQLite → Response

---

## Allowed Modules in Phase 1

- Task module ONLY

Everything else is stubbed or ignored.

---

## Required Behavior

When editing or generating code:

- Follow existing folder structure under /apps/api
- Do not create new architectural layers without approval
- Keep services thin and deterministic
- Use schema definitions in /schemas as contract reference

---

## First Milestone

System must support:

POST /event
→ type: task_created
→ creates task in SQLite
→ returns task_id

This is the ONLY required success condition for Phase 1.