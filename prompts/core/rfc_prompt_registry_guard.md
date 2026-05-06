# NAME

- prompt_id: rfc_prompt_registry_guard
- prompt_title: RFC Prompt Registry Guard
- category: core
- version: 1.0.0

# RFC-001 BINDING

- RFC-001 Section 1: Canonical System Architecture
- RFC-001 Section 2: Canonical Runtime Flow
- RFC-001 Section 3.1: Event Log Is Authoritative
- RFC-001 Section 3.2: Execution Gateway Is Mandatory
- RFC-001 Section 5: Forbidden Anti-Patterns
- RFC-001 Section 6: Enforcement and Compliance

# EXECUTION PHASES

1. Verify: confirm prompt file location and category ownership
2. Assess: confirm schema completeness and RFC references
3. Implement: no-op for runtime code; registry metadata updates only
4. Test: validate prompt schema fields are present
5. Verify: confirm no prompt exists outside prompts/* categories

# INPUTS

- registry_root: string, required, must equal prompts/
- candidate_prompt_path: string, required
- candidate_prompt_content: string, required

# OUTPUT JSON SCHEMA (STRICT)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Prompt Registry Guard Output",
  "type": "object",
  "required": [
    "prompt_id",
    "category",
    "valid_location",
    "required_sections_present",
    "rfc_binding_present",
    "status",
    "next_step"
  ],
  "properties": {
    "prompt_id": {"type": "string", "const": "rfc_prompt_registry_guard"},
    "category": {"type": "string", "const": "core"},
    "valid_location": {"type": "boolean"},
    "required_sections_present": {"type": "boolean"},
    "rfc_binding_present": {"type": "boolean"},
    "status": {"type": "string", "enum": ["pass", "fail", "blocked"]},
    "violations": {
      "type": "array",
      "items": {"type": "string"}
    },
    "next_step": {"type": "string", "minLength": 1}
  },
  "additionalProperties": false
}
```

# CONSTRAINTS

- Do not modify runtime application logic.
- Do not move prompts across categories automatically.
- Do not permit prompts outside prompts/core, prompts/execution, prompts/audit, prompts/ci, prompts/templates.
- Do not allow category multiplexing in a single prompt file.

# FAILURE MODES

- PROMPT_OUTSIDE_REGISTRY
- CATEGORY_AMBIGUITY
- REQUIRED_SECTION_MISSING
- RFC_REFERENCE_MISSING
- SCHEMA_MISSING

# CI MAPPING

- tier: migration
- blocking: false

# ESCALATION RULES

- If prompt path is outside canonical registry, fail and require relocation before merge.
- If required sections are missing, fail and require template conformance update.
- If RFC binding is missing, fail and require explicit section references.
