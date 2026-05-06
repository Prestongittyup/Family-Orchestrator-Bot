ALLOWED_DEPENDENCIES = {
    "apps.api.endpoints": {
        "integration_core.orchestrator",
    },
    "integration_core.orchestrator": {
        "integration_core.state_builder",
    },
    "integration_core.state_builder": {
        "integration_core.providers",
    },
    "integration_core.providers": set(),
}
