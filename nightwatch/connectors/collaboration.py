def mock_create_collaboration_ticket(
    system: str,
    owner: str,
    title: str,
    body: str,
    labels: list[str] | None = None,
) -> dict[str, object]:
    normalized_system = system.lower().strip() or "jira"
    normalized_owner = owner.strip() or "unassigned"
    ticket_id = f"{normalized_system.upper()}-{normalized_owner.replace('.', '_')}"

    return {
        "system": normalized_system,
        "ticket_id": ticket_id,
        "owner": normalized_owner,
        "title": title,
        "body": body,
        "labels": labels or [],
        "result": "created",
    }
