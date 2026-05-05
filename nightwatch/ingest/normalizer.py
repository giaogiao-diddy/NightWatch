from nightwatch.models.incident import IncidentEvent


def normalize_incident(incident: IncidentEvent) -> dict[str, str]:
    return {
        "incident_id": incident.incident_id,
        "source": incident.source,
        "job_name": incident.job_name,
        "cluster": incident.cluster,
        "severity": incident.severity,
    }