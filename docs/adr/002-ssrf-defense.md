# ADR-002: Server-Side Request Forgery (SSRF) Defense

## Status
Accepted

## Context
InferenceOS provides an administrative endpoint (`POST /admin/backends`) allowing dynamic registration of model serving endpoints (e.g., `http://10.0.1.25:8000/v1`). If left unvalidated, an attacker could register internal IP addresses to probe internal services, access container metadata, or exploit cloud instance metadata services (e.g. AWS/GCP `169.254.169.254`).

## Decision
1. **Destination Validation**:
   - Every registered backend URL must be a valid HTTP or HTTPS URI.
   - Hostnames and resolved IP addresses are evaluated against a security filter:
     - Cloud metadata IPs (`169.254.169.254`, `fd00:ec2::254`) are strictly denied.
     - Multicast (`224.0.0.0/4`) and broadcast addresses are denied.
     - Admin can configure an explicit `ALLOWED_BACKEND_CIDRS` allowlist or domain suffix whitelist.
2. **Local Development Exception**:
   - For local development and docker-compose profiles, loopback (`127.0.0.1`, `localhost`) and Docker internal subnet patterns (`172.16.0.0/12`) are permitted only when `INFERENCEOS_ALLOW_LOCAL_BACKENDS=true`.
3. **Pre-flight Probe**:
   - Backend registration performs a fast connectivity probe to `/healthz` or `/v1/models` before the worker is admitted to the active routing table.

## Consequences
- Robust protection against SSRF and cloud credential exfiltration.
- Safe dynamic runtime cluster reconfiguration.
