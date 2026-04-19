"""Clone provenance — HMAC sign/verify endpoints for clone manifests."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_db_client, get_job_manager

router = APIRouter()


@router.post("/sign/{job_id}")
async def sign_job(
    job_id: str,
    client=Depends(get_db_client),
    jm=Depends(get_job_manager),
):
    """Sign the manifest for a completed clone job by ID.

    Looks up the job from the in-memory JobManager, constructs a canonical
    manifest, returns the signed envelope. Returns ``{"signed": false, …}``
    when the runtime secret is not configured.
    """
    from src.clone_provenance import build_manifest, sign_manifest

    job = jm.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    # Reconstruct inputs from the job snapshot — config lives under job_type-specific keys.
    manifest = build_manifest(
        source_catalog=job.get("source_catalog") or "",
        destination_catalog=job.get("destination_catalog") or "",
        config={
            "clone_type": job.get("clone_type"),
            "job_type": job.get("job_type"),
            "created_at": job.get("created_at"),
            "completed_at": job.get("completed_at"),
        },
        result=job.get("result") or {},
        job_id=job_id,
    )
    return sign_manifest(manifest)


@router.post("/sign")
async def sign_manifest_endpoint(payload: dict):
    """Sign an arbitrary manifest supplied by the caller.

    Body: the raw manifest dict. Useful for external orchestrators that
    construct their own manifest and want Clone-Xs to attest to it.
    """
    from src.clone_provenance import build_manifest, sign_manifest

    source = (payload.get("source_catalog") or "").strip()
    dest = (payload.get("destination_catalog") or "").strip()
    if not source or not dest:
        raise HTTPException(400, "source_catalog + destination_catalog required")

    manifest = build_manifest(
        source_catalog=source,
        destination_catalog=dest,
        config=payload.get("config") or {},
        result=payload.get("result") or {},
        job_id=payload.get("job_id"),
    )
    return sign_manifest(manifest)


@router.post("/verify")
async def verify(envelope: dict):
    """Verify a previously-signed manifest envelope. Returns ``{valid, reason}``."""
    from src.clone_provenance import verify_signature

    return verify_signature(envelope)
