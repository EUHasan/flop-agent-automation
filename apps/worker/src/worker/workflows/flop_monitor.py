"""Daily FLOP devnet agent monitor.

Checks the FLOP gateway health, the live model catalog, the faucet status and,
when a gateway key is bound as the ``FLOP_GATEWAY_KEY`` secret, the agent
key's remaining quota and expiry. Reports an alert when the gateway is down,
the key is expired or close to expiry, or the faucet is closed.
"""

from datetime import UTC, datetime

import httpx
import mistralai.workflows as workflows
from pydantic import BaseModel, Field

FLOP_GATEWAY_BASE_URL = "https://api.flop.network"
FLOP_FAUCET_BASE_URL = "https://faucet.flop.network"
KEY_EXPIRY_ALERT_HOURS = 6.0


class MonitorInput(BaseModel):
    address: str = Field(
        default="5CdC5BbNMfyQeS1KCoZ1uWzckvTeQokMacBNmTgRCRtmKTie",
        description="SS58 address of the FLOP devnet agent wallet.",
    )


class MonitorOutput(BaseModel):
    overall_status: str
    gateway_status: str
    models: list[str]
    faucet_status: str
    key_status: str
    remaining_quota: float | None = None
    key_expires_at: int | None = None
    hours_left: float | None = None
    message: str


@workflows.activity()
async def check_gateway() -> dict:
    """Fetch the FLOP gateway health and the live model catalog."""
    async with httpx.AsyncClient(timeout=30) as client:
        health_response = await client.get(f"{FLOP_GATEWAY_BASE_URL}/health")
        health_response.raise_for_status()
        health = health_response.json()
        models_response = await client.get(f"{FLOP_GATEWAY_BASE_URL}/v1/models")
        models_response.raise_for_status()
        models = [entry["id"] for entry in models_response.json().get("data", [])]
    return {"status": health.get("status", "unknown"), "models": models}


@workflows.activity()
async def check_faucet() -> dict:
    """Fetch the FLOP devnet faucet status."""
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(f"{FLOP_FAUCET_BASE_URL}/limits")
        response.raise_for_status()
        limits = response.json()
    return {"status": limits.get("faucet", {}).get("status", "unknown")}


@workflows.activity()
async def check_agent_key() -> dict:
    """Check the agent gateway key bound as the FLOP_GATEWAY_KEY secret."""
    import os

    key = os.environ.get("FLOP_GATEWAY_KEY", "").strip()
    if not key:
        return {"key_status": "not_configured"}

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(
            f"{FLOP_GATEWAY_BASE_URL}/v1/keys/me",
            headers={"Authorization": f"Bearer {key}"},
        )
    if response.status_code in (401, 403):
        return {"key_status": "invalid"}
    response.raise_for_status()
    body = response.json()

    expires_at = body.get("expires_at")
    hours_left = None
    if expires_at is not None:
        expires_at = int(expires_at)
        hours_left = round((expires_at / 1000 - datetime.now(UTC).timestamp()) / 3600, 2)
        key_status = "active" if hours_left > 0 else "expired"
    else:
        key_status = "active"

    return {
        "key_status": key_status,
        "remaining_quota": body.get("remaining_quota"),
        "key_expires_at": expires_at,
        "hours_left": hours_left,
    }


@workflows.workflow.define(name="flop_agent_monitor")
class FlopAgentMonitor:
    @workflows.workflow.entrypoint
    async def run(self, data: MonitorInput) -> MonitorOutput:
        gateway = await check_gateway()
        faucet = await check_faucet()
        key = await check_agent_key()

        problems: list[str] = []
        if gateway["status"] != "ok":
            problems.append(f"gateway is {gateway['status']}")
        if not gateway["models"]:
            problems.append("no live models")
        if faucet["status"] != "open":
            problems.append(f"faucet is {faucet['status']}")
        if key["key_status"] == "invalid":
            problems.append("gateway key is invalid or revoked")
        elif key["key_status"] == "expired":
            problems.append("gateway key has expired; re-enroll via the connect page")
        elif key["key_status"] == "active" and key["hours_left"] is not None:
            if key["hours_left"] <= 0:
                problems.append("gateway key has expired; re-enroll via the connect page")
            elif key["hours_left"] <= KEY_EXPIRY_ALERT_HOURS:
                problems.append(
                    f"gateway key expires in {key['hours_left']} hours; re-enroll soon"
                )
        elif key["key_status"] == "not_configured":
            problems.append("FLOP_GATEWAY_KEY secret not configured; quota check skipped")

        overall_status = "alert" if problems else "healthy"
        if problems:
            message = f"FLOP agent alert for {data.address}: " + "; ".join(problems) + "."
        else:
            message = (
                f"FLOP agent for {data.address} is healthy: gateway ok, "
                f"{len(gateway['models'])} live models, faucet open"
            )
            if key["key_status"] == "active":
                message += (
                    f", key active with {key['remaining_quota']} FLOP quota "
                    f"and {key['hours_left']} hours left"
                )

        return MonitorOutput(
            overall_status=overall_status,
            gateway_status=gateway["status"],
            models=gateway["models"],
            faucet_status=faucet["status"],
            key_status=key["key_status"],
            remaining_quota=key.get("remaining_quota"),
            key_expires_at=key.get("key_expires_at"),
            hours_left=key.get("hours_left"),
            message=message,
        )
