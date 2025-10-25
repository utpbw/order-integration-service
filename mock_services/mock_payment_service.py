"""
mock_payment_service.py — Mock Implementation of the Payment Service (REST API)

This module provides a simulated Payment Service for testing the integration workflow.
It exposes a simple FastAPI application that mimics real-world payment processing behavior.

Simulation Scenarios:
    • Successful payment processing
    • Declined payment (HTTP 402)
    • Timeout simulation (simulates client read timeout)

Endpoints:
    POST /v2/charges — Handles incoming charge requests.

Port:
    Default: 8001 (HTTP)
"""

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import logging
import time
import uuid

app = FastAPI(title="Mock Payment Service")
logging.basicConfig(level=logging.INFO)


class ChargeRequest(BaseModel):
    """
    Represents a payment charge request payload.

    Attributes:
        amount (int): Total payment amount in the smallest currency units (e.g., cents).
        currency (str): ISO 4217 currency code (e.g., 'EUR', 'USD').
        paymentToken (str): Payment authorization token.
        referenceId (str): Unique identifier for the order associated with this charge.
    """
    amount: int
    currency: str
    paymentToken: str
    referenceId: str


@app.post("/v2/charges")
def create_charge(
        request: ChargeRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key")
):
    """
        Processes a payment charge request.

        This endpoint simulates different payment outcomes based on the provided `paymentToken`:
            - Starts with "tok_decline_" → Payment declined (HTTP 402)
            - Starts with "tok_timeout_" → Simulated timeout (long-running process)
            - Any other token → Successful transaction

        Args:
            request (ChargeRequest): The charge details including amount, currency, token, and referenceId.
            idempotency_key (str): Unique identifier from the client to ensure request idempotency.

        Returns:
            dict: Payment transaction result on success, including:
                - transactionId (str): Unique transaction identifier.
                - status (str): Always "succeeded" for successful payments.
                - createdAt (str): UTC timestamp of the transaction.

        Raises:
            HTTPException(402): If the payment is declined.
            TimeoutError (simulated): If the request token triggers a timeout condition.
    """
    logging.info(f"[PS] Zahlungsanfrage für {request.referenceId} (Idempotenz: {idempotency_key})")

    # Scenario simulation
    if request.paymentToken.startswith("tok_decline_"):
        logging.warning(f"[PS] Zahlung für {request.referenceId} abgelehnt.")
        raise HTTPException(
            status_code=402,
            detail={"errorCode": "payment_declined", "message": "Karte abgelehnt."}
        )

    if request.paymentToken.startswith("tok_timeout_"):
        logging.info(f"[PS] Simuliere Timeout für {request.referenceId}...")
        time.sleep(10)
        logging.error(f"[PS] Timeout-Anfrage {request.referenceId} abgeschlossen (zu spät).")
        return

    # Success case
    logging.info(f"[PS] Zahlung für {request.referenceId} erfolgreich.")
    return {
        "transactionId": f"tr_{uuid.uuid4()}",
        "status": "succeeded",
        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)