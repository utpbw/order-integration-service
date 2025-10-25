from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import logging
import time
import uuid

app = FastAPI(title="Mock Payment Service")
logging.basicConfig(level=logging.INFO)


class ChargeRequest(BaseModel):
    amount: int
    currency: str
    paymentToken: str
    referenceId: str


@app.post("/v2/charges")
def create_charge(
        request: ChargeRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key")
):
    logging.info(f"[PS] Zahlungsanfrage für {request.referenceId} (Idempotenz: {idempotency_key})")

    # Szenario-Simulation
    if request.paymentToken.startswith("tok_decline_"):
        logging.warning(f"[PS] Zahlung für {request.referenceId} abgelehnt.")
        raise HTTPException(
            status_code=402,
            detail={"errorCode": "payment_declined", "message": "Karte abgelehnt."}
        )

    if request.paymentToken.startswith("tok_timeout_"):
        logging.info(f"[PS] Simuliere Timeout für {request.referenceId}...")
        time.sleep(10)  # Simuliert ein Timeout (länger als der Client wartet)
        logging.error(f"[PS] Timeout-Anfrage {request.referenceId} abgeschlossen (zu spät).")
        # Der Client wird hier bereits einen 504 oder 503 Fehler erhalten
        return

    # Erfolgsfall
    logging.info(f"[PS] Zahlung für {request.referenceId} erfolgreich.")
    return {
        "transactionId": f"tr_{uuid.uuid4()}",
        "status": "succeeded",
        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)