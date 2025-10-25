from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from .models import NewOrderRequest
from .workflow import process_order_workflow
from .clients import start_wms_status_listener
from .logging_config import setup_logging, get_logger
import threading

# --- Initialisierung ---
setup_logging()
log = get_logger(__name__)
app = FastAPI(title="Shop AG Integrationslösung")


# --- Startup Event: WMS Listener ---
@app.on_event("startup")
def on_startup():
    log.info("Integrations-Service startet...")
    # Starte den WMS-Status-Listener in einem Hintergrund-Thread
    # Daemon=True sorgt dafür, dass der Thread mit der Hauptanwendung stirbt
    listener_thread = threading.Thread(target=start_wms_status_listener, daemon=True)
    listener_thread.start()
    log.info("WMS Status Listener Thread gestartet.")


# --- API Endpunkt (OMS -> Integration) ---
@app.post("/v1/orders", status_code=202)
async def submit_order(
        order: NewOrderRequest,
        background_tasks: BackgroundTasks
):
    """
    Nimmt eine neue Bestellung vom OMS entgegen.
    Antwortet sofort mit 202 (Accepted) und startet die
    Verarbeitung im Hintergrund.
    """
    try:
        log_prefix = f"[Order: {order.orderId}]"
        log.info(f"{log_prefix} Neue Bestellung vom OMS API erhalten.")

        # Füge den eigentlichen Workflow als Hintergrund-Task hinzu
        # Die API kehrt sofort zurück (asynchrone Verarbeitung)
        background_tasks.add_task(
            process_order_workflow,
            order_data=order.dict()
        )

        processing_id = f"proc-{order.orderId}"
        log.info(f"{log_prefix} Zur Hintergrundverarbeitung (ID: {processing_id}) angenommen.")

        return {
            "processingId": processing_id,
            "orderId": order.orderId,
            "status": "Processing accepted"
        }

    except Exception as e:
        log.critical(f"Kritischer Fehler bei der Annahme von Order {order.orderId}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while accepting order.")


# --- Health Check (Optional, aber empfohlen) ---
@app.get("/health")
def health_check():
    # Hier könnte man die Verbindung zu MQ, IS, PS prüfen
    return {"status": "ok"}