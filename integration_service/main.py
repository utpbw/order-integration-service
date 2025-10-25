"""
main.py — FastAPI Entry Point for Integration Service

This module provides the REST API interface for the Shop AG order integration service.
It acts as the entry point between the Order Management System (OMS) and the internal
orchestration workflow that automates order processing across multiple systems.

Responsibilities:
    • Accept new orders via HTTP API
    • Trigger asynchronous background processing (Inventory → Payment → WMS)
    • Start and manage background threads for message queue listeners
    • Provide system health information
"""

from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from .models import NewOrderRequest
from .workflow import process_order_workflow
from .clients import start_wms_status_listener
from .logging_config import setup_logging, get_logger
import threading

# Initialization
# Configure logging and initialize FastAPI app
setup_logging()
log = get_logger(__name__)
app = FastAPI(title="Shop AG Integrationslösung")


# Startup Event: Launch WMS Listener
@app.on_event("startup")
def on_startup():
    """
    FastAPI startup event handler.

    Starts the background thread for the Warehouse Management System (WMS)
    status listener. This thread continuously listens for incoming messages
    from RabbitMQ and logs order status updates.

    Behavior:
        - The thread runs as a daemon and stops automatically when the main app terminates.
        - Ensures real-time synchronization of WMS updates with the integration log.
    """
    log.info("Integrations-Service startet...")
    listener_thread = threading.Thread(target=start_wms_status_listener, daemon=True)
    listener_thread.start()
    log.info("WMS Status Listener Thread gestartet.")


# API Endpoint: OMS → Integration Service
@app.post("/v1/orders", status_code=202)
async def submit_order(
        order: NewOrderRequest,
        background_tasks: BackgroundTasks
):
    """
    Receives a new order from the Order Management System (OMS) and starts processing.

    The endpoint accepts the order payload, validates it using the `NewOrderRequest` model,
    and triggers asynchronous background execution of the `process_order_workflow()` function.

    Response code 202 (Accepted) indicates that processing has started successfully,
    while the actual fulfillment continues in the background.

    Args:
        order (NewOrderRequest): Validated order payload from the OMS.
        background_tasks (BackgroundTasks): FastAPI background task handler for async processing.

    Returns:
        dict: JSON response containing:
            - processingId (str): Unique processing identifier.
            - orderId (str): Original order ID.
            - status (str): Processing acknowledgment message.

    Raises:
        HTTPException(500): If an internal error occurs during order acceptance.
    """
    try:
        log_prefix = f"[Order: {order.orderId}]"
        log.info(f"{log_prefix} Neue Bestellung vom OMS API erhalten.")

        # Schedule asynchronous workflow execution
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


# Health Check Endpoint
@app.get("/health")
def health_check():
    """
    Simple health check endpoint.

    Can be used by monitoring systems or container orchestrators
    (e.g., Docker, Kubernetes) to verify that the service is running.

    Returns:
        dict: A basic JSON object indicating service availability.
    """
    return {"status": "ok"}