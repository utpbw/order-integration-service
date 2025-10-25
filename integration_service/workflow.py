"""
workflow.py — Core Orchestration Logic for Order Processing

This module contains the main workflow logic for automatic order processing.
It coordinates all service interactions (Inventory, Payment, WMS) in the correct sequence.

Workflow Overview:
1. Check and reserve inventory via Inventory Service (gRPC)
2. Process payment via Payment Service (REST)
3. Send shipment instruction to WMS via RabbitMQ
4. Handle errors and compensation steps (Saga Pattern)
"""

import logging

import grpc
import pika

from .clients import InventoryClient, PaymentClient, WMSClient
# Importiere die gRPC-Status-Enums für den Vergleich
from protos import inventory_pb2
import httpx

log = logging.getLogger(__name__)


def process_order_workflow(order_data: dict):
    """
    Executes the complete order processing workflow for a single order.

    This function is called asynchronously by the API (e.g. in a background task)
    after receiving an order from the Order Management System (OMS).

    The workflow includes:
        1. Inventory check and reservation via Inventory Service (gRPC)
        2. Payment processing via Payment Service (REST API)
        3. Shipment initiation via Warehouse Management System (RabbitMQ)
        4. Compensation logic (Saga Pattern) in case of errors

    Args:
        - order_data (dict): Order details, expected keys:
        - orderId (str): Unique order identifier
        - items (list[dict]): List of items with 'sku' and 'quantity'
        - paymentToken (str): Payment authorization token
        - totalAmount (float): Total order value
        - currency (str): Currency code (e.g., "EUR")

    Returns:
        None. The function logs all results and errors for monitoring.

    Raises:
        grpc.RpcError: If the gRPC communication with Inventory Service fails.
        httpx.HTTPStatusError: If the Payment Service returns a failed response.
        pika.exceptions.AMQPError: If the message queue communication with WMS fails.
        Exception: For any unexpected internal errors.

    Workflow Steps:
        Step 1 – Inventory Service:
            - Sends reservation request via gRPC.
            - Cancels workflow if items are out of stock or not found.

        Step 2 – Payment Service:
            - Converts totalAmount to cents and sends payment request.
            - If declined or timeout → compensates by releasing items.

        Step 3 – WMS:
            - Publishes order shipment message via RabbitMQ.
            - Logs success and waits for WMS status updates.

    Compensation (Saga Pattern):
        - If the payment fails → release reserved items.
        - If WMS fails after successful payment → log critical error (manual intervention required).
    """

    order_id = order_data["orderId"]
    log_prefix = f"[Order: {order_id}]"

    log.info(f"{log_prefix} Starte Verarbeitung.")

    is_client = None
    ps_client = None
    wms_client = None

    try:
        # --- 1. Inventory Service (gRPC) ---
        log.info(f"{log_prefix} Schritt 1: Prüfe und reserviere Inventar (IS)...")
        is_client = InventoryClient()
        reservation = is_client.reserve_items(order_id, order_data["items"])

        if reservation.status == inventory_pb2.ReservationStatus.OUT_OF_STOCK:
            log.warning(f"{log_prefix} Storniert: Artikel nicht auf Lager (OUT_OF_STOCK).")
            return

        if reservation.status == inventory_pb2.ReservationStatus.ITEM_NOT_FOUND:
            log.error(f"{log_prefix} Storniert: Artikel-SKU nicht gefunden (ITEM_NOT_FOUND).")
            return

        if reservation.status != inventory_pb2.ReservationStatus.RESERVED:
            log.error(f"{log_prefix} Storniert: Unbekannter Fehler vom IS (Status: {reservation.status}).")
            return

        log.info(f"{log_prefix} Inventar erfolgreich reserviert. (ID: {reservation.reservation_id})")

        # --- 2. Payment Service (REST) ---
        log.info(f"{log_prefix} Schritt 2: Führe Zahlung durch (PS)...")
        ps_client = PaymentClient()
        # Conversion of float (e.g., 149.99) to cents (14999)
        amount_cents = int(order_data["totalAmount"] * 100)

        try:
            payment_result = ps_client.create_charge(
                order_id=order_id,
                token=order_data["paymentToken"],
                amount_cents=amount_cents,
                currency=order_data["currency"]
            )
            log.info(f"{log_prefix} Zahlung erfolgreich. (TxID: {payment_result.get('transactionId')})")

        except httpx.HTTPStatusError as e:
            # Payment failed (e.g., 402 Rejected)
            log.error(f"{log_prefix} Zahlung fehlgeschlagen (HTTP {e.response.status_code}). Starte Kompensation.")

            # KOMPENSATION (Saga-Pattern)
            try:
                is_client.release_items_compensation(order_id)
                log.info(f"{log_prefix} Kompensation erfolgreich. Workflow gestoppt.")
            except Exception as comp_e:
                log.critical(f"{log_prefix} KRITISCH: Kompensation fehlgeschlagen! {comp_e}")
            return

        except (httpx.ReadTimeout, httpx.ConnectError) as e:
            # Transient error or timeout.
            # A retry (with an idempotency key) would be correct here.
            # For the sake of simplicity, we abort here and compensate.
            log.error(f"{log_prefix} Payment Service nicht erreichbar ({e}). Starte Kompensation.")
            is_client.release_items_compensation(order_id)
            log.info(f"{log_prefix} Kompensation (wegen PS-Fehler) erfolgreich. Workflow gestoppt.")
            return

        # --- 3. Warehouse Management System (MQ) ---
        log.info(f"{log_prefix} Schritt 3: Sende Auftrag an WMS (MQ)...")
        wms_client = WMSClient()
        wms_client.send_shipment_instruction(order_id, order_data["items"])

        log.info(f"{log_prefix} Verarbeitung erfolgreich abgeschlossen. Warte auf WMS-Updates.")

    except grpc.RpcError as e:
        log.error(f"{log_prefix} Workflow abgebrochen: Kritischer gRPC-Fehler mit IS. {e.details()}")
        # Hier ist keine Kompensation nötig, da IS der erste Schritt war.

    except pika.exceptions.AMQPError as e:
        log.critical(f"{log_prefix} Workflow abgebrochen: Kritischer MQ-Fehler mit WMS. {e}")
        # VERY CRITICAL: Payment was successful, but WMS could not be notified.
        # An alert must be triggered here and the order must be manually entered into the WMS!
        # Reversing the transaction (canceling the payment) is often more complex.

    except Exception as e:
        log.critical(f"{log_prefix} Unbekannter Fehler im Workflow: {e}", exc_info=True)
        # If necessary, attempt compensation if IS has already run.
        if 'reservation' in locals() and reservation.status == inventory_pb2.ReservationStatus.RESERVED:
            log.info(f"{log_prefix} Versuche Notfall-Kompensation...")
            if is_client:
                is_client.release_items_compensation(order_id)

    finally:
        # Close client connections (important for MQ)
        if wms_client:
            wms_client.close()
        # gRPC and HTTPX clients are handled via __del__ or context managers