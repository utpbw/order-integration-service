import logging
from .clients import InventoryClient, PaymentClient, WMSClient
# Importiere die gRPC-Status-Enums für den Vergleich
from protos import inventory_pb2
import httpx

log = logging.getLogger(__name__)


def process_order_workflow(order_data: dict):
    """
    Der Kern-Orchestrierungsworkflow.
    Wird asynchron von der API aufgerufen.
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
            # Hier könnte man das OMS über die Stornierung informieren
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
        # Umrechnung von float (z.B. 149.99) in Cent (14999)
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
            # Zahlung fehlgeschlagen (z.B. 402 Abgelehnt)
            log.error(f"{log_prefix} Zahlung fehlgeschlagen (HTTP {e.response.status_code}). Starte Kompensation.")

            # KOMPENSATION (Saga-Pattern)
            try:
                is_client.release_items_compensation(order_id)
                log.info(f"{log_prefix} Kompensation erfolgreich. Workflow gestoppt.")
            except Exception as comp_e:
                log.critical(f"{log_prefix} KRITISCH: Kompensation fehlgeschlagen! {comp_e}")
            return  # Workflow hier beenden

        except (httpx.ReadTimeout, httpx.ConnectError) as e:
            # Transienter Fehler oder Timeout.
            # Ein Retry (mit Idempotenz-Key) wäre hier korrekt.
            # Aus Vereinfachungsgründen brechen wir hier ab und kompensieren.
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
        # SEHR KRITISCH: Zahlung war erfolgreich, aber WMS konnte nicht benachrichtigt werden.
        # Hier muss alarmiert werden und der Auftrag manuell ins WMS!
        # Die Kompensation (Zahlung stornieren) ist oft komplexer.

    except Exception as e:
        log.critical(f"{log_prefix} Unbekannter Fehler im Workflow: {e}", exc_info=True)
        # Ggf. Kompensation versuchen, falls IS schon lief
        if 'reservation' in locals() and reservation.status == inventory_pb2.ReservationStatus.RESERVED:
            log.info(f"{log_prefix} Versuche Notfall-Kompensation...")
            if is_client:
                is_client.release_items_compensation(order_id)

    finally:
        # Schließe Client-Verbindungen (wichtig für MQ)
        if wms_client:
            wms_client.close()
        # gRPC und HTTPX Clients werden durch __del__ oder Kontext-Manager gehandhabt