import grpc
import httpx
import pika
import json
import logging
import uuid
import os
import time

# Importierte gRPC Stubs
from protos import inventory_pb2_grpc, inventory_pb2

# Service-Adressen (normalerweise aus Env Vars)
INVENTORY_SERVICE_URL = "mock_inventory_service:50051"
PAYMENT_SERVICE_URL = "http://mock_payment_service:8001"
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")

log = logging.getLogger(__name__)


# --- Inventory Client (gRPC) ---
class InventoryClient:
    def __init__(self):
        # Channel wird pro Instanz erstellt. In einer echten App: Singleton oder Pool.
        self.channel = grpc.insecure_channel(INVENTORY_SERVICE_URL)
        self.stub = inventory_pb2_grpc.InventoryServiceStub(self.channel)

    def __del__(self):
        self.channel.close()

    def reserve_items(self, order_id: str, items: list) -> inventory_pb2.ReserveItemsResponse:
        proto_items = [inventory_pb2.Item(sku=item['sku'], quantity=item['quantity']) for item in items]
        request = inventory_pb2.ReserveItemsRequest(order_id=order_id, items=proto_items)
        try:
            return self.stub.ReserveItems(request, timeout=5)
        except grpc.RpcError as e:
            log.error(f"[Order: {order_id}] gRPC Call zu IS fehlgeschlagen: {e.code()} - {e.details()}")
            raise

    def release_items_compensation(self, order_id: str):
        """Kompensationsaufruf. Darf nicht fehlschlagen (Retries nötig)."""
        log.info(f"[Order: {order_id}] Kompensation: Sende 'ReleaseItems' an IS.")
        request = inventory_pb2.ReleaseItemsRequest(order_id=order_id)
        try:
            self.stub.ReleaseItems(request, timeout=5)
        except grpc.RpcError as e:
            log.critical(f"[Order: {order_id}] KOMPENSATION FEHLGESCHLAGEN: {e.details()}. BENÖTIGT MANUELLE AKTION!")
            # In einer echten App: Retry-Loop oder in eine separate "Compensation-Failed"-Queue.
            raise


# --- Payment Client (REST) ---
class PaymentClient:
    def __init__(self):
        # Timeout: 5s connect, 8s read (muss kürzer als der PS-Timeout sein)
        timeout_config = httpx.Timeout(5.0, read=8.0)
        self.client = httpx.Client(base_url=PAYMENT_SERVICE_URL, timeout=timeout_config)

    def __del__(self):
        self.client.close()

    def create_charge(self, order_id: str, token: str, amount_cents: int, currency: str):
        idempotency_key = str(uuid.uuid4())
        payload = {
            "amount": amount_cents,
            "currency": currency,
            "paymentToken": token,
            "referenceId": order_id
        }
        headers = {"Idempotency-Key": idempotency_key}

        try:
            response = self.client.post("/v2/charges", json=payload, headers=headers)
            response.raise_for_status()  # Löst HTTPStatusError bei 4xx/5xx aus
            return response.json()
        except httpx.ReadTimeout:
            log.error(f"[Order: {order_id}] Payment Service Timeout (ReadTimeout). Status unbekannt.")
            # Dies ist der gefährliche Fall. Idempotenz-Key ist überlebenswichtig.
            # Wir behandeln es als Fehler, aber ein Retry (mit gleichem Key) wäre hier die Lösung.
            raise
        except httpx.HTTPStatusError as e:
            # Speziell für 402 (Payment Declined)
            if e.response.status_code == 402:
                log.warning(f"[Order: {order_id}] Zahlung abgelehnt: {e.response.json()}")
            else:
                log.error(f"[Order: {order_id}] HTTP-Fehler beim Payment: {e}")
            raise  # Fehler wird im Workflow behandelt


# --- WMS Client (MQ) ---
class WMSClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        try:
            credentials = pika.PlainCredentials('shopag', 'shopag')
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=60)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='wms.orders.new')
            log.info("WMS Client mit RabbitMQ verbunden.")
        except pika.exceptions.AMQPConnectionError as e:
            log.critical(f"WKATSTROPHAL: Kann nicht zu RabbitMQ (WMS) verbinden: {e}")
            raise

    def send_shipment_instruction(self, order_id: str, items: list):
        # In einer echten App würden hier noch Adressdaten etc. hinzukommen
        message = {
            "instructionId": str(uuid.uuid4()),
            "orderId": order_id,
            "instructionTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "items": items,
            "shippingAddress": {"name": "Max Mustermann", "street": "Testweg 1"}  # Dummy-Daten
        }
        try:
            if not self.connection or self.connection.is_closed:
                self._connect()

            self.channel.basic_publish(
                exchange='',
                routing_key='wms.orders.new',
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)  # Macht Nachricht persistent
            )
            log.info(f"[Order: {order_id}] Versandanweisung an WMS-Queue gesendet.")
        except Exception as e:
            log.error(f"[Order: {order_id}] FEHLER beim Senden an WMS-Queue: {e}")
            raise

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()


# --- WMS Listener (MQ Consumer) ---
def start_wms_status_listener():
    """Diese Funktion läuft in einem eigenen Thread und lauscht auf WMS-Updates."""
    log.info("WMS Status Listener Thread startet...")
    while True:
        try:
            credentials = pika.PlainCredentials('shopag', 'shopag')
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            channel = connection.channel()
            channel.queue_declare(queue='wms.status.updates')

            def callback(ch, method, properties, body):
                try:
                    data = json.loads(body)
                    order_id = data.get("orderId", "UNKNOWN")
                    status = data.get("status", "UNKNOWN")
                    # Alle WMS Updates werden zentral geloggt
                    log.info(f"[WMS-STATUS][Order: {order_id}] Status-Update: {status}. Details: {data}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except json.JSONDecodeError:
                    log.error(f"[WMS-STATUS] Ungültige JSON-Nachricht erhalten: {body}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # -> DLQ

            log.info("[WMS-STATUS] Listener ist aktiv und lauscht auf Updates.")
            channel.basic_consume(queue='wms.status.updates', on_message_callback=callback)
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            log.warning("WMS Listener: Verbindung zu RabbitMQ verloren. Versuche Reconnect in 10s...")
            time.sleep(10)
        except Exception as e:
            log.error(f"WMS Listener: Kritischer Fehler. {e}. Neustart in 10s.")
            time.sleep(10)