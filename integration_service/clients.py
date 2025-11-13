"""
This module provides communication clients for external systems used in the integration service:
- Inventory Service (gRPC)
- Payment Service (REST API)
- Warehouse Management System (RabbitMQ)
Each class encapsulates its protocol logic, error handling, and connection management.
"""

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
INVENTORY_SERVICE_URL = os.environ.get("INVENTORY_SERVICE_URL", "inventory_service:50051")
PAYMENT_SERVICE_URL = os.environ.get("PAYMENT_SERVICE_URL", "http://payment_service:8001")
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")

log = logging.getLogger(__name__)


# --- Inventory Client (gRPC) ---
class InventoryClient:
    """
    Client for the Inventory Service (gRPC).
    Handles reservation and release of items during order processing.
    """
    def __init__(self):
        """
        Initializes the gRPC channel and stub for the Inventory Service.
        """
        self.channel = grpc.insecure_channel(INVENTORY_SERVICE_URL)
        self.stub = inventory_pb2_grpc.InventoryServiceStub(self.channel)

    def __del__(self):
        """Closes the gRPC channel when the instance is deleted."""
        self.channel.close()

    def reserve_items(self, order_id: str, items: list) -> inventory_pb2.ReserveItemsResponse:
        """
        Sends a ReserveItems request to the Inventory Service.
        Args:
            order_id (str): Unique ID of the order.
            items (list): List of item dicts with 'sku' and 'quantity'.
        Returns:
            inventory_pb2.ReserveItemsResponse: The reservation result including status and reservation_id.
        Raises:
            grpc.RpcError: If the gRPC call fails or times out.
        """
        proto_items = [inventory_pb2.Item(sku=item['sku'], quantity=item['quantity']) for item in items]
        request = inventory_pb2.ReserveItemsRequest(order_id=order_id, items=proto_items)
        try:
            return self.stub.ReserveItems(request, timeout=5)
        except grpc.RpcError as e:
            log.error(f"[Order: {order_id}] gRPC Call zu IS fehlgeschlagen: {e.code()} - {e.details()}")
            raise

    def release_items_compensation(self, order_id: str):
        """
        Sends a compensation request to release previously reserved items.
        This method should never fail silently. If an exception occurs,
        it should be logged and retried later.
        Args:
            order_id (str): The order ID whose reservation should be released.
        Raises:
            grpc.RpcError: If the gRPC call fails.
        """
        log.info(f"[Order: {order_id}] Kompensation: Sende 'ReleaseItems' an IS.")
        request = inventory_pb2.ReleaseItemsRequest(order_id=order_id)
        try:
            self.stub.ReleaseItems(request, timeout=5)
        except grpc.RpcError as e:
            log.critical(f"[Order: {order_id}] KOMPENSATION FEHLGESCHLAGEN: {e.details()}. BENÖTIGT MANUELLE AKTION!")
            raise


# --- Payment Client (REST) ---
class PaymentClient:
    """
    Client for the Payment Service (REST API).
    Handles the creation of payment charges and error responses.
    """
    def __init__(self):
        """
        Initializes the HTTP client with proper timeout configuration.
        """
        timeout_config = httpx.Timeout(5.0, read=8.0)
        self.client = httpx.Client(base_url=PAYMENT_SERVICE_URL, timeout=timeout_config)

    def __del__(self):
        """Closes the HTTP client session."""
        self.client.close()

    def create_charge(self, order_id: str, token: str, amount_cents: int, currency: str):
        """
        Creates a new charge via the Payment Service REST API.
        Args:
            order_id (str): Unique order identifier.
            token (str): Payment token provided by the OMS.
            amount_cents (int): Charge amount in cents.
            currency (str): ISO currency code (e.g. 'EUR').
        Returns:
            dict: JSON response containing transaction details and status.
        Raises:
            httpx.ReadTimeout: If the service does not respond within the timeout.
            httpx.HTTPStatusError: If the service returns an error status (4xx or 5xx).
        """
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
    """
    Client for the Warehouse Management System (RabbitMQ).
    Sends shipment instructions and manages the MQ connection.
    """
    def __init__(self):
        """Initializes the RabbitMQ connection and declares required queues."""
        self.connection = None
        self.channel = None
        self._connect()

    def _connect(self):
        """
        Establishes a RabbitMQ connection using predefined credentials.
        Raises:
            pika.exceptions.AMQPConnectionError: If the connection fails.
        """
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
        """
        Sends a shipment instruction message to the WMS queue.
        Args:
            order_id (str): The order ID.
            items (list): List of items in the order.
        Raises:
            Exception: If message publishing fails.
        """
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
    """
    Starts a background thread that listens for WMS status updates.
    The listener consumes messages from the `wms.status.updates` queue,
    parses them as JSON, logs their content, and acknowledges valid messages.
    On connection loss or errors, it attempts automatic reconnection after 10 seconds.
    """
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
