"""
mock_wms.py — Mock Implementation of the Warehouse Management System (WMS)

This module simulates the behavior of a Warehouse Management System that receives
shipping instructions from the integration service (via RabbitMQ) and publishes
status updates back to the system.

Purpose:
    • Simulate asynchronous warehouse operations (pick, pack, ship)
    • Test the end-to-end order fulfillment workflow
    • Provide realistic timing and message patterns for integration tests

Communication Channels:
    - Input Queue:  'wms.orders.new'        ← Receives new shipment instructions
    - Output Queue: 'wms.status.updates'    → Sends order status updates

The WMS mock uses threads to simulate non-blocking message processing and
publishes status messages over time to emulate real warehouse delays.
"""

import pika
import time
import json
import threading
import os
import logging

logging.basicConfig(level=logging.INFO)
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")

# Connection Utilities
def get_mq_connection():
    """
    Establishes and returns a connection to the RabbitMQ message broker.

    Uses default credentials (`shopag` / `shopag`) and the host defined
    by the `RABBITMQ_HOST` environment variable.

    Returns:
        pika.BlockingConnection: Active connection to the RabbitMQ broker.

    Raises:
        pika.exceptions.AMQPConnectionError: If the broker is unavailable.
    """
    credentials = pika.PlainCredentials('shopag', 'shopag')
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )


def send_status_update(order_id: str):
    """
    Simulates the shipment process and sends sequential status updates
    for a given order via RabbitMQ.

    The simulated lifecycle includes:
        1. ITEMS_PICKED
        2. ORDER_PACKED
        3. ORDER_SHIPPED

    Each stage includes a sleep delay to mimic real processing times.

    Args:
        order_id (str): Unique identifier of the order being processed.
    Raises:
        Exception: Logs and handles any publishing or connection errors.
    """
    try:
        connection = get_mq_connection()
        channel = connection.channel()
        channel.queue_declare(queue='wms.status.updates')  # Target queue of the integration

        logging.info(f"[WMS] Beginne Bearbeitung für Order {order_id}")

        time.sleep(3)  # Simulate order picking
        msg_picked = {"orderId": order_id, "status": "ITEMS_PICKED",
                      "updateTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        channel.basic_publish(exchange='', routing_key='wms.status.updates', body=json.dumps(msg_picked))
        logging.info(f"[WMS] Status gesendet: ITEMS_PICKED for {order_id}")

        time.sleep(3)  # Simulate packaging
        msg_packed = {"orderId": order_id, "status": "ORDER_PACKED",
                      "updateTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        channel.basic_publish(exchange='', routing_key='wms.status.updates', body=json.dumps(msg_packed))
        logging.info(f"[WMS] Status gesendet: ORDER_PACKED for {order_id}")

        time.sleep(2)  # Simulate shipping
        msg_shipped = {"orderId": order_id, "status": "ORDER_SHIPPED",
                       "updateTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "trackingNumber": "TRK12345ABC"}
        channel.basic_publish(exchange='', routing_key='wms.status.updates', body=json.dumps(msg_shipped))
        logging.info(f"[WMS] Status gesendet: ORDER_SHIPPED for {order_id}")

        connection.close()
    except Exception as e:
        logging.error(f"[WMS] Fehler im Status-Update-Thread: {e}")


def on_order_received(ch, method, properties, body):
    """
    Callback function triggered when a new message arrives on the 'wms.orders.new' queue.

    Parses the incoming shipment instruction, extracts the order ID, and starts
    a separate thread to simulate the fulfillment process.

    Args:
        ch (BlockingChannel): The RabbitMQ channel object.
        method (pika.spec.Basic.Deliver): Delivery metadata for acknowledgment.
        properties (pika.BasicProperties): Message properties.
        body (bytes): The raw message payload in JSON format.

    Behavior:
        - Logs received orders.
        - Starts a background thread via `send_status_update(order_id)`.
        - Acknowledges successful message handling.
        - Rejects malformed messages to Dead Letter Queue (DLQ).

    Raises:
        Exception: Logs and Negative Acknowledgment any message that cannot be parsed or processed.
    """
    try:
        data = json.loads(body)
        order_id = data.get("orderId")
        logging.info(f"[WMS] Neue Versandanweisung für Order {order_id} erhalten.")

        # Start the simulation in a new thread so as not to block the MQ consumer.
        threading.Thread(target=send_status_update, args=(order_id,)).start()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"[WMS] Fehler bei Nachrichtenverarbeitung: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # In DLQ (falls konfiguriert)


def main():
    """
        Starts the mock WMS message consumer loop.

        The service listens for new shipment instructions on the
        `wms.orders.new` queue, processes them asynchronously, and
        simulates the warehouse update cycle.

        Behavior:
            - Establishes a RabbitMQ connection.
            - Waits for incoming messages.
            - Automatically retries connection every 5 seconds if lost.
            - Stops gracefully on keyboard interrupt (Ctrl+C).
    """
    logging.info("Mock WMS Service (MQ) startet...")
    while True:
        try:
            connection = get_mq_connection()
            channel = connection.channel()
            channel.queue_declare(queue='wms.orders.new')  # Wartet auf neue Aufträge

            logging.info("[WMS] Wartet auf neue Aufträge. (Consumer aktiv)")
            channel.basic_consume(queue='wms.orders.new', on_message_callback=on_order_received)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logging.warning(f"MQ-Verbindung fehlgeschlagen, versuche erneut in 5s... {e}")
            time.sleep(5)
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    main()