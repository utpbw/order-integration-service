import pika
import time
import json
import threading
import os
import logging

logging.basicConfig(level=logging.INFO)
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")


def get_mq_connection():
    credentials = pika.PlainCredentials('shopag', 'shopag')
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    )


def send_status_update(order_id: str):
    """Simuliert den Versandprozess und sendet Updates."""
    try:
        connection = get_mq_connection()
        channel = connection.channel()
        channel.queue_declare(queue='wms.status.updates')  # Ziel-Queue der Integration

        logging.info(f"[WMS] Beginne Bearbeitung für Order {order_id}")

        time.sleep(3)  # Simuliere Kommissionierung
        msg_picked = {"orderId": order_id, "status": "ITEMS_PICKED",
                      "updateTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        channel.basic_publish(exchange='', routing_key='wms.status.updates', body=json.dumps(msg_picked))
        logging.info(f"[WMS] Status gesendet: ITEMS_PICKED for {order_id}")

        time.sleep(3)  # Simuliere Verpackung
        msg_packed = {"orderId": order_id, "status": "ORDER_PACKED",
                      "updateTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        channel.basic_publish(exchange='', routing_key='wms.status.updates', body=json.dumps(msg_packed))
        logging.info(f"[WMS] Status gesendet: ORDER_PACKED for {order_id}")

        time.sleep(2)  # Simuliere Versand
        msg_shipped = {"orderId": order_id, "status": "ORDER_SHIPPED",
                       "updateTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "trackingNumber": "TRK12345ABC"}
        channel.basic_publish(exchange='', routing_key='wms.status.updates', body=json.dumps(msg_shipped))
        logging.info(f"[WMS] Status gesendet: ORDER_SHIPPED for {order_id}")

        connection.close()
    except Exception as e:
        logging.error(f"[WMS] Fehler im Status-Update-Thread: {e}")


def on_order_received(ch, method, properties, body):
    try:
        data = json.loads(body)
        order_id = data.get("orderId")
        logging.info(f"[WMS] Neue Versandanweisung für Order {order_id} erhalten.")

        # Starte Simulation in einem neuen Thread, um den MQ-Consumer nicht zu blockieren
        threading.Thread(target=send_status_update, args=(order_id,)).start()

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"[WMS] Fehler bei Nachrichtenverarbeitung: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # In DLQ (falls konfiguriert)


def main():
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