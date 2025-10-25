import grpc
from concurrent import futures
import time
import logging

# Importierte generierte Stubs
from protos import inventory_pb2, inventory_pb2_grpc

logging.basicConfig(level=logging.INFO)


class InventoryService(inventory_pb2_grpc.InventoryServiceServicer):

    def ReserveItems(self, request, context):
        logging.info(f"[IS] Reservierungsanfrage für Order: {request.order_id}")

        # Szenario-Simulation
        for item in request.items:
            if "OUT-OF-STOCK" in item.sku:
                logging.warning(f"[IS] SKU {item.sku} ist nicht auf Lager.")
                return inventory_pb2.ReserveItemsResponse(
                    status=inventory_pb2.ReservationStatus.OUT_OF_STOCK
                )
            if "NOT-FOUND" in item.sku:
                logging.error(f"[IS] SKU {item.sku} nicht gefunden.")
                return inventory_pb2.ReserveItemsResponse(
                    status=inventory_pb2.ReservationStatus.ITEM_NOT_FOUND
                )

        # Erfolgsfall
        reservation_id = f"res-{request.order_id}-{int(time.time())}"
        logging.info(f"[IS] Artikel für {request.order_id} reserviert. ID: {reservation_id}")
        return inventory_pb2.ReserveItemsResponse(
            reservation_id=reservation_id,
            status=inventory_pb2.ReservationStatus.RESERVED
        )

    def ReleaseItems(self, request, context):
        logging.info(f"[IS-KOMPENSATION] Reservierung für {request.order_id} freigegeben.")
        return inventory_pb2.ReleaseItemsResponse(success=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryService(), server)
    server.add_insecure_port('[::]:50051')
    logging.info("Mock Inventory Service (gRPC) startet auf Port 50051...")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()