"""
mock_inventory_service.py — Mock Implementation of the Inventory Service (gRPC)

This module provides a simulated (mock) Inventory Service for the integration tests.
It implements the gRPC server defined in `inventory.proto` to test communication and
workflow orchestration without relying on a real backend system.

The mock simulates common inventory-related scenarios:
    • Successful reservation of items
    • Out-of-stock situations
    • Invalid SKU (item not found)
    • Compensation via release operation

Used primarily for local testing and workflow validation.

Port:
    Default: 50051 (gRPC)
"""

import grpc
from concurrent import futures
import time
import logging

from protos import inventory_pb2, inventory_pb2_grpc

logging.basicConfig(level=logging.INFO)


class InventoryService(inventory_pb2_grpc.InventoryServiceServicer):
    """
    Mock implementation of the InventoryService gRPC servicer.

    Provides two RPC endpoints:
        • ReserveItems() — Handles stock checks and reservations.
        • ReleaseItems() — Handles compensation (releasing previously reserved stock).

    The behavior is scenario-driven based on SKU keywords in the request.

    Supported simulation patterns:
        - "OUT-OF-STOCK" → Reservation denied, status = OUT_OF_STOCK
        - "NOT-FOUND" → Reservation denied, status = ITEM_NOT_FOUND
        - Otherwise → Reservation successful
    """

    def ReserveItems(self, request, context):
        """
        Handles a gRPC call to reserve items for a given order.

        Args:
            request (inventory_pb2.ReserveItemsRequest):
                Contains order_id and a list of items (SKU, quantity).
            context (grpc.ServicerContext):
                The gRPC context for metadata and control.

        Returns:
            inventory_pb2.ReserveItemsResponse:
                Reservation result with status and reservation_id (if successful).

        Behavior:
            - Logs the request.
            - Checks for special SKU markers to simulate out-of-stock or not-found errors.
            - Generates a timestamp-based reservation ID on success.
        """
        logging.info(f"[IS] Reservierungsanfrage für Order: {request.order_id}")

        # Scenario simulation
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

        # Success case
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