"""
models.py — Data Models for Order Processing

This module defines the data structures used for order creation and validation.
It uses Pydantic models to ensure type safety and automatic validation of incoming data.

Models:
    - OrderItem: Represents a single item in an order.
    - NewOrderRequest: Represents the complete order request payload received from the OMS.
"""

from pydantic import BaseModel, Field
from typing import List

class OrderItem(BaseModel):
    """
    Represents a single product item in an order.

    Attributes:
        sku (str): The unique product identifier (Stock Keeping Unit).
        quantity (int): The quantity of the product to order. Must be greater than zero.
    """
    sku: str
    quantity: int = Field(..., gt=0) # gt=0 bedeutet "greater than 0"

class NewOrderRequest(BaseModel):
    """
    Represents a new order request created by the Order Management System (OMS).

    This model contains all required data for initiating the order processing workflow.

    Attributes:
        orderId (str): Unique identifier for the order.
        paymentToken (str): Token used for payment authorization.
        totalAmount (float): Total amount of the order in major currency units (converted to cents for the Payment Service).
        currency (str): ISO 4217 currency code (e.g., 'EUR', 'USD').
        items (List[OrderItem]): List of items included in the order.
    """
    orderId: str
    paymentToken: str
    totalAmount: float
    currency: str
    items: List[OrderItem]