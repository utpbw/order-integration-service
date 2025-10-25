from pydantic import BaseModel, Field
from typing import List

class OrderItem(BaseModel):
    sku: str
    quantity: int = Field(..., gt=0) # gt=0 bedeutet "greater than 0"

class NewOrderRequest(BaseModel):
    orderId: str
    paymentToken: str
    totalAmount: float # Wird in Cent umgerechnet für PS
    currency: str
    items: List[OrderItem]