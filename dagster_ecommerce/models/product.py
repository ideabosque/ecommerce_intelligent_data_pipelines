"""Pydantic models for WooCommerce product data.

These models define the structure and validation rules for products
before they are synced to WooCommerce.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator


class WooCategory(BaseModel):
    """WooCommerce product category."""

    id: int | None = None
    name: str | None = None
    slug: str | None = None


class WooTag(BaseModel):
    """WooCommerce product tag."""

    id: int | None = None
    name: str | None = None
    slug: str | None = None


class WooImage(BaseModel):
    """WooCommerce product image."""

    id: int | None = None
    src: str | None = None
    name: str | None = None
    alt: str | None = None
    position: int = 0


class WooAttribute(BaseModel):
    """WooCommerce product attribute."""

    id: int | None = None
    name: str
    position: int = 0
    visible: bool = True
    variation: bool = False
    options: list[str] = Field(default_factory=list)


class WooDimensions(BaseModel):
    """WooCommerce product dimensions."""

    length: str | None = None
    width: str | None = None
    height: str | None = None


class WooProduct(BaseModel):
    """WooCommerce product model for sync operations.

    This model represents a product ready for sync to WooCommerce.
    It includes validation rules and type conversions.
    """

    # Required fields
    name: str = Field(..., min_length=1, max_length=255, description="Product name")

    # Identifiers
    sku: str | None = Field(None, max_length=100, description="Stock keeping unit")
    id: int | None = Field(None, description="WooCommerce product ID (read-only)")

    # Type and status
    type: str = Field("simple", description="Product type")
    status: str = Field("publish", description="Product status")
    featured: bool = Field(False, description="Featured product")

    # Pricing
    regular_price: str | Decimal = Field(..., description="Regular price")
    sale_price: str | Decimal | None = Field(None, description="Sale price")
    tax_class: str = Field("", description="Tax class")
    tax_status: str = Field("taxable", description="Tax status")

    # Inventory
    manage_stock: bool = Field(False, description="Manage stock")
    stock_quantity: int | None = Field(None, ge=0, description="Stock quantity")
    stock_status: str = Field("instock", description="Stock status")
    backorders: str = Field("no", description="Allow backorders")
    low_stock_amount: int | None = Field(None, description="Low stock threshold")
    sold_individually: bool = Field(False, description="Sold individually")

    # Shipping
    weight: str | None = Field(None, description="Product weight")
    dimensions: WooDimensions | None = Field(None, description="Product dimensions")
    shipping_class: str | None = Field(None, description="Shipping class slug")
    virtual: bool = Field(False, description="Virtual product")
    downloadable: bool = Field(False, description="Downloadable product")

    # Descriptions
    description: str | None = Field(None, description="Full product description")
    short_description: str | None = Field(None, description="Short description")

    # Categories and tags
    categories: list[WooCategory] | None = Field(None, description="Product categories")
    tags: list[dict[str, Any]] | None = Field(None, description="Product tags")

    # Images
    images: list[WooImage] | None = Field(None, description="Product images")

    # Attributes
    attributes: list[WooAttribute] | None = Field(
        None, description="Product attributes"
    )
    variations: list[int] | None = Field(None, description="Variation IDs")
    parent_id: int | None = Field(None, description="Parent product ID")

    # Metadata
    meta_data: list[dict[str, Any]] | None = Field(None, description="Custom metadata")

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate product type."""
        valid_types = {"simple", "variable", "grouped", "external"}
        if v not in valid_types:
            raise ValueError(f"Invalid product type: {v}. Must be one of {valid_types}")
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate product status."""
        valid_statuses = {"publish", "draft", "pending", "private"}
        if v not in valid_statuses:
            raise ValueError(f"Invalid status: {v}. Must be one of {valid_statuses}")
        return v

    @field_validator("stock_status")
    @classmethod
    def validate_stock_status(cls, v: str) -> str:
        """Validate stock status."""
        valid_statuses = {"instock", "outofstock", "onbackorder"}
        if v not in valid_statuses:
            raise ValueError(
                f"Invalid stock status: {v}. Must be one of {valid_statuses}"
            )
        return v

    @field_validator("regular_price", "sale_price", mode="before")
    @classmethod
    def convert_price_to_string(cls, v) -> str | None:
        """Convert price to string format."""
        if v is None:
            return None
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, (int, float)):
            return str(Decimal(str(v)))
        return str(v)


class SyncResult(BaseModel):
    """Result of a sync operation."""

    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[dict] = Field(default_factory=list)

    @property
    def total_processed(self) -> int:
        """Total number of products processed."""
        return self.created + self.updated + self.skipped

    @property
    def success_rate(self) -> float:
        """Success rate of the sync operation."""
        if self.total_processed == 0:
            return 0.0
        return (self.created + self.updated) / self.total_processed
