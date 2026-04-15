"""Configuration settings for E-Commerce Data Pipelines.

Uses Pydantic Settings for environment variable loading with validation.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # AWS S3 Configuration
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region: str = "us-east-1"
    s3_bucket_name: str = "ecommerce-products"
    s3_prefix: str = "products/incoming/"
    s3_processed_prefix: str = "products/processed/"
    s3_dead_letter_prefix: str = "products/dead_letter/"

    # WooCommerce Configuration
    woocommerce_url: str = ""
    woocommerce_consumer_key: str = ""
    woocommerce_consumer_secret: str = ""
    woocommerce_timeout: int = 30
    woocommerce_requests_per_second: int = 10

    # Knowledge Graph Configuration
    knowledge_graph_url: str = ""
    knowledge_graph_endpoint_id: str = ""
    knowledge_graph_part_id: str = ""
    knowledge_graph_api_key: str = ""
    knowledge_graph_bearer_token: str = ""

    # RFQ Engine Configuration
    rfq_engine_base_url: str = ""  # RFQ Engine base URL
    rfq_engine_endpoint_id: str = ""  # Endpoint ID (URL path segment, e.g., "gpt")
    rfq_engine_part_id: str = ""  # Partition ID for data isolation (Part-Id header)
    rfq_engine_api_key: str = ""  # x-api-key header (required)
    rfq_engine_bearer_token: str = ""  # Optional Authorization: Bearer header
    rfq_engine_timeout: int = 60
    rfq_engine_requests_per_second: int = 5
    rfq_engine_updated_by: str = "dagster-pipeline"  # Actor name for updatedBy field
    rfq_engine_provider_external_id: str = "woocommerce"  # Provider corp identifier
    rfq_engine_default_uom: str = "EA"  # Default unit of measure
    rfq_engine_enable_price_tiers: bool = False  # Enable tier pricing sync (Scope v2)

    # Pipeline Configuration
    max_retries: int = 3
    batch_size: int = 25

    # Logging
    log_level: str = "INFO"

    @property
    def s3_full_bucket_uri(self) -> str:
        """Full S3 bucket URI."""
        return f"s3://{self.s3_bucket_name}"

    @property
    def s3_incoming_uri(self) -> str:
        """Full S3 URI for incoming files."""
        return f"s3://{self.s3_bucket_name}/{self.s3_prefix}"

    @property
    def s3_processed_uri(self) -> str:
        """Full S3 URI for processed files."""
        return f"s3://{self.s3_bucket_name}/{self.s3_processed_prefix}"

    @property
    def s3_dead_letter_uri(self) -> str:
        """Full S3 URI for dead letter files."""
        return f"s3://{self.s3_bucket_name}/{self.s3_dead_letter_prefix}"


# Global settings instance
settings = Settings()
