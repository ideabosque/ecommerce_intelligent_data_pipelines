"""Sensors package."""

from dagster_ecommerce.sensors.s3_sensor import s3_product_file_sensor

__all__ = ["s3_product_file_sensor"]
