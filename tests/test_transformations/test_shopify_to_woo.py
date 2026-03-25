"""Tests for Shopify to WooCommerce transformation."""

from dagster_ecommerce.transformations.shopify_to_woo import (
    consolidate_shopify_rows,
    is_shopify_format,
    map_shopify_to_woo,
)


def _make_shopify_row(
    handle="test-product",
    title="Test Product",
    price="29.99",
    sku="TEST-001",
    **overrides,
):
    """Helper to create a Shopify CSV row dict."""
    row = {
        "Handle": handle,
        "Title": title,
        "Body (HTML)": "<p>Description</p>",
        "Vendor": "TestVendor",
        "Product Category": "",
        "Type": "",
        "Tags": "tag1, tag2",
        "Published": "true",
        "Option1 Name": "Title",
        "Option1 Value": "Default Title",
        "Option2 Name": "",
        "Option2 Value": "",
        "Option3 Name": "",
        "Option3 Value": "",
        "Variant SKU": sku,
        "Variant Grams": "500.0",
        "Variant Inventory Policy": "deny",
        "Variant Fulfillment Service": "manual",
        "Variant Price": price,
        "Variant Compare At Price": "",
        "Variant Requires Shipping": "true",
        "Variant Taxable": "true",
        "Variant Barcode": "",
        "Image Src": "https://example.com/img1.jpg",
        "Image Position": "1",
        "Image Alt Text": "primary_image",
        "Gift Card": "false",
        "SEO Title": "Test Product",
        "SEO Description": "Short desc",
        "Variant Weight Unit": "lb",
        "Variant Tax Code": "",
        "Cost per item": "",
        "Status": "active",
    }
    row.update(overrides)
    return row


class TestIsShopifyFormat:
    def test_detects_shopify(self):
        rows = [_make_shopify_row()]
        assert is_shopify_format(rows) is True

    def test_rejects_woocommerce(self):
        rows = [{"name": "Product", "sku": "SKU-1", "price": "10"}]
        assert is_shopify_format(rows) is False

    def test_empty_list(self):
        assert is_shopify_format([]) is False


class TestConsolidateShopifyRows:
    def test_single_row_product(self):
        rows = [_make_shopify_row()]
        result = consolidate_shopify_rows(rows)
        assert len(result) == 1
        assert result[0]["Title"] == "Test Product"
        assert len(result[0]["_images"]) == 1

    def test_multi_row_images(self):
        rows = [
            _make_shopify_row(
                **{"Image Src": "https://example.com/img1.jpg", "Image Position": "1"}
            ),
            _make_shopify_row(
                title="",
                sku="",
                price="",
                **{
                    "Image Src": "https://example.com/img2.jpg",
                    "Image Position": "2",
                    "Image Alt Text": "alt2",
                },
            ),
            _make_shopify_row(
                title="",
                sku="",
                price="",
                **{
                    "Image Src": "https://example.com/img3.jpg",
                    "Image Position": "3",
                    "Image Alt Text": "alt3",
                },
            ),
        ]
        result = consolidate_shopify_rows(rows)
        assert len(result) == 1
        assert len(result[0]["_images"]) == 3
        assert result[0]["_images"][0]["src"] == "https://example.com/img1.jpg"
        assert result[0]["_images"][2]["position"] == 3

    def test_deduplicates_images(self):
        rows = [
            _make_shopify_row(**{"Image Src": "https://example.com/img1.jpg"}),
            _make_shopify_row(
                title="", sku="", price="",
                **{"Image Src": "https://example.com/img1.jpg"},
            ),
        ]
        result = consolidate_shopify_rows(rows)
        assert len(result[0]["_images"]) == 1

    def test_multi_row_variants(self):
        rows = [
            _make_shopify_row(
                **{
                    "Option1 Name": "Color",
                    "Option1 Value": "Red",
                    "Variant SKU": "TEST-RED",
                }
            ),
            _make_shopify_row(
                title="",
                price="29.99",
                **{
                    "Option1 Name": "Color",
                    "Option1 Value": "Blue",
                    "Variant SKU": "TEST-BLUE",
                    "Image Src": "",
                },
            ),
        ]
        result = consolidate_shopify_rows(rows)
        assert len(result) == 1
        assert result[0]["_is_variable"] is True
        assert "Color" in result[0]["_option_values"]
        assert "Red" in result[0]["_option_values"]["Color"]
        assert "Blue" in result[0]["_option_values"]["Color"]
        assert len(result[0]["_variant_skus"]) == 2

    def test_simple_product_not_variable(self):
        rows = [_make_shopify_row()]
        result = consolidate_shopify_rows(rows)
        assert result[0]["_is_variable"] is False

    def test_multiple_products(self):
        rows = [
            _make_shopify_row(handle="product-a", title="Product A", sku="A-001"),
            _make_shopify_row(handle="product-b", title="Product B", sku="B-001"),
        ]
        result = consolidate_shopify_rows(rows)
        assert len(result) == 2

    def test_skips_rows_without_handle(self):
        rows = [
            _make_shopify_row(handle=""),
            _make_shopify_row(handle="valid", title="Valid"),
        ]
        result = consolidate_shopify_rows(rows)
        assert len(result) == 1


class TestMapShopifyToWoo:
    def test_basic_field_mapping(self):
        rows = [_make_shopify_row()]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)

        assert result["name"] == "Test Product"
        assert result["sku"] == "TEST-001"
        assert result["regular_price"] == "29.99"
        assert result["description"] == "<p>Description</p>"
        assert result["short_description"] == "Short desc"

    def test_status_active_published(self):
        rows = [_make_shopify_row(**{"Status": "active", "Published": "true"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["status"] == "publish"

    def test_status_active_not_published(self):
        rows = [_make_shopify_row(**{"Status": "active", "Published": "false"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["status"] == "draft"

    def test_status_draft(self):
        rows = [_make_shopify_row(**{"Status": "draft", "Published": "true"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["status"] == "draft"

    def test_status_archived(self):
        rows = [_make_shopify_row(**{"Status": "archived", "Published": "true"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["status"] == "private"

    def test_compare_at_price_sets_sale_price(self):
        rows = [
            _make_shopify_row(
                price="19.99",
                **{"Variant Compare At Price": "29.99"},
            )
        ]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["regular_price"] == "29.99"
        assert result["sale_price"] == "19.99"

    def test_weight_grams_to_kg(self):
        rows = [_make_shopify_row(**{"Variant Grams": "1500.0"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["weight"] == "1.5"

    def test_zero_weight_excluded(self):
        rows = [_make_shopify_row(**{"Variant Grams": "0.0"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert "weight" not in result

    def test_gift_card_sets_virtual(self):
        rows = [_make_shopify_row(**{"Gift Card": "true"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["virtual"] is True

    def test_no_shipping_sets_virtual(self):
        rows = [_make_shopify_row(**{"Variant Requires Shipping": "false"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["virtual"] is True

    def test_tags_include_vendor(self):
        rows = [_make_shopify_row()]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert "Vendor: TestVendor" in result["tags"]
        assert "tag1" in result["tags"]

    def test_images_mapped(self):
        rows = [_make_shopify_row()]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert len(result["images"]) == 1
        assert result["images"][0]["src"] == "https://example.com/img1.jpg"

    def test_variable_type_with_options(self):
        rows = [
            _make_shopify_row(
                **{
                    "Option1 Name": "Size",
                    "Option1 Value": "S",
                    "Variant SKU": "TEST-S",
                }
            ),
            _make_shopify_row(
                title="",
                price="29.99",
                **{
                    "Option1 Name": "Size",
                    "Option1 Value": "M",
                    "Variant SKU": "TEST-M",
                    "Image Src": "",
                },
            ),
        ]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["type"] == "variable"
        assert len(result["attributes"]) == 1
        assert result["attributes"][0]["name"] == "Size"
        assert "S" in result["attributes"][0]["options"]
        assert "M" in result["attributes"][0]["options"]

    def test_tax_status_taxable(self):
        rows = [_make_shopify_row(**{"Variant Taxable": "true"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["tax_status"] == "taxable"

    def test_tax_status_none(self):
        rows = [_make_shopify_row(**{"Variant Taxable": "false"})]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["tax_status"] == "none"

    def test_handle_preserved_in_metadata(self):
        rows = [_make_shopify_row()]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["meta_data"][0]["key"] == "shopify_handle"
        assert result["meta_data"][0]["value"] == "test-product"

    def test_source_key_preserved(self):
        rows = [_make_shopify_row()]
        rows[0]["__source_key"] = "products/incoming/export.csv"
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["__source_key"] == "products/incoming/export.csv"

    def test_category_hierarchy_uses_leaf(self):
        rows = [
            _make_shopify_row(
                **{"Product Category": "Home & Garden > Kitchen > Cookware"}
            )
        ]
        consolidated = consolidate_shopify_rows(rows)[0]
        result = map_shopify_to_woo(consolidated)
        assert result["categories"] == "Cookware"
