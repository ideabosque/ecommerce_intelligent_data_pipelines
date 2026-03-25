# WooCommerce Product API Mapping

**Project:** `ecommerce_intelligent_data_pipelines`  
**Last Updated:** 2026-03-22

## Purpose

This document describes how source product fields are transformed into the internal payloads used for WooCommerce sync.

The current implementation parses raw files directly from S3.

## Supported Input Formats

- CSV
- JSON
- NDJSON

## Shopify CSV Input Mapping

The pipeline auto-detects Shopify CSV exports and transforms them before validation. Shopify exports use multi-row format where rows sharing the same `Handle` belong to one product.

### Multi-Row Consolidation

1. Rows are grouped by `Handle`
2. The first row with a non-empty `Title` is the primary row (carries product-level fields)
3. Images are collected from all rows, deduplicated by `Image Src`, and ordered by `Image Position`
4. Variant options (`Option1/2/3 Name` + `Value`) are collected across all rows per option name
5. If any option has multiple distinct values, the product type is set to `variable`; otherwise `simple`

### Shopify to WooCommerce Field Mapping

| Shopify Field | WooCommerce Field | Transformation |
|---|---|---|
| `Title` | `name` | Direct copy |
| `Variant SKU` | `sku` | From primary row |
| `Variant Price` | `regular_price` | Direct copy |
| `Variant Compare At Price` | `regular_price` / `sale_price` | Compare At = `regular_price`, Variant Price = `sale_price` |
| `Body (HTML)` | `description` | HTML passed through |
| `SEO Description` | `short_description` | Direct copy |
| `Status` + `Published` | `status` | See status mapping below |
| `Tags` | `tags` | Comma-separated, vendor appended |
| `Vendor` | `tags` | Appended as "Vendor: {value}" |
| `Product Category` | `categories` | Hierarchy flattened to leaf category |
| `Type` | `categories` | Fallback when Product Category is empty |
| Collected `Image Src` / `Image Alt Text` | `images` | List of dicts with `src`, `alt`, `position` |
| Collected `Option Name` / `Value` | `attributes` | List with `name`, `options`, `variation: true` |
| `Variant Grams` | `weight` | Divided by 1000 (grams to kg) |
| `Gift Card` = true | `virtual` | Set to `true` |
| `Variant Requires Shipping` = false | `virtual` | Set to `true` |
| `Variant Taxable` | `tax_status` | `"taxable"` if true, `"none"` if false |
| `Handle` | `meta_data` | Preserved as `shopify_handle` |

### Status Mapping

| Shopify Status | Published | WooCommerce Status |
|---|---|---|
| `active` | `true` | `publish` |
| `active` | `false` | `draft` |
| `draft` | any | `draft` |
| `archived` | any | `private` |

### Backward Compatibility

If the incoming data does not have `Handle` and `Title` columns, the transformation is skipped and records pass through unchanged. This allows WooCommerce-native CSVs and JSON files to work without changes.

## Core Mapping Rules (WooCommerce Native)

| Source Field | Target Field | Rule |
|---|---|---|
| `name` | `name` | Required |
| `sku` | `sku` | Used as the primary reconciliation key when present |
| `price` / `regular_price` | `regular_price` | At least one must be present |
| `sale_price` | `sale_price` | Used when present |
| `stock_quantity` | `stock_quantity` | Converted to integer when provided |
| `stock_status` | `stock_status` | Must be `instock`, `outofstock`, or `onbackorder` |
| `type` | `type` | Defaults to `simple`; must be WooCommerce-supported |
| `status` | `status` | Defaults to `publish`; must be WooCommerce-supported |
| `featured` | `featured` | Parsed from bool-like values |
| `manage_stock` | `manage_stock` | Parsed from bool-like values |
| `description` | `description` | Passed through when provided |
| `short_description` | `short_description` | Passed through when provided |
| `categories` | `categories` | Parsed from comma-separated strings or lists |
| `tags` | `tags` | Parsed from comma-separated strings or lists |
| `images` | `images` | Parsed from comma-separated strings or lists |
| `attributes` | `attributes` | Parsed from list input |

## Pricing Logic

The validation layer uses this pricing rule:

- when both active price and sale price are present, the higher value becomes `regular_price`
- the lower value becomes `sale_price`
- otherwise `regular_price` is set from the available main price field

## Boolean Parsing

The pipeline treats these string values as `True`:

- `true`
- `1`
- `yes`
- `on`

Everything else follows normal Python truthiness or resolves to `False` for empty values.

## Matching Strategy

WooCommerce product matching is currently:

1. match by SKU when provided
2. create a new product when no SKU match exists

There is no implemented fallback today for slug matching, title matching, or source-handle matching.

## Invalid Records

Invalid records are not sent to WooCommerce. They are written to the dead-letter queue with structured error messages.

## Notes

- Keep source fields preserved long enough for debugging and reprocessing
- Keep this document focused on the implemented S3 parsing and WooCommerce mapping behavior
