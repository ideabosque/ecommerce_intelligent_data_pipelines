"""Tests for product processing assets."""


from dagster_ecommerce.assets.product_processing import (
    parse_attributes,
    parse_bool,
    parse_categories,
    parse_dimensions,
    parse_images,
    parse_tags,
    validate_product,
)


class TestValidateProduct:
    """Tests for product validation."""

    def test_validate_product_valid(self):
        """Test validating a valid product."""
        record = {
            "name": "Test Product",
            "sku": "TEST-001",
            "price": "29.99",
            "stock_quantity": 100,
            "stock_status": "instock",
        }
        seen_skus = set()

        product, errors = validate_product(record, seen_skus)

        assert product is not None
        assert len(errors) == 0
        assert product.name == "Test Product"
        assert product.sku == "TEST-001"
        assert "TEST-001" in seen_skus

    def test_validate_product_missing_name(self):
        """Test validating a product missing required name."""
        record = {
            "sku": "TEST-001",
            "price": "29.99",
        }
        seen_skus = set()

        product, errors = validate_product(record, seen_skus)

        assert product is None
        assert len(errors) > 0
        assert any("name" in e.lower() for e in errors)

    def test_validate_product_missing_price(self):
        """Test validating a product missing required price."""
        record = {
            "name": "Test Product",
            "sku": "TEST-001",
        }
        seen_skus = set()

        product, errors = validate_product(record, seen_skus)

        assert product is None
        assert len(errors) > 0
        assert any("price" in e.lower() for e in errors)

    def test_validate_product_duplicate_sku(self):
        """Test validating a product with duplicate SKU."""
        record = {
            "name": "Test Product",
            "sku": "TEST-001",
            "price": "29.99",
        }
        seen_skus = {"TEST-001"}  # SKU already exists

        product, errors = validate_product(record, seen_skus)

        assert product is None
        assert any("duplicate" in e.lower() for e in errors)

    def test_validate_product_invalid_stock_status(self):
        """Test validating a product with invalid stock_status."""
        record = {
            "name": "Test Product",
            "price": "29.99",
            "stock_status": "invalid_status",
        }
        seen_skus = set()

        product, errors = validate_product(record, seen_skus)

        assert product is None
        assert any("stock_status" in e.lower() for e in errors)


class TestParseFunctions:
    """Tests for parsing helper functions."""

    def test_parse_bool_string_true(self):
        """Test parsing boolean from string 'true'."""
        assert parse_bool("true") is True
        assert parse_bool("TRUE") is True
        assert parse_bool("True") is True

    def test_parse_bool_string_yes(self):
        """Test parsing boolean from string 'yes'."""
        assert parse_bool("yes") is True
        assert parse_bool("YES") is True

    def test_parse_bool_int(self):
        """Test parsing boolean from int."""
        assert parse_bool(1) is True
        assert parse_bool(0) is False

    def test_parse_dimensions_from_record(self):
        """Test parsing dimensions from record."""
        record = {
            "length": "10",
            "width": "5",
            "height": "2",
        }

        result = parse_dimensions(record)

        assert result is not None
        assert result.length == "10"
        assert result.width == "5"
        assert result.height == "2"

    def test_parse_dimensions_from_nested(self):
        """Test parsing dimensions from nested dict."""
        record = {
            "dimensions": {
                "length": "10",
                "width": "5",
                "height": "2",
            }
        }

        result = parse_dimensions(record)

        assert result is not None
        assert result.length == "10"

    def test_parse_categories_string(self):
        """Test parsing categories from string."""
        result = parse_categories("Electronics, Gadgets")

        assert len(result) == 2
        assert result[0].name == "Electronics"
        assert result[1].name == "Gadgets"

    def test_parse_categories_list_of_strings(self):
        """Test parsing categories from list of strings."""
        result = parse_categories(["Electronics", "Gadgets"])

        assert len(result) == 2
        assert result[0].name == "Electronics"

    def test_parse_categories_list_of_dicts(self):
        """Test parsing categories from list of dicts."""
        result = parse_categories([
            {"id": 1, "name": "Electronics"},
            {"id": 2, "name": "Gadgets"},
        ])

        assert len(result) == 2
        assert result[0].id == 1
        assert result[1].name == "Gadgets"

    def test_parse_tags_string(self):
        """Test parsing tags from string."""
        result = parse_tags("new, featured")

        assert len(result) == 2
        assert result[0]["name"] == "new"
        assert result[1]["name"] == "featured"

    def test_parse_images_string(self):
        """Test parsing images from string."""
        result = parse_images("http://example.com/img1.jpg, http://example.com/img2.jpg")

        assert len(result) == 2
        assert result[0].src == "http://example.com/img1.jpg"
        assert result[1].src == "http://example.com/img2.jpg"

    def test_parse_attributes_list(self):
        """Test parsing attributes from list."""
        result = parse_attributes([
            {"name": "Color", "options": ["Red", "Blue"], "visible": True},
            {"name": "Size", "options": ["S", "M", "L"], "variation": True},
        ])

        assert len(result) == 2
        assert result[0].name == "Color"
        assert "Red" in result[0].options
        assert result[1].variation is True
