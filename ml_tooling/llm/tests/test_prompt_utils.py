"""Unit tests for prompt generation utilities."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from ml_tooling.llm.prompt_utils import (
    _interpolate_prompt_with_values,
    generate_batch_prompts,
)


@dataclass(frozen=True)
class _SampleItem:
    name: str
    age: int
    optional: str | None = None


class TestGenerateBatchPrompts:
    """Tests for generate_batch_prompts function."""

    def test_generates_formatted_prompts_for_each_batch_item(self):
        """Test that prompts are generated for each item using the mapping and template."""
        # Arrange
        batch = [_SampleItem(name="Alice", age=30), _SampleItem(name="Bob", age=22)]
        prompt_template = "Hello {name}! You are {age} years old."
        template_variable_to_model_field_mapping = {"name": "name", "age": "age"}
        expected = [
            "Hello Alice! You are 30 years old.",
            "Hello Bob! You are 22 years old.",
        ]

        # Act
        result = generate_batch_prompts(
            batch=batch,
            prompt_template=prompt_template,
            template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
        )

        # Assert
        assert result == expected

    def test_returns_empty_list_when_batch_is_empty(self):
        """Test that an empty batch yields an empty list of prompts."""
        # Arrange
        batch: list[_SampleItem] = []

        # Act
        result = generate_batch_prompts(
            batch=batch,
            prompt_template="Hello {name}",
            template_variable_to_model_field_mapping={"name": "name"},
        )

        # Assert
        assert result == []

    def test_raises_attribute_error_when_required_field_is_missing_on_item(self):
        """Test that missing model fields raise AttributeError with a helpful message."""
        # Arrange
        @dataclass(frozen=True)
        class _MissingFieldItem:
            name: str

        batch = [_MissingFieldItem(name="Alice")]
        prompt_template = "Hello {name}! You are {age} years old."
        template_variable_to_model_field_mapping = {"name": "name", "age": "age"}

        # Act / Assert
        with pytest.raises(AttributeError, match="required for template variable 'age'"):
            generate_batch_prompts(
                batch=batch,
                prompt_template=prompt_template,
                template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
            )


class TestInterpolatePromptWithValues:
    """Tests for _interpolate_prompt_with_values function."""

    def test_interpolates_template_using_mapped_attributes(self):
        """Test that mapped attributes are interpolated into the template."""
        # Arrange
        item = _SampleItem(name="Alice", age=30)
        prompt_template = "Hello {name}! You are {age}."
        template_variable_to_model_field_mapping = {"name": "name", "age": "age"}
        expected = "Hello Alice! You are 30."

        # Act
        result = _interpolate_prompt_with_values(
            batch_item=item,
            prompt_template=prompt_template,
            template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
        )

        # Assert
        assert result == expected

    def test_raises_attribute_error_when_attribute_is_missing(self):
        """Test that a missing attribute raises AttributeError with model_field in message."""
        # Arrange
        item = _SampleItem(name="Alice", age=30)
        prompt_template = "Hello {missing}"
        template_variable_to_model_field_mapping = {"missing": "does_not_exist"}

        # Act / Assert
        with pytest.raises(AttributeError, match="neither attribute nor key 'does_not_exist'"):
            _interpolate_prompt_with_values(
                batch_item=item,
                prompt_template=prompt_template,
                template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
            )

    def test_raises_value_error_when_attribute_value_is_none(self):
        """Test that None values raise ValueError with model_field in message."""
        # Arrange
        item = _SampleItem(name="Alice", age=30, optional=None)
        prompt_template = "Optional: {opt}"
        template_variable_to_model_field_mapping = {"opt": "optional"}

        # Act / Assert
        with pytest.raises(ValueError, match="has a value of None for attribute or key 'optional'"):
            _interpolate_prompt_with_values(
                batch_item=item,
                prompt_template=prompt_template,
                template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
            )

    def test_raises_key_error_when_template_references_unprovided_placeholder(self):
        """Test that template placeholders not provided by mapping raise KeyError during format."""
        # Arrange
        item = _SampleItem(name="Alice", age=30)
        prompt_template = "Hello {name} {extra}"
        template_variable_to_model_field_mapping = {"name": "name"}

        # Act / Assert
        with pytest.raises(KeyError, match="extra"):
            _interpolate_prompt_with_values(
                batch_item=item,
                prompt_template=prompt_template,
                template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
            )

    def test_dict_batch_item_is_not_supported_and_raises_attribute_error(self):
        """Test that dict batch items raise AttributeError since interpolation uses getattr."""
        # Arrange
        item = {"name": "Alice"}
        prompt_template = "Hello {name}"
        template_variable_to_model_field_mapping = {"name": "name"}

        # Act / Assert
        with pytest.raises(AttributeError, match="neither attribute nor key 'name'"):
            _interpolate_prompt_with_values(
                batch_item=item,
                prompt_template=prompt_template,
                template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
            )

