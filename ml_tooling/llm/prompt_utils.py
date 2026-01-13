from typing import Sequence, Mapping, TypeVar

T = TypeVar("T")


# TODO: add unit testing.
def generate_batch_prompts(
    batch: Sequence[T],
    prompt_template: str,
    template_variable_to_model_field_mapping: Mapping[str, str],
) -> list[str]:
    """
    Generates a list of prompts by filling in the `prompt_template` using values
    from each element in the batch, where each element is a data structure (e.g., a Pydantic model).

    Args:
        batch: A sequence of data structures (e.g., Pydantic models, dataclasses, dicts).
        prompt_template: A string template using Python format placeholders (e.g., "Hello {name}").
        field_mapping: A mapping from string template variable names to the attribute or key
            name on model objects to use for that field.

    Returns:
        A list of formatted prompt strings, one per input in the batch.
    """
    prompts: list[str] = []
    for batch_item in batch:
        prompt = _interpolate_prompt_with_values(
            batch_item=batch_item,
            prompt_template=prompt_template,
            template_variable_to_model_field_mapping=template_variable_to_model_field_mapping,
        )
        prompts.append(prompt)
    return prompts


def _interpolate_prompt_with_values(
    batch_item: object,
    prompt_template: str,
    template_variable_to_model_field_mapping: Mapping[str, str],
) -> str:
    """
    Interpolates a prompt template with a dictionary of values.
    """
    values_to_interpolate: dict[str, str] = {}
    for template_var, model_field in template_variable_to_model_field_mapping.items():
        value = getattr(batch_item, model_field, None)
        if value is None:
            raise AttributeError(
                f"Item {batch_item} has neither attribute nor key '{model_field}' required for template variable '{template_var}'"
            )
        values_to_interpolate[template_var] = value
    return prompt_template.format(**values_to_interpolate)
