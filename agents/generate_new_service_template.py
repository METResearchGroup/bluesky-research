"""CLI app for generating new service templates.

This script provides an interactive interface to generate templates for new services,
following the format required by the CREATE_NEW_SERVICE_PROMPT.
"""

import os
import sys
from typing import List, Tuple


def get_service_name() -> str:
    """Prompt for and return the service name."""
    return input("1. What is the name of the new service?\n> ").strip()


def get_service_details() -> str:
    """Prompt for and return the service details."""
    print(
        "\n2. What are the details of the new service, to be included in the README.md file"
    )
    return input("> ").strip()


def parse_parameter(param: str) -> Tuple[str, str]:
    """Parse a parameter string to extract name and optional default value.

    Args:
        param: Parameter string in format "name" or "name=default_value"

    Returns:
        Tuple of (parameter name, formatted parameter string)
    """
    if "=" not in param:
        return param, param

    name, default = param.split("=", 1)
    # Remove any existing quotes
    default = default.strip("\"'")
    # Format with quotes for string defaults
    formatted_param = f'{name}="{default}"'
    return name, formatted_param


def get_service_parameters() -> List[str]:
    """Prompt for and return the service parameters.

    Parameters can include default values using the format:
    param_name=default_value
    """
    print("\n3. Enter the parameters for the service (space-separated):")
    print("   Use '=' to set default values, e.g., 'param=default_value'")
    raw_params = input("> ").strip().split()

    # Parse and format parameters
    return [parse_parameter(param)[1] for param in raw_params]


def get_service_steps() -> List[str]:
    """Interactively collect and return the service steps."""
    steps = []
    step_num = 1

    while True:
        print("\nCurrent steps:")
        for i, step in enumerate(steps, 1):
            print(f"{i}. {step}")

        print("\nOptions:")
        print("- Enter a new step")
        print("- Enter a step number to edit that step")
        print("- Enter 'done' to finish")

        response = input(f"\nStep {step_num} (or option)> ").strip()

        if response.lower() == "done":
            break

        try:
            # Check if user wants to edit an existing step
            edit_num = int(response)
            if 1 <= edit_num <= len(steps):
                print(f"\nCurrent step {edit_num}: {steps[edit_num-1]}")
                new_step = input("Enter new text for this step:\n> ").strip()
                steps[edit_num - 1] = new_step
            else:
                print("Invalid step number!")
            continue
        except ValueError:
            # Not a number, treat as new step
            steps.append(response)
            step_num += 1

    return steps


def generate_template(
    name: str, details: str, parameters: List[str], steps: List[str]
) -> str:
    """Generate the final template string.

    Args:
        name: Service name
        details: Service details
        parameters: List of service parameters
        steps: List of service steps

    Returns:
        Formatted template string
    """
    template = []

    # Add name section
    template.append("[Name]")
    template.append(name)
    template.append("")

    # Add details section
    template.append("[Details]")
    template.append(details)
    template.append("")

    # Add parameters section
    template.append("[Parameters]")
    for i, param in enumerate(parameters, 1):
        template.append(f"{i}. {param}")
    template.append("")

    # Add steps section
    template.append("[Steps]")
    for i, step in enumerate(steps, 1):
        template.append(f"{i}. {step}")

    return "\n".join(template)


def update_service_templates(service_name: str, template: str) -> None:
    """Update the service_templates.py file with the new template.

    Args:
        service_name: Name of the service
        template: Generated template string
    """
    templates_path = os.path.join(os.path.dirname(__file__), "service_templates.py")

    # Create file if it doesn't exist
    if not os.path.exists(templates_path):
        with open(templates_path, "w") as f:
            f.write('"""Service templates for the CREATE_NEW_SERVICE_PROMPT."""\n\n')
            f.write("MAP_SERVICE_TO_SERVICE_TEMPLATE: dict[str, str] = {}\n")

    # Read existing content
    with open(templates_path, "r") as f:
        content = f.read()

    # Check if MAP_SERVICE_TO_SERVICE_TEMPLATE exists
    if "MAP_SERVICE_TO_SERVICE_TEMPLATE" not in content:
        content += "\nMAP_SERVICE_TO_SERVICE_TEMPLATE: dict[str, str] = {}\n"

    # Format the new template entry
    template_entry = (
        f'\nMAP_SERVICE_TO_SERVICE_TEMPLATE["{service_name}"] = """\n{template}\n"""'
    )

    # Append the new template
    with open(templates_path, "a") as f:
        f.write(template_entry)


def main():
    """Main function to run the template generator."""
    try:
        # Get service information through interactive prompts
        name = get_service_name()
        details = get_service_details()
        parameters = get_service_parameters()
        steps = get_service_steps()

        # Generate template
        template = generate_template(name, details, parameters, steps)

        # Ask if user wants to save to service_templates.py
        print("\nDo you want to add this template to service_templates.py? (y/n)")
        save_template = input("> ").strip().lower() == "y"

        if save_template:
            update_service_templates(name, template)
            print("\nTemplate added to service_templates.py")

        # Print the generated template
        print("\nGenerated Template:")
        print("```")
        print(template)
        print("```")

    except KeyboardInterrupt:
        print("\nTemplate generation cancelled.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError generating template: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
