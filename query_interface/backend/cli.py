#!/usr/bin/env python3
"""CLI tool for querying Athena with natural language queries."""

import os

import requests
import typer
from rich.console import Console
from rich.table import Table

console = Console()

# Default API URL - can be overridden via QUERY_INTERFACE_API_URL env var
DEFAULT_API_URL = os.getenv("QUERY_INTERFACE_API_URL", "http://127.0.0.1:8000")


def main(
    query_text: str = typer.Argument(..., help="Natural language query to execute"),
    show_sql: bool = typer.Option(
        False, "--show-sql", help="Show the generated SQL query"
    ),
    api_url: str = typer.Option(
        DEFAULT_API_URL,
        "--api-url",
        help="URL of the query interface API server",
        envvar="QUERY_INTERFACE_API_URL",
    ),
):
    """Execute a natural language query against Athena.

    The query will be automatically converted to SQL, executed, and results
    will be limited to 10 rows for safety.

    Example:
        python cli.py "how many generated feeds do we have?"
    """
    try:
        # Make request to API
        console.print("[cyan]Sending query to API...[/cyan]")
        response = requests.post(
            f"{api_url}/query",
            json={"query": query_text},
            timeout=300,  # 5 minute timeout for long-running queries
        )

        # Handle HTTP errors
        if response.status_code == 400:
            error_detail = response.json().get(
                "detail", "Query cannot be answered with SQL"
            )
            console.print(f"[red]Error:[/red] {error_detail}")
            console.print(
                "\n[dim]The query cannot be converted to SQL. Please try a different query.[/dim]"
            )
            raise typer.Exit(code=1)
        elif response.status_code == 500:
            error_detail = response.json().get("detail", "Server error occurred")
            console.print(f"[red]Error:[/red] {error_detail}")
            raise typer.Exit(code=1)
        elif not response.ok:
            console.print(
                f"[red]Error:[/red] HTTP {response.status_code}: {response.text}"
            )
            raise typer.Exit(code=1)

        # Parse response
        data = response.json()
        sql_query = data.get("sql_query", "")
        results = data.get("results", [])
        row_count = data.get("row_count", 0)

        # Show SQL if requested
        if show_sql:
            console.print("\n[bold]Generated SQL:[/bold]")
            console.print(f"[dim]{sql_query}[/dim]\n")

        # Display results
        if not results:
            console.print("[yellow]No results found.[/yellow]")
            return

        # Create a rich table for pretty display
        table = Table(show_header=True, header_style="bold magenta")

        # Get columns from first result
        if results:
            columns = list(results[0].keys())
            for col in columns:
                table.add_column(str(col), overflow="fold")

            # Add rows
            for row in results:
                table.add_row(
                    *[str(val) if val is not None else "" for val in row.values()]
                )

        console.print(f"\n[bold green]Results ({row_count} rows):[/bold green]")
        console.print(table)

        if row_count >= 10:
            console.print("\n[yellow]Note: Results are limited to 10 rows.[/yellow]")

    except requests.exceptions.ConnectionError:
        console.print(f"[red]Error:[/red] Could not connect to API server at {api_url}")
        console.print(
            "\n[dim]Make sure the API server is running. Start it with:[/dim]"
        )
        console.print("[dim]  python -m query_interface.backend.app[/dim]")
        raise typer.Exit(code=1)
    except requests.exceptions.Timeout:
        console.print(
            "[red]Error:[/red] Request timed out. The query may be taking too long."
        )
        raise typer.Exit(code=1)
    except requests.exceptions.RequestException as e:
        console.print(f"[red]Error:[/red] Request failed: {str(e)}")
        raise typer.Exit(code=1)
    except KeyError as e:
        console.print(f"[red]Error:[/red] Unexpected response format: missing key {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(code=1)


app = typer.Typer()
app.command()(main)

if __name__ == "__main__":
    app()
