#!/usr/bin/env python3
"""CLI tool for querying Athena with natural language queries."""

import typer
from rich.console import Console
from rich.table import Table

from query_interface.backend.agents.router import can_convert_to_sql
from query_interface.backend.agents.sql_generator import generate_sql
from query_interface.backend.services.query_executor import execute_query

console = Console()


def main(
    query_text: str = typer.Argument(..., help="Natural language query to execute"),
    show_sql: bool = typer.Option(
        False, "--show-sql", help="Show the generated SQL query"
    ),
):
    """Execute a natural language query against Athena.

    The query will be automatically converted to SQL, executed, and results
    will be limited to 10 rows for safety.

    Example:
        python cli.py "how many generated feeds do we have?"
    """
    try:
        # Step 1: Router agent - check if query can be converted
        console.print("[cyan]Checking if query can be converted to SQL...[/cyan]")
        can_convert, reason = can_convert_to_sql(query_text)

        if not can_convert:
            console.print(f"[red]Error:[/red] {reason}")
            console.print(
                "\n[dim]The query cannot be converted to SQL. Please try a different query.[/dim]"
            )
            raise typer.Exit(code=1)

        # Step 2: Generate SQL
        console.print("[cyan]Generating SQL query...[/cyan]")
        sql = generate_sql(query_text)

        if show_sql:
            console.print("\n[bold]Generated SQL:[/bold]")
            console.print(f"[dim]{sql}[/dim]\n")

        # Step 3: Execute query
        console.print("[cyan]Executing query against Athena...[/cyan]")
        df = execute_query(sql)

        # Step 4: Display results
        if df.empty:
            console.print("[yellow]No results found.[/yellow]")
            return

        # Create a rich table for pretty display
        table = Table(show_header=True, header_style="bold magenta")

        # Add columns
        for col in df.columns:
            table.add_column(str(col), overflow="fold")

        # Add rows (limit to 10, though it should already be limited)
        for _, row in df.head(10).iterrows():
            table.add_row(*[str(val) if val is not None else "" for val in row.values])

        console.print(f"\n[bold green]Results ({len(df)} rows):[/bold green]")
        console.print(table)

        if len(df) >= 10:
            console.print("\n[yellow]Note: Results are limited to 10 rows.[/yellow]")

    except ValueError as e:
        console.print(f"[red]Configuration Error:[/red] {str(e)}")
        console.print(
            "\n[dim]Please ensure OPENAI_API_KEY is set in your environment.[/dim]"
        )
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(code=1)


app = typer.Typer()
app.command()(main)

if __name__ == "__main__":
    app()
