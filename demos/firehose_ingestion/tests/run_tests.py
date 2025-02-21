"""Main test runner for the firehose ingestion system."""

import asyncio
from rich.console import Console
from rich.table import Table

from test_basic_functionality import run_all_tests as run_basic_tests
from test_performance import run_performance_tests

console = Console()

async def run_all_tests():
    """Run all test suites and display comprehensive results."""
    console.print("\n[bold yellow]Starting Firehose Ingestion System Tests[/bold yellow]\n")
    
    # Run basic functionality tests
    console.print("[bold cyan]Running Basic Functionality Tests...[/bold cyan]")
    await run_basic_tests()
    
    # Run performance tests
    console.print("\n[bold cyan]Running Performance Tests...[/bold cyan]")
    await run_performance_tests()
    
    console.print("\n[bold green]All tests completed![/bold green]")

if __name__ == "__main__":
    asyncio.run(run_all_tests()) 