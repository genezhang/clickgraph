"""CLI for cg-schema - ClickGraph Schema Designer."""

import sys
import argparse
from pathlib import Path

import requests
from rich.console import Console
from rich.table import Table

from cg_schema import __version__
from cg_schema.analyzer import SchemaAnalyzer
from cg_schema.output import generate_yaml, print_suggestions

console = Console()


def get_server_url(server: str) -> str:
    """Ensure server URL has http:// prefix."""
    if not server.startswith("http://") and not server.startswith("https://"):
        server = f"http://{server}"
    return server.rstrip("/")


def cmd_introspect(args):
    """Introspect database via ClickGraph server."""
    server = get_server_url(args.server)
    
    console.print(f"[bold]Connecting to ClickGraph server:[/bold] {server}")
    
    # Call the introspect endpoint
    url = f"{server}/schemas/introspect"
    
    payload = {"database": args.database}
    
    try:
        console.print(f"[dim]Calling:[/dim] POST {url}")
        console.print(f"[dim]Payload:[/dim] {payload}")
        
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        tables = data.get("tables", [])
        
        console.print(f"[green]✓[/green] Found {len(tables)} tables")
        
    except requests.exceptions.ConnectionError:
        console.print(f"[red]✗[/red] Cannot connect to server at {server}")
        console.print("[yellow]Is ClickGraph server running?[/yellow]")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        console.print(f"[red]✗[/red] HTTP error: {e}")
        if e.response.text:
            console.print(f"[dim]Response:[/dim] {e.response.text}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        sys.exit(1)
    
    # Run GLiNER analysis
    console.print("\n[bold]Running ML analysis...[/bold]")
    
    analyzer = SchemaAnalyzer()
    suggestions = analyzer.analyze(tables)
    
    # Print suggestions
    print_suggestions(suggestions, console)
    
    # Generate output
    if args.output:
        yaml_content = generate_yaml(tables, suggestions)
        Path(args.output).write_text(yaml_content)
        console.print(f"\n[green]✓[/green] Schema saved to: {args.output}")
    
    return suggestions


def cmd_interactive(args):
    """Interactive mode - review and refine suggestions."""
    server = get_server_url(args.server)
    
    console.print("[bold cyan]Interactive Schema Designer[/bold cyan]")
    console.print(f"Server: {server}")
    console.print(f"Database: {args.database}")
    console.print("\n[yellow]Interactive mode not yet implemented[/yellow]")
    console.print("Use 'introspect' command with --output to generate schema.yaml")
    
    # TODO: Implement interactive mode
    # - Show suggestions one by one
    # - Allow user to edit/confirm
    # - Save final schema


def cmd_push(args):
    """Push schema to ClickGraph server."""
    server = get_server_url(args.server)
    
    schema_path = Path(args.schema)
    if not schema_path.exists():
        console.print(f"[red]✗[/red] Schema file not found: {args.schema}")
        sys.exit(1)
    
    url = f"{server}/schemas"
    
    try:
        with open(schema_path) as f:
            schema_content = f.read()
        
        console.print(f"[dim]Pushing schema to:[/dim] {url}")
        
        response = requests.post(url, data=schema_content, timeout=30)
        response.raise_for_status()
        
        console.print(f"[green]✓[/green] Schema pushed successfully")
        
    except requests.exceptions.ConnectionError:
        console.print(f"[red]✗[/red] Cannot connect to server at {server}")
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        console.print(f"[red]✗[/red] HTTP error: {e}")
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="cg-schema",
        description="ClickGraph Schema Designer - ML-powered schema discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  cg-schema introspect --server localhost:8080 --db mydb
  cg-schema introspect -s localhost:8080 -d mydb -o schema.yaml
  cg-schema push schema.yaml -s localhost:8080
        """
    )
    
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Introspect command
    introspect_parser = subparsers.add_parser(
        "introspect",
        help="Introspect database and generate schema suggestions"
    )
    introspect_parser.add_argument(
        "-s", "--server",
        default="localhost:8080",
        help="ClickGraph server address (default: localhost:8080)"
    )
    introspect_parser.add_argument(
        "-d", "--database",
        required=True,
        help="Database name to introspect"
    )
    introspect_parser.add_argument(
        "-o", "--output",
        help="Output YAML file path"
    )
    introspect_parser.set_defaults(func=cmd_introspect)
    
    # Interactive command
    interactive_parser = subparsers.add_parser(
        "interactive",
        help="Interactive mode to review suggestions"
    )
    interactive_parser.add_argument(
        "-s", "--server",
        default="localhost:8080",
        help="ClickGraph server address"
    )
    interactive_parser.add_argument(
        "-d", "--database",
        required=True,
        help="Database name"
    )
    interactive_parser.set_defaults(func=cmd_interactive)
    
    # Push command
    push_parser = subparsers.add_parser(
        "push",
        help="Push schema to ClickGraph server"
    )
    push_parser.add_argument(
        "schema",
        help="Path to schema.yaml file"
    )
    push_parser.add_argument(
        "-s", "--server",
        default="localhost:8080",
        help="ClickGraph server address"
    )
    push_parser.set_defaults(func=cmd_push)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == "__main__":
    main()
