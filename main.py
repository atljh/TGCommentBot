import argparse
import asyncio
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table

from config import get_settings
from src.database import Database
from src.commenter import Commenter
from src.session_loader import SessionLoader
from src.parser import LinkParser
from src.utils import parse_proxy_string, json_read, json_write, log_info


console = Console()


def parse_delay(delay_str: str) -> tuple:
    if "-" in delay_str:
        parts = delay_str.split("-")
        return int(parts[0]), int(parts[1])
    val = int(delay_str)
    return val, val


def load_proxies(proxies_file: Path) -> list:
    if not proxies_file.exists():
        return []
    proxies = []
    with open(proxies_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                proxy = parse_proxy_string(line)
                if proxy:
                    proxies.append(proxy)
    return proxies


async def sync_sessions(db: Database, loader: SessionLoader, proxies: list):
    proxy_index = 0
    synced = 0

    for session_file, json_file, json_data in loader.find_sessions():
        phone = json_data.get("phone", session_file.stem)
        proxy = json_data.get("proxy")

        if not proxy and proxies:
            proxy = proxies[proxy_index % len(proxies)]
            proxy_index += 1
            json_data["proxy"] = proxy
            json_write(json_file, json_data)
            console.print(f"  [cyan]~ Assigned proxy to: {phone}[/cyan]")

        proxy_str = str(proxy) if proxy else None

        existing = await db.get_account(phone)
        if not existing:
            await db.add_account(
                phone=phone,
                session_file=str(session_file),
                json_file=str(json_file),
                proxy=proxy_str
            )
            console.print(f"  [green]+ Added account: {phone}[/green]")
            synced += 1

    return synced


async def main():
    parser = argparse.ArgumentParser(
        description="TG Comments - Telegram Comment Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --link https://t.me/channel/123 --comments "great" "nice" "cool" --count 5
  python main.py -l https://t.me/channel/123 -m "awesome" "super" -c 3 -t 2 -d 10-20
  python main.py --sync
  python main.py --stats
        """
    )

    parser.add_argument(
        "--link", "-l",
        help="Post link (https://t.me/channel/123 or https://t.me/c/123456789/123)"
    )
    parser.add_argument(
        "--comments", "-m",
        nargs="+",
        help="Comments to post (space-separated, use quotes for multi-word)"
    )
    parser.add_argument(
        "--count", "-c",
        type=int,
        default=10,
        help="Number of comments to post (default: 10)"
    )
    parser.add_argument(
        "--threads", "-t",
        type=int,
        default=5,
        help="Number of parallel threads (default: 5)"
    )
    parser.add_argument(
        "--delay", "-d",
        default="5-15",
        help="Delay range in seconds (default: 5-15)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test run without sending comments"
    )
    parser.add_argument(
        "--invite", "-i",
        help="Invite link for private channels (https://t.me/+XXX)"
    )
    parser.add_argument(
        "--config",
        help="Path to config.yaml"
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Sync sessions from sessions/ folder to database"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show account statistics"
    )
    parser.add_argument(
        "--clear-history",
        action="store_true",
        help="Clear comment history for specified post"
    )

    args = parser.parse_args()

    try:
        settings = get_settings(args.config)
    except FileNotFoundError:
        console.print("[red]Error: config.yaml not found![/red]")
        console.print("Copy config/config.yaml.example to config.yaml and configure it")
        sys.exit(1)

    db = Database(settings.database)
    await db.connect()

    loader = SessionLoader(settings.sessions_dir)
    proxies = load_proxies(settings.proxies_file)

    if proxies:
        console.print(f"[dim]Loaded {len(proxies)} proxies[/dim]")

    if args.sync:
        synced = await sync_sessions(db, loader, proxies)
        console.print(f"[green]Sync completed! Added: {synced}[/green]")
        await db.close()
        return

    if args.stats:
        accounts = await db.get_all_accounts(active_only=False)

        table = Table(title="Account Statistics")
        table.add_column("Phone", style="cyan")
        table.add_column("Active", style="green")
        table.add_column("Today", style="yellow")
        table.add_column("Proxy")
        table.add_column("Last Used")

        for acc in accounts:
            proxy_info = "-"
            if acc.get("proxy"):
                try:
                    p = eval(acc["proxy"]) if isinstance(acc["proxy"], str) else acc["proxy"]
                    if isinstance(p, dict):
                        proxy_info = f"{p.get('addr', '?')}:{p.get('port', '?')}"
                except:
                    proxy_info = "yes"

            table.add_row(
                acc["phone"],
                "Yes" if acc["is_active"] else "No",
                str(acc["comments_today"] or 0),
                proxy_info,
                str(acc["last_used"] or "Never")[:19]
            )

        console.print(table)
        console.print(f"\nTotal: {len(accounts)} | Active: {sum(1 for a in accounts if a['is_active'])}")
        await db.close()
        return

    if not args.link:
        parser.print_help()
        await db.close()
        return

    if not args.comments:
        console.print("[red]Error: --comments is required[/red]")
        await db.close()
        sys.exit(1)

    if not LinkParser.is_valid(args.link):
        console.print(f"[red]Error: Invalid link format: {args.link}[/red]")
        await db.close()
        sys.exit(1)

    if args.clear_history:
        parsed = LinkParser.parse(args.link)
        await db.clear_comments(parsed.channel_id, parsed.message_id)
        console.print(f"[green]Cleared comment history for post[/green]")
        await db.close()
        return

    delay_range = parse_delay(args.delay)

    comments_preview = ", ".join(f'"{c}"' for c in args.comments[:3])
    if len(args.comments) > 3:
        comments_preview += f" ... (+{len(args.comments) - 3} more)"

    log_info(f"START | link={args.link} | comments={len(args.comments)} | count={args.count}")

    console.print(f"\n[bold]TG Comments[/bold]")
    console.print(f"Link: {args.link}")
    if args.invite:
        console.print(f"Invite: {args.invite}")
    console.print(f"Comments: {comments_preview}")
    console.print(f"Count: {args.count}")
    console.print(f"Threads: {args.threads}")
    console.print(f"Delay: {delay_range[0]}-{delay_range[1]}s")
    if args.dry_run:
        console.print("[yellow]DRY RUN MODE[/yellow]")
    console.print()

    await sync_sessions(db, loader, proxies)

    commenter = Commenter(
        database=db,
        delay_range=delay_range,
        max_comments_per_day=settings.max_comments_per_day,
        sessions_dir=settings.sessions_dir,
        console=console
    )

    results = await commenter.run(
        post_link=args.link,
        comments=args.comments,
        count=args.count,
        threads=args.threads,
        dry_run=args.dry_run,
        invite_link=args.invite
    )

    stats = commenter.get_stats()

    log_info(f"END | success={stats['success']} | failed={stats['failed']} | total={stats['total']}")

    console.print()
    console.print(f"[green]Success: {stats['success']}[/green]")
    console.print(f"[red]Failed: {stats['failed']}[/red]")

    if commenter.moved_accounts:
        console.print(f"\n[yellow]Moved accounts: {len(commenter.moved_accounts)}[/yellow]")
        for phone, folder in commenter.moved_accounts:
            console.print(f"  - {phone} -> sessions_{folder}/")

    if stats['errors']:
        console.print("\nErrors:")
        for error, count in stats['errors'].items():
            console.print(f"  - {error}: {count}")

    # Show successful comments
    successful = [r for r in results if r.success]
    if successful:
        console.print("\n[green]Sent comments:[/green]")
        for r in successful[:10]:
            comment_short = r.comment[:40] + "..." if len(r.comment) > 40 else r.comment
            console.print(f"  [dim]{r.phone}: {comment_short}[/dim]")
        if len(successful) > 10:
            console.print(f"  [dim]... and {len(successful) - 10} more[/dim]")

    await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[red]Script stopped by user[/red]")
