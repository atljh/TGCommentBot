import asyncio
import random
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from telethon.tl.functions.channels import GetParticipantRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import (
    FloodWaitError,
    ChannelPrivateError,
    UserNotParticipantError,
    MsgIdInvalidError,
    MessageIdInvalidError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    InviteHashInvalidError,
    InviteHashExpiredError,
    ChannelsTooMuchError,
    UsersTooMuchError,
    InviteRequestSentError,
    SlowModeWaitError,
)
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from .client import BaseThon
from .database import Database
from .parser import LinkParser, ParsedLink
from .utils import log_error, log_info, log_comment, move_account_to_status_folder, get_status_folder


class CommentResult:
    def __init__(self, phone: str, success: bool, error: Optional[str] = None, comment: Optional[str] = None):
        self.phone = phone
        self.success = success
        self.error = error
        self.comment = comment


class Commenter:
    def __init__(
        self,
        database: Database,
        delay_range: Tuple[int, int] = (5, 15),
        max_comments_per_day: int = 20,
        sessions_dir: Optional[Path] = None,
        console=None
    ):
        self.db = database
        self.delay_range = delay_range
        self.max_comments_per_day = max_comments_per_day
        self.sessions_dir = sessions_dir
        self.console = console
        self.results: List[CommentResult] = []
        self.moved_accounts: List[Tuple[str, str]] = []

    async def check_subscription(self, client: BaseThon, channel_id: int) -> bool:
        try:
            await client.client(GetParticipantRequest(channel_id, "me"))
            return True
        except UserNotParticipantError:
            return False
        except Exception:
            return False

    async def join_channel(
        self,
        client: BaseThon,
        channel_id: int,
        invite_hash: str = None,
        phone: Optional[str] = None,
        is_private: bool = False,
    ) -> str:
        phone_label = phone or getattr(client, "phone", "UNKNOWN")

        if is_private and not invite_hash:
            status = "CHANNEL_PRIVATE"
            msg = f"{status}: invite link is required to join a private channel"
            log_error("join", phone_label, msg)
            if self.console:
                self.console.print(
                    f"  [yellow]! {phone_label}: CHANNEL_PRIVATE - private link without invite; use --invite[/yellow]"
                )
            return status

        try:
            if invite_hash:
                await client.client(ImportChatInviteRequest(invite_hash))
            else:
                entity = await client.client.get_entity(channel_id)
                await client.client(JoinChannelRequest(entity))
            return "OK"
        except FloodWaitError as e:
            if self.console:
                self.console.print(
                    f"  [yellow]! {phone_label}: FLOOD_WAIT while joining, {e.seconds}s[/yellow]"
                )
            raise
        except InviteHashInvalidError:
            status = "INVITE_INVALID"
            log_error("join", phone_label, status)
            if self.console:
                self.console.print(f"  [red]x {phone_label}: INVITE_INVALID[/red]")
            return status
        except InviteHashExpiredError:
            status = "INVITE_EXPIRED"
            log_error("join", phone_label, status)
            if self.console:
                self.console.print(f"  [red]x {phone_label}: INVITE_EXPIRED[/red]")
            return status
        except ChannelsTooMuchError:
            status = "CHANNELS_TOO_MUCH"
            log_error("join", phone_label, status)
            if self.console:
                self.console.print(f"  [red]x {phone_label}: CHANNELS_TOO_MUCH[/red]")
            return status
        except UsersTooMuchError:
            status = "USERS_TOO_MUCH"
            log_error("join", phone_label, status)
            if self.console:
                self.console.print(f"  [red]x {phone_label}: USERS_TOO_MUCH[/red]")
            return status
        except InviteRequestSentError:
            status = "INVITE_REQUEST_SENT"
            log_error("join", phone_label, status)
            if self.console:
                self.console.print(f"  [yellow]! {phone_label}: INVITE_REQUEST_SENT - waiting for admin approval[/yellow]")
            return status
        except UserBannedInChannelError:
            status = "BANNED_IN_CHANNEL"
            log_error("join", phone_label, status)
            if self.console:
                self.console.print(f"  [yellow]! {phone_label}: BANNED_IN_CHANNEL[/yellow]")
            return status
        except ChannelPrivateError:
            status = "CHANNEL_PRIVATE"
            log_error("join", phone_label, status)
            if self.console:
                self.console.print(f"  [yellow]! {phone_label}: CHANNEL_PRIVATE[/yellow]")
            return status
        except Exception as e:
            status = "JOIN_ERROR"
            msg = f"{status}: {type(e).__name__}: {str(e)}"
            log_error("join", phone_label, msg)
            if self.console:
                self.console.print(f"  [yellow]! {phone_label}: JOIN_ERROR - {str(e)[:50]}[/yellow]")
            return status

    async def send_comment(
        self,
        client: BaseThon,
        channel_id: int,
        message_id: int,
        text: str
    ) -> bool:
        entity = await client.client.get_entity(channel_id)
        await client.client.send_message(
            entity=entity,
            message=text,
            comment_to=message_id
        )
        return True

    async def resolve_channel(self, client: BaseThon, parsed: ParsedLink) -> int:
        if parsed.channel_id != 0:
            return parsed.channel_id

        entity = await client.client.get_entity(parsed.username)
        return entity.id

    async def process_account(
        self,
        account: Dict[str, Any],
        channel_id: int,
        message_id: int,
        comments: List[str],
        post_link: str,
        parsed: ParsedLink,
        semaphore: asyncio.Semaphore,
        invite_hash: str = None
    ) -> CommentResult:
        phone = account["phone"]
        comment_text = random.choice(comments)

        async with semaphore:
            session_file = Path(account["session_file"]) if account.get("session_file") else None
            json_file = Path(account["json_file"]) if account.get("json_file") else None

            if json_file and json_file.exists():
                from .utils import json_read
                json_data = json_read(json_file)
            else:
                json_data = {}

            client = BaseThon(session_file=session_file, json_data=json_data)

            try:
                await client.connect()

                if parsed.channel_id == 0:
                    actual_channel_id = await self.resolve_channel(client, parsed)
                else:
                    actual_channel_id = channel_id

                is_subscribed = await self.check_subscription(client, actual_channel_id)
                if not is_subscribed:
                    join_status = await self.join_channel(
                        client,
                        actual_channel_id,
                        invite_hash,
                        phone,
                        parsed.is_private,
                    )
                    if join_status != "OK":
                        log_error("comment", phone, join_status)
                        return CommentResult(phone, False, join_status, comment_text)
                    log_info(f"JOIN | {phone} | channel={actual_channel_id}")
                    if self.console:
                        self.console.print(f"  [green]+ {phone}: JOIN | channel={actual_channel_id}[/green]")
                    await self.db.update_subscription(account["id"], actual_channel_id, True)

                await self.send_comment(client, actual_channel_id, message_id, comment_text)

                await self.db.log_comment(
                    account["id"],
                    post_link,
                    actual_channel_id,
                    message_id,
                    comment_text
                )

                log_comment(phone, str(actual_channel_id), message_id, comment_text, True)

                delay = random.uniform(*self.delay_range)
                await asyncio.sleep(delay)

                return CommentResult(phone, True, comment=comment_text)

            except FloodWaitError as e:
                error_msg = f"FLOOD:{e.seconds}s"
                log_error("comment", phone, error_msg)
                if self.console:
                    self.console.print(f"  [yellow]! {phone}: {error_msg}[/yellow]")
                return CommentResult(phone, False, error_msg, comment_text)

            except SlowModeWaitError as e:
                error_msg = f"SLOWMODE:{e.seconds}s"
                log_error("comment", phone, error_msg)
                if self.console:
                    self.console.print(f"  [yellow]! {phone}: {error_msg}[/yellow]")
                return CommentResult(phone, False, error_msg, comment_text)

            except ChannelPrivateError:
                await self.db.update_subscription(account["id"], actual_channel_id, False)
                log_error("comment", phone, "CHANNEL_PRIVATE")
                if self.console:
                    self.console.print(f"  [yellow]! {phone}: CHANNEL_PRIVATE[/yellow]")
                return CommentResult(phone, False, "CHANNEL_PRIVATE", comment_text)

            except UserBannedInChannelError:
                log_error("comment", phone, "BANNED_IN_CHANNEL")
                if self.console:
                    self.console.print(f"  [yellow]! {phone}: BANNED_IN_CHANNEL[/yellow]")
                return CommentResult(phone, False, "BANNED_IN_CHANNEL", comment_text)

            except ChatWriteForbiddenError:
                log_error("comment", phone, "CHAT_WRITE_FORBIDDEN")
                if self.console:
                    self.console.print(f"  [yellow]! {phone}: CHAT_WRITE_FORBIDDEN - comments disabled[/yellow]")
                return CommentResult(phone, False, "CHAT_WRITE_FORBIDDEN", comment_text)

            except (MsgIdInvalidError, MessageIdInvalidError):
                log_error("comment", phone, "MSG_ID_INVALID")
                if self.console:
                    self.console.print(f"  [yellow]! {phone}: MSG_ID_INVALID[/yellow]")
                return CommentResult(phone, False, "MSG_ID_INVALID", comment_text)

            except Exception as e:
                error_msg = str(e)
                error_lower = error_msg.lower()

                if "message" in error_lower and "invalid" in error_lower:
                    log_error("comment", phone, "MSG_ID_INVALID")
                    if self.console:
                        self.console.print(f"  [yellow]! {phone}: MSG_ID_INVALID[/yellow]")
                    return CommentResult(phone, False, "MSG_ID_INVALID", comment_text)

                log_error("comment", phone, error_msg)

                is_ban = any(x in error_lower for x in ["banned", "deactivated", "spam", "restrict"])

                if is_ban:
                    await self.db.set_account_active(account["id"], False)
                    if self.console:
                        self.console.print(f"  [red]x {phone}: {error_msg[:40]}[/red]")

                    if self.sessions_dir:
                        status = "BANNED" if "banned" in error_lower else "SPAM" if "spam" in error_lower else "RESTRICTED"
                        moved = move_account_to_status_folder(
                            session_file, json_file, status,
                            self.sessions_dir, None
                        )
                        if moved:
                            folder = get_status_folder(status)
                            self.moved_accounts.append((phone, folder))
                            if self.console:
                                self.console.print(f"    [dim]-> moved to sessions_{folder}/[/dim]")
                else:
                    if self.console:
                        self.console.print(f"  [yellow]! {phone}: {error_msg[:40]}[/yellow]")

                return CommentResult(phone, False, error_msg[:50], comment_text)

            finally:
                await client.disconnect()

    async def check_accounts(self, accounts: List[Dict[str, Any]], threads: int) -> List[Dict[str, Any]]:
        semaphore = asyncio.Semaphore(max(1, threads))

        async def check_one(account: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            phone = account["phone"]
            session_file = Path(account["session_file"]) if account.get("session_file") else None
            json_file = Path(account["json_file"]) if account.get("json_file") else None

            if json_file and json_file.exists():
                from .utils import json_read
                json_data = json_read(json_file)
            else:
                json_data = {}

            client = BaseThon(session_file=session_file, json_data=json_data)

            try:
                async with semaphore:
                    check_result = await client.check()

                if check_result != "OK":
                    await self.db.set_account_active(account["id"], False)
                    log_error("check", phone, check_result)

                    if self.console:
                        self.console.print(f"  [red]x {phone}: {check_result}[/red]")

                    if self.sessions_dir and get_status_folder(check_result):
                        moved = move_account_to_status_folder(
                            session_file, json_file, check_result,
                            self.sessions_dir, None
                        )
                        if moved:
                            folder = get_status_folder(check_result)
                            self.moved_accounts.append((phone, folder))
                            if self.console:
                                self.console.print(f"    [dim]-> moved to sessions_{folder}/[/dim]")
                    return None
                else:
                    if self.console:
                        self.console.print(f"  [green]+ {phone}: OK[/green]")
                    return account

            except Exception as e:
                log_error("check", phone, str(e))
                if self.console:
                    self.console.print(f"  [red]x {phone}: {str(e)[:30]}[/red]")
                return None

            finally:
                await client.disconnect()

        results = await asyncio.gather(*(check_one(acc) for acc in accounts))
        return [acc for acc in results if acc is not None]

    @staticmethod
    def parse_invite_hash(invite_link: str) -> Optional[str]:
        if not invite_link:
            return None
        invite_link = invite_link.strip()
        if "/+" in invite_link:
            return invite_link.split("/+")[-1]
        if "joinchat/" in invite_link:
            return invite_link.split("joinchat/")[-1]
        return None

    async def run(
        self,
        post_link: str,
        comments: List[str],
        count: int,
        threads: int = 5,
        dry_run: bool = False,
        invite_link: str = None
    ) -> List[CommentResult]:
        self.results = []

        parsed = LinkParser.parse(post_link)
        if not parsed:
            raise ValueError(f"Invalid link: {post_link}")

        invite_hash = self.parse_invite_hash(invite_link)

        channel_id = parsed.channel_id
        message_id = parsed.message_id

        accounts = await self.db.get_available_accounts(
            channel_id if channel_id != 0 else 0,
            message_id,
            count,
            self.max_comments_per_day
        )

        if not accounts:
            if self.console:
                self.console.print("[yellow]No available accounts[/yellow]")
            return []

        if self.console:
            self.console.print(f"\n[bold]Checking {len(accounts)} accounts...[/bold]")

        valid_accounts = await self.check_accounts(accounts, threads)

        if not valid_accounts:
            if self.console:
                self.console.print("[yellow]No valid accounts after check[/yellow]")
            return []

        if self.console:
            self.console.print(f"\n[bold]Sending comments...[/bold]")

        if dry_run:
            for acc in valid_accounts:
                comment_text = random.choice(comments)
                self.results.append(CommentResult(acc["phone"], True, "DRY_RUN", comment_text))
            return self.results

        semaphore = asyncio.Semaphore(threads)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[cyan]{task.completed}/{task.total}"),
        ) as progress:
            task = progress.add_task(f"Comments", total=len(valid_accounts))

            async def process_with_progress(account):
                result = await self.process_account(
                    account, channel_id, message_id, comments, post_link, parsed, semaphore, invite_hash
                )
                self.results.append(result)
                progress.advance(task)
                return result

            await asyncio.gather(
                *[process_with_progress(acc) for acc in valid_accounts],
                return_exceptions=True
            )

        return self.results

    def get_stats(self) -> Dict[str, Any]:
        success = sum(1 for r in self.results if r.success)
        failed = len(self.results) - success

        errors = {}
        for r in self.results:
            if not r.success and r.error:
                error_type = r.error.split(":")[0] if ":" in r.error else r.error
                errors[error_type] = errors.get(error_type, 0) + 1

        return {
            "total": len(self.results),
            "success": success,
            "failed": failed,
            "errors": errors
        }
