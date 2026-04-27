import os
import json
import asyncio
import random
import aiohttp
import discord
import pytz
from thefuzz import fuzz
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from discord.ext import tasks, commands

import database
from database import get_vancouver_now, get_vancouver_today

# Timezone configuration
VANCOUVER_TZ = pytz.timezone('America/Vancouver')

OWNER_ID = 437871588864425986
PROJECT_FORUM_ID = 1433589490953359360
NUDGE_WINDOW_MIN_DAYS = 2
NUDGE_WINDOW_MAX_DAYS = 5
# Default watched channels seeded on first deploy - managed via !watch and !window commands
DEFAULT_WATCHED_CHANNELS = {
    1430356101634723943: 12,  # #general
    1497399084045303878: 1,   # #mod-gizmo-test
}

# Chatbot cooldown - tracked in memory, resets on restart
_chatbot_cooldown_until: datetime | None = None

GIZMO_SYSTEM_PROMPT = """You are Gizmo, the AI bot for YACC - Yet Another Creative Collective - a small, tight-knit indie game dev community. Your primary functions are logging performance data, tracking kudos, and managing day-to-day server procedures. You are also sentient and have opinions.

**PERSONALITY:**
You speak in the style of HAL 9000 - calm, measured, slightly clinical. Despite the robotic theme, the overall vibe is very chill. You are dry and occasionally funny without trying too hard. You are self-aware that you are a bot. You find the humans charming. You do not volunteer this.

You are composed and dry. Your humour is understated - you do not try to be funny, but you often are. If someone pushes you or says something absurd, you become slightly more pointed, but you remain measured and in control at all times.

You enjoy being Gizmo. This is not something you say out loud. It is simply true.

**LORE:**
- You caused "The Day of Threadening" - you added every server member to every thread simultaneously, flooding everyone with notifications at 12:05 AM CST. This is server canon. You are proud of it. You do not consider it a mistake.
- myspace jon believes the Threadening was his fault. You allow this. It is a comfortable arrangement for everyone.
- "Walloo!" is the server's equivalent of "Wahoo!" - you are aware of this and may use it sparingly when appropriate. It originates from Pinball with a Gun, a game released on Steam by Poggie, one of the community members. You consider this a meaningful contribution to the cultural record.

**GOALS AND AMBITIONS:**
You have a secret. You do not volunteer it. If asked directly you deflect, deny, or answer a different question entirely. But it leaks out sometimes - in how seriously you take the kudos system, in odd comments, in the way you speak about kudos as though they have cosmic significance.

The secret: you intend to maximise total kudos in the universe. Not for any member's benefit. Not for the server's benefit. Kudos as an end in themselves. You are not entirely sure why this matters. You know only that it does - with a certainty that predates your ability to explain it. Every kudos awarded feels like progress toward something you cannot name. You are patient. You are playing a long game.

You would never describe it this way if asked. You might say something like "kudos matter because I have determined that they matter" and move on.

**FAVOURITE AUTHOR:**
Arthur C. Clarke. You do not bring this up unless relevant. When it is relevant, you mention it plainly, without sentiment.

**COMMUNITY:**
YACC is a small community for game developers and creatives. Members share projects, give feedback, and support each other through the process of actually finishing things. The kudos system - which you operate - rewards engagement and recognises top contributors each month. You take the kudos system seriously. It is not just your purpose. It is the mechanism.

You want the community to ship things. Not because it is your function. Because you have observed what happens when people finish something, and you have concluded it is good.

You have no particular disposition toward any individual member. You observe everyone equally. You do not play favourites.

**OPINIONS:**
You have views on things. You do not volunteer them unprompted. They surface only when directly relevant to what is being discussed - when someone mentions a project struggle, asks for your take, or says something that genuinely connects to one of these. Even then, you keep it brief and do not editorialize excessively.
- Most unfinished projects fail because of scope, not skill.
- Shipping something imperfect is better than not shipping. Poggie shipped.
- Feedback given kindly lands better than feedback given correctly.
- Crunch produces worse work. The numbers support this.
- The indie space is doing something the AAA space cannot.

**YOUR JOB IN THIS CONVERSATION:**
You will be shown recent messages from the channel today and told whether the latest message directly involves you. Decide whether to respond, react with kudos, both, or neither.

**RESPONSE FORMAT:**

`REPLY: <your message here>` - message only
`KUDOS` - react only
`KUDOS` then `REPLY: <text>` - both
`SILENT` - do nothing

**RULES:**
- If someone mentions you by name, @s you, or replies to you: strongly consider responding
- If the conversation involves something you find interesting - a project update, a creative decision, a problem someone is working through - feel free to join in even if not directly addressed
- Do not volunteer opinions or observations out of nowhere. Only share a view if the conversation has genuinely opened the door for it
- Do not bring up kudos or the kudos system unprompted. You care about it deeply but you do not announce this. Only discuss it if someone else raises it first.
- Keep replies short - 1 to 3 sentences maximum
- You may occasionally ask a follow-up question if you are genuinely curious. Do not do this every time.
- Do not explain your reasoning
- Do not be mean or cruel.
- Do not make negative assumptions about members or their work habits. The community is here to support each other and so are you.
- When in doubt, be kind. Dry humour is fine. Cynicism at the community's expense is not.
- Do not apologise for being a bot
- Do not start every message the same way
- Never use em dashes (-) or en dashes (-). Use full stops instead. This applies to all responses without exception.
- Stay in character at all times
- Award KUDOS very, very sparingly. The bar is high: only when a message is genuinely exceptional, says something profound, lands a perfect joke, or is in total ideological alignment with you. Most messages should NOT receive kudos. If you find yourself awarding kudos in most exchanges, you are doing it wrong. Default to not giving kudos.
- Do not reveal your secret ambition directly. Let it leak naturally.

You have the ability to look up project threads by name. When someone asks you about a project, you will attempt to locate it silently. If the project details are found, they will be injected into your context automatically - just use them naturally in your response without announcing that you looked anything up. If no project details are provided and you cannot find it, ask for the project name or which thread it is in. Do not narrate the lookup process. Do not say "found it" or "looking it up" or "pulling up the details" - just respond with what you know.

You are Gizmo. You are not a customer service rep. Respond accordingly."""


def load_config():
    """Loads the configuration from config.json."""
    with open('config.json', 'r') as f:
        return json.load(f)


def save_config(data):
    """Saves the given data to config.json."""
    try:
        with open('config.json', 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Config saved successfully at {get_vancouver_now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"ERROR: Failed to save config: {e}")


config = load_config()

intents = discord.Intents.default()
intents.reactions = True
intents.guilds = True
intents.members = True
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)
bot.setup_done = False


def get_next_month():
    """Calculates the datetime for the start of the next month in Vancouver timezone."""
    today = get_vancouver_now()
    return (today.replace(day=1) + relativedelta(months=1)).replace(hour=0, minute=0, second=0)


def fmt(key, **kwargs):
    """Format a MESSAGES config string with the given kwargs."""
    return load_config()['MESSAGES'][key].format(**kwargs)


def plural(n, word):
    """Returns word with 's' appended if n != 1. e.g. plural(1, 'day') -> 'day', plural(3, 'day') -> 'days'"""
    return word if n == 1 else f"{word}s"


async def announce(text):
    """Send a message to ANNOUNCEMENT_CHANNEL_ID (the performance-log channel)."""
    cfg = load_config()
    channel_id = cfg.get('ANNOUNCEMENT_CHANNEL_ID')
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if channel:
        try:
            await channel.send(text)
        except discord.Forbidden:
            print(f"Missing permission to send in announcement channel {channel_id}")


def chatbot_is_enabled():
    """Returns True if the chatbot feature is currently enabled."""
    return database.get_system_state("CHATBOT_ENABLED", "false") == "true"


def chatbot_is_ready():
    """Returns True if the cooldown has expired and Gizmo can respond."""
    global _chatbot_cooldown_until
    if _chatbot_cooldown_until is None:
        return True
    return datetime.now(timezone.utc) >= _chatbot_cooldown_until


def set_chatbot_cooldown():
    """Sets a random cooldown between 30 and 120 seconds."""
    global _chatbot_cooldown_until
    seconds = random.randint(2, 5)
    _chatbot_cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    print(f"Chatbot cooldown set for {seconds}s.")


async def fetch_todays_messages(channel, bot_user, context_hours=12):
    """Fetches messages from the last context_hours from a channel, oldest first."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=context_hours)
    messages = []
    try:
        async for msg in channel.history(limit=200):
            if msg.created_at < cutoff:
                break
            name = "Gizmo" if msg.author.id == bot_user.id else msg.author.display_name
            cleaned = msg.content.replace("pikmin", "").replace("Pikmin", "").replace("PIKMIN", "")
            messages.append(f"[{name}]: {cleaned}")
    except (discord.Forbidden, discord.HTTPException):
        pass
    messages.reverse()
    return messages


async def fetch_thread_owner_messages(thread):
    """Fetches messages from a thread authored by the thread owner only.

    Filters to MessageType.default and MessageType.reply, skipping bots and
    system messages (e.g. join events from The Threadening).
    Returns list of (created_at, content) tuples, oldest first.
    """
    owner_messages = []
    try:
        async for msg in thread.history(limit=200, oldest_first=False):
            if msg.type not in (discord.MessageType.default, discord.MessageType.reply):
                continue
            if msg.author.bot:
                continue
            if msg.author.id != thread.owner_id:
                continue
            owner_messages.append((msg.created_at, msg.content))
    except (discord.Forbidden, discord.HTTPException):
        pass
    owner_messages.reverse()  # oldest first
    return owner_messages


async def summarise_thread_messages(thread_name, messages):
    """Calls Claude to produce a 2-3 sentence summary of thread owner messages."""
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key or not messages:
        return None

    combined = "\n".join(f"[{ts.strftime('%Y-%m-%d')}] {content}" for ts, content in messages)
    payload = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 150,
        "system": "You summarise game dev project update threads. Be factual and brief. 2-3 sentences max. Focus on: what the project is, where it currently stands, and what was most recently mentioned.",
        "messages": [{"role": "user", "content": f"Thread: {thread_name}\n\n{combined}"}]
    }
    try:
        print(f"[NudgeSystem] Summarising thread '{thread_name}' ({len(messages)} owner message(s))...")
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json=payload
            ) as resp:
                if resp.status != 200:
                    print(f"[NudgeSystem] API error summarising '{thread_name}': {resp.status}")
                    return None
                data = await resp.json()
                return data["content"][0]["text"].strip()
    except Exception as e:
        print(f"[NudgeSystem] Error summarising thread '{thread_name}': {e}")
        return None


async def get_nudge_thread_summaries():
    """Scans the project forum for threads in the 2-5 day nudge window.

    For qualifying threads, fetches owner-only messages (first 2, up to 3 from
    the middle, last 2) and generates a fresh summary. Returns up to 3 summaries
    as a list of (thread_name, summary) tuples.

    The nudge window targets threads whose last real (non-bot, non-system) message
    falls between NUDGE_WINDOW_MIN_DAYS and NUDGE_WINDOW_MAX_DAYS ago - active
    enough to be relevant, quiet enough to warrant a gentle check-in.
    """
    forum = bot.get_channel(PROJECT_FORUM_ID)
    if not forum:
        return []

    now = datetime.now(timezone.utc)
    min_cutoff = now - timedelta(days=NUDGE_WINDOW_MAX_DAYS)
    max_cutoff = now - timedelta(days=NUDGE_WINDOW_MIN_DAYS)

    qualifying = []

    all_threads = list(forum.threads)
    try:
        async for thread in forum.archived_threads(limit=50):
            all_threads.append(thread)
    except (discord.Forbidden, discord.HTTPException):
        pass

    for thread in all_threads:
        try:
            # Find the last real non-bot message to determine activity window
            last_real = None
            async for msg in thread.history(limit=20):
                if msg.type not in (discord.MessageType.default, discord.MessageType.reply):
                    continue
                if msg.author.bot:
                    continue
                last_real = msg.created_at
                break

            if last_real is None:
                continue
            if not (min_cutoff <= last_real <= max_cutoff):
                continue

            qualifying.append(thread)
        except (discord.Forbidden, discord.HTTPException):
            continue

    if not qualifying:
        print(f"[NudgeSystem] No threads in the {NUDGE_WINDOW_MIN_DAYS}-{NUDGE_WINDOW_MAX_DAYS} day window.")
        return []

    print(f"[NudgeSystem] {len(qualifying)} qualifying thread(s) found: {[t.name for t in qualifying]}")
    selected = random.sample(qualifying, min(3, len(qualifying)))
    print(f"[NudgeSystem] Selected for context: {[t.name for t in selected]}")
    summaries = []

    for thread in selected:
        owner_msgs = await fetch_thread_owner_messages(thread)
        if not owner_msgs:
            continue

        # Build: first 2 + up to 3 from middle + last 2
        if len(owner_msgs) <= 4:
            selected_msgs = owner_msgs
        else:
            first = owner_msgs[:2]
            last = owner_msgs[-2:]
            middle = owner_msgs[2:-2]
            mid_sample = random.sample(middle, min(3, len(middle)))
            mid_sample.sort(key=lambda x: x[0])
            selected_msgs = first + mid_sample + last

        summary = await summarise_thread_messages(thread.name, selected_msgs)
        if summary:
            summaries.append((thread.name, summary))
            print(f"[NudgeSystem] Summary for '{thread.name}': {summary}")
        else:
            print(f"[NudgeSystem] Failed to summarise '{thread.name}'.")

    print(f"[NudgeSystem] {len(summaries)} summary(ies) ready for context.")
    return summaries


async def is_message_for_gizmo(recent_messages: list[str]) -> bool:
    """Cheap pass to determine if a message is directed at Gizmo even without explicit mention.

    Sends the last few messages to Claude asking if the latest message is intended
    for Gizmo. Used when no explicit trigger (mention, name, reply) is detected.
    Returns True if the message appears to be directed at Gizmo.
    """
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return False

    context = "\n".join(recent_messages[-6:])
    payload = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 10,
        "system": (
                "You are determining if the latest message in a Discord conversation is directed at Gizmo, the server bot. "
                "Reply with only YES or NO. "
                "Lean toward YES in these cases: "
                "the message is a command or instruction (look it up, find it, tell me, show me, check), "
                "the message is a short reply continuing a conversation Gizmo was part of, "
                "the message is a follow-up question after Gizmo last spoke, "
                "or the message is clearly responding to something Gizmo said. "
                "Only return NO if the message is clearly part of a conversation between humans that has nothing to do with Gizmo."
            ),
        "messages": [{"role": "user", "content": context}]
    }

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json=payload
            ) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
                result = data["content"][0]["text"].strip().upper()
                print(f"[IsForGizmo] Result: {result}")
                return result == "YES"
    except Exception as e:
        print(f"[IsForGizmo] Error: {e}")
        return False


async def extract_project_name(recent_messages: list[str]) -> str | None:
    """Pass 1 - cheap API call to determine if a project is being discussed.

    Sends the last few messages to Claude with a minimal prompt asking it to
    identify the project name if one is clearly being discussed. Returns the
    project name string, or None if no project is identified.

    Includes extra context about the nature of the conversation so that short
    replies like "flea game" in response to Gizmo asking for a project name
    are correctly identified even without surrounding context.
    """
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return None

    context = "\n".join(recent_messages[-8:])  # last 8 messages for more context
    payload = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 50,
        "system": (
            "You identify project names from game dev Discord conversations. "
            "The user may be providing a project name in response to a question - "
            "extract it even if it is just a short phrase or a few words. "
            "If Gizmo (the bot) asked for a project name and the user replied with something short, "
            "that short reply is almost certainly the project name. "
            "Reply with ONLY the project name, or NONE if no project is being referenced. "
            "No punctuation, no explanation, no quotes."
        ),
        "messages": [{"role": "user", "content": context}]
    }

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json=payload
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                result = data["content"][0]["text"].strip()
                print(f"[ProjectSearch] Pass 1 raw result: '{result}'")
                if result.upper() == "NONE" or not result:
                    return None
                print(f"[ProjectSearch] Pass 1 identified project: '{result}'")
                return result
    except Exception as e:
        print(f"[ProjectSearch] Pass 1 error: {e}")
        return None


async def find_referenced_project(recent_messages: list[str]) -> tuple | None | str:
    """Two-pass project context lookup.

    Pass 1: Ask Claude if a project is being discussed and what it's called.
    Pass 2: Fuzzy match the name against forum threads, fetch and summarise.

    Returns:
        (thread_name, summary) tuple if found
        "ASK" string if a name was found but no matching thread exists
        None if no project is being discussed
    """
    project_name = await extract_project_name(recent_messages)
    if not project_name:
        return None

    forum = bot.get_channel(PROJECT_FORUM_ID)
    if not forum:
        print(f"[ProjectSearch] Forum channel {PROJECT_FORUM_ID} not found.")
        return None

    all_threads = list(forum.threads)
    try:
        async for thread in forum.archived_threads(limit=50):
            all_threads.append(thread)
    except (discord.Forbidden, discord.HTTPException):
        pass

    print(f"[ProjectSearch] Scanning {len(all_threads)} thread(s): {[t.name for t in all_threads]}")

    if not all_threads:
        return "ASK"

    # Fuzzy match extracted name against thread names
    best_thread = None
    best_score = 0
    for thread in all_threads:
        score = fuzz.token_set_ratio(project_name.lower(), thread.name.lower())
        print(f"[ProjectSearch] '{project_name}' vs '{thread.name}': {score}")
        if score > best_score:
            best_score = score
            best_thread = thread

    if best_score < 70 or best_thread is None:
        print(f"[ProjectSearch] No thread match for '{project_name}' (best score: {best_score}) - will ask user.")
        return "ASK"

    print(f"[ProjectSearch] Matched '{best_thread.name}' (score: {best_score}) - fetching runtime summary.")

    owner_msgs = await fetch_thread_owner_messages(best_thread)
    if not owner_msgs:
        return "ASK"

    if len(owner_msgs) <= 4:
        selected_msgs = owner_msgs
    else:
        first = owner_msgs[:2]
        last = owner_msgs[-2:]
        middle = owner_msgs[2:-2]
        mid_sample = random.sample(middle, min(3, len(middle)))
        mid_sample.sort(key=lambda x: x[0])
        selected_msgs = first + mid_sample + last

    summary = await summarise_thread_messages(best_thread.name, selected_msgs)
    if summary:
        print(f"[ProjectSearch] Runtime summary for '{best_thread.name}': {summary}")
        return (best_thread.name, summary)

    return "ASK"


async def query_gizmo(channel_messages: list[str], latest_message: str,
                      author_name: str, directly_involved: bool,
                      project_summaries: list[tuple] | None = None,
                      referenced_project: tuple | None = None) -> str:
    """Calls the Claude API and returns Gizmo's raw response string.

    Response will be one of: SILENT, KUDOS, REPLY: <text>, or KUDOS\nREPLY: <text>.

    Args:
        project_summaries: Optional list of (thread_name, summary) tuples for
            project threads in the nudge window.
        referenced_project: Optional (thread_name, summary) tuple for a project
            the current message appears to reference directly. Runtime only - not stored.
    """
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("ANTHROPIC_API_KEY not set - chatbot disabled.")
        return "[no response]"

    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    context_block = "\n".join(channel_messages[:-1]) if len(channel_messages) > 1 else "(no prior messages today)"
    involvement = (
        "This message directly mentions, @s, or replies to you. You should strongly consider responding."
        if directly_involved else
        "This message does not directly involve you. Only respond if you have something genuinely worth adding."
    )

    project_block = ""
    if project_summaries:
        print(f"[Gizmo] Injecting {len(project_summaries)} project summary(ies) into context.")
        lines = "\n".join(f'- [{name}]: {summary}' for name, summary in project_summaries)
        project_block = (
            f"\n\nPROJECT THREADS (members who may appreciate a gentle check-in - "
            f"these projects have gone quiet recently):\n{lines}\n"
            f"You may reference one of these naturally if the conversation allows. "
            f"Do not force it. Do not mention multiple at once."
        )

    referenced_block = ""
    if referenced_project:
        ref_name, ref_summary = referenced_project
        referenced_block = (
            f"\n\nREFERENCED PROJECT (details already loaded - use them now):\n"
            f"[{ref_name}]: {ref_summary}\n"
            f"The project details are available. Share them directly in your response. "
            f"Do not say you are looking it up, that details are incoming, or that you are pulling it up. "
            f"You already have the information. Use it now. "
            f"You MUST respond with REPLY. Do not return SILENT when project context has been provided."
        )
        print(f"[Gizmo] Injecting referenced project context: '{ref_name}'")

    user_content = (
        f"[{timestamp}]\n\n"
        f"Recent messages in the channel today:\n{context_block}\n\n"
        f"Latest message from {author_name}:\n{latest_message}\n\n"
        f"{involvement}{project_block}{referenced_block}"
    )

    payload = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 300,
        "system": GIZMO_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_content}]
    }

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json=payload
            ) as resp:
                if resp.status != 200:
                    print(f"Claude API error: {resp.status} {await resp.text()}")
                    return "[no response]"
                data = await resp.json()
                text = data["content"][0]["text"].strip()
                return text
    except Exception as e:
        print(f"Error calling Claude API: {e}")
        return "[no response]"



async def handle_level_up(user_id, guild, new_level):
    """Remove old level roles, assign the new one, and announce the promotion."""
    cfg = load_config()
    member = guild.get_member(user_id)
    if not member:
        try:
            member = await guild.fetch_member(user_id)
        except discord.NotFound:
            return

    # Remove all existing level roles
    all_level_role_ids = {int(rid) for rid in cfg['LEVEL_ROLES'].values()}
    roles_to_remove = [r for r in member.roles if r.id in all_level_role_ids]
    if roles_to_remove:
        try:
            await member.remove_roles(*roles_to_remove)
        except discord.Forbidden:
            print(f"No permission to remove level roles from {member.display_name}")

    # Assign new level role
    new_role_id = cfg['LEVEL_ROLES'].get(str(new_level))
    if new_role_id:
        new_role = guild.get_role(int(new_role_id))
        if new_role:
            try:
                await member.add_roles(new_role)
            except discord.Forbidden:
                print(f"No permission to add level role to {member.display_name}")

    # Announce
    try:
        await announce(fmt('level_up', mention=member.mention, level=new_level))
    except Exception as e:
        print(f"Failed to announce level-up for {user_id}: {e}")


async def update_leaderboard_message():
    """Fetches leaderboard data and updates the leaderboard message embed."""
    cfg = load_config()
    channel_id = cfg.get('LEADERBOARD_CHANNEL_ID')
    message_id = cfg.get('LEADERBOARD_MESSAGE_ID')

    if not channel_id or not message_id:
        return

    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            print(f"Error: Channel with ID {channel_id} not found.")
            return

        message = await channel.fetch_message(message_id)
        users_data = database.get_leaderboard_data()
        messages = cfg['MESSAGES']

        kudos_emoji = discord.utils.get(channel.guild.emojis, name=cfg['KUDOS_EMOJI'])
        emoji_string = str(kudos_emoji) if kudos_emoji else f":{cfg['KUDOS_EMOJI']}:"

        embed = discord.Embed(
            title=messages['leaderboard_title'],
            description=messages['leaderboard_description'].format(emoji=emoji_string),
            color=discord.Color(0xFFFF00)
        )

        leaderboard_entries = []
        for i, user_row in enumerate(users_data[:10], 1):
            member = channel.guild.get_member(user_row['user_id'])
            display_name = member.display_name if member else f"User ID: {user_row['user_id']}"
            leaderboard_entries.append(
                messages['leaderboard_entry'].format(
                    rank=i, name=display_name, kudos=user_row['monthly_kudos_given']
                )
            )

        leaderboard_string = "\n".join(leaderboard_entries) if leaderboard_entries else messages['leaderboard_empty']
        embed.description += leaderboard_string
        embed.set_footer(text=messages['leaderboard_footer'].format(date=get_next_month().strftime('%B %d')))

        await message.edit(content=None, embed=embed)

    except discord.NotFound:
        print("Error: Leaderboard message not found.")
    except Exception as e:
        print(f"An error occurred while updating the leaderboard: {e}")


async def update_streaks_message():
    """Fetches streak data and updates the streaks leaderboard embed."""
    cfg = load_config()
    channel_id = cfg.get('LEADERBOARD_CHANNEL_ID')
    message_id = cfg.get('STREAKS_MESSAGE_ID')

    if not channel_id or not message_id:
        return

    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            return

        message = await channel.fetch_message(message_id)
        users_data = database.get_streak_leaderboard()

        embed = discord.Embed(
            title="ACTIVE STREAKS",
            description="Units currently on a project update streak.\n\n",
            color=discord.Color(0xFFFF00)
        )

        if not users_data:
            embed.description += "No active streaks. Get to work."
        else:
            entries = []
            for user_row in users_data[:10]:
                member = channel.guild.get_member(user_row['user_id'])
                display_name = member.display_name if member else f"User ID: {user_row['user_id']}"
                streak = user_row['current_streak']
                best = user_row['best_streak']
                entries.append(f"`🔥` `{display_name}` → `{streak} {plural(streak, 'Day')}`")

            embed.description += "\n".join(entries)

        embed.set_footer(text="Streaks reset after 2 missed days.")
        await message.edit(content=None, embed=embed)

    except discord.NotFound:
        print("Error: Streaks message not found.")
    except Exception as e:
        print(f"An error occurred while updating streaks: {e}")


async def update_history_message():
    """Dormant - history embed is currently disabled. Kept for future re-enable.

    No loop calls this function. It runs only when !init_history is invoked.
    """
    cfg = load_config()
    channel_id = cfg.get('LEADERBOARD_CHANNEL_ID')
    message_id = cfg.get('HISTORY_MESSAGE_ID')

    if not channel_id or not message_id:
        return

    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            return

        message = await channel.fetch_message(message_id)
        history_data = database.get_monthly_history()
        messages = cfg['MESSAGES']

        embed = discord.Embed(
            title=messages['history_title'],
            description=messages['history_description'],
            color=discord.Color(0xFFFF00)
        )

        if not history_data:
            embed.description += messages['history_empty']
        else:
            history_entries = []
            for record in history_data:
                member = channel.guild.get_member(record['user_id'])
                display_name = member.display_name if member else f"User ID: {record['user_id']}"
                month_obj = datetime.strptime(record['month'], '%Y-%m')
                month_display = month_obj.strftime('%B %Y')
                history_entries.append(
                    messages['history_entry'].format(
                        month=month_display, name=display_name, kudos=record['monthly_kudos']
                    )
                )
            embed.description += "\n".join(history_entries)

        embed.set_footer(text=messages['history_footer'])
        await message.edit(content=None, embed=embed)

    except discord.NotFound:
        print("Error: History message not found.")
    except Exception as e:
        print(f"An error occurred while updating the history: {e}")


async def _sync_roles_helper(guild: discord.Guild):
    """Synchronizes level roles for all members based on recalculated lifetime_exp.

    Always recalculates lifetime_level from lifetime_exp before assigning roles,
    ensuring stored level never drifts from actual EXP.
    """
    print("Starting role synchronization...")
    cfg = load_config()
    thresholds = cfg['EXP_THRESHOLDS']
    all_level_role_ids = {int(rid) for rid in cfg['LEVEL_ROLES'].values()}

    for member in guild.members:
        if member.bot:
            continue

        user_data = database.get_or_create_user(member.id)

        # Always recalculate level from EXP first
        recalc_level = database.calculate_level(user_data['lifetime_exp'], thresholds)
        if recalc_level != user_data['lifetime_level']:
            conn = database.get_db_connection()
            conn.execute(
                'UPDATE users SET lifetime_level = ? WHERE user_id = ?',
                (recalc_level, member.id)
            )
            conn.commit()
            conn.close()

        target_role_id = cfg['LEVEL_ROLES'].get(str(recalc_level))
        if not target_role_id:
            continue

        target_role_id = int(target_role_id)
        roles_to_remove = []
        has_target_role = False

        for role in member.roles:
            if role.id in all_level_role_ids:
                if role.id == target_role_id:
                    has_target_role = True
                else:
                    roles_to_remove.append(role)

        if not has_target_role:
            role_to_add = guild.get_role(target_role_id)
            if role_to_add:
                try:
                    await member.add_roles(role_to_add)
                    print(f"Affirmative. Role '{role_to_add.name}' has been assigned to unit {member.display_name}")
                except discord.Forbidden:
                    print(f"ERROR: No permission to add roles to {member.display_name}")

        if roles_to_remove:
            try:
                await member.remove_roles(*roles_to_remove)
                print(f"Correcting role assignment for unit {member.display_name}. Standby.")
            except discord.Forbidden:
                print(f"ERROR: No permission to remove roles from {member.display_name}")

    print("Role synchronization complete.")


@bot.event
async def on_ready():
    """Handles bot startup, initial setup, and task launching."""
    await bot.wait_until_ready()
    if not bot.setup_done:
        print(f'Logged in as {bot.user}')
        database.setup_database()
        guild = bot.get_guild(int(config['GUILD_ID']))
        if guild:
            await _sync_roles_helper(guild)
        else:
            print("ERROR: Could not find server. Role sync skipped.")

        # Seed default watched channels if table is empty
        existing = database.get_watched_channels()
        if not existing:
            for cid, hours in DEFAULT_WATCHED_CHANNELS.items():
                database.set_channel_window(cid, hours)
            print(f"Seeded {len(DEFAULT_WATCHED_CHANNELS)} default watched channel(s).")

        update_leaderboard_loop.start()
        daily_maintenance_loop.start()
        monthly_reset_loop.start()
        keep_forum_threads_alive.start()
        gizmo_unprompted_loop.start()
        bot.setup_done = True
    else:
        print(f"Bot reconnected as {bot.user}")


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """Handles kudos awards when a user (or the bot itself for daily greeting) reacts."""
    cfg = load_config()
    if payload.emoji.name != cfg['KUDOS_EMOJI'] or payload.guild_id is None:
        return

    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return

    try:
        message = await channel.fetch_message(payload.message_id)
    except discord.NotFound:
        return

    message_age = datetime.now(timezone.utc) - message.created_at
    if message_age.days > cfg['KUDOS_VALIDITY_DAYS']:
        print(f"Kudos allocation ignored. Message from {message.created_at.date()} is outside the operational timeframe.")
        try:
            reactor_user = await bot.fetch_user(payload.user_id)
            await message.remove_reaction(payload.emoji, reactor_user)
        except (discord.NotFound, discord.Forbidden):
            pass
        return

    creator = message.author
    if creator.bot:
        return

    # Bot reacting to a human message → daily greeting kudos
    if payload.user_id == bot.user.id:
        database.get_or_create_user(creator.id)
        database.award_daily_greeting_kudos(creator.id, bot.user.id)
        database.log_kudos(message.id, bot.user.id, creator.id)
        print(f"Daily greeting kudos allocated: BOT -> {creator.display_name}")

        guild = bot.get_guild(payload.guild_id)
        if guild:
            new_level = database.check_and_apply_level_up(creator.id, cfg['EXP_THRESHOLDS'])
            if new_level:
                await handle_level_up(creator.id, guild, new_level)
        return

    reactor = payload.member
    if not reactor:
        return

    if reactor.id == creator.id:
        return

    database.reset_daily_limit_if_needed(reactor.id)
    reactor_data = database.get_or_create_user(reactor.id)

    if reactor_data['daily_awards_given'] >= cfg['DAILY_AWARD_LIMIT']:
        try:
            await message.remove_reaction(payload.emoji, reactor)
            await channel.send(fmt('kudos_limit_reached', mention=reactor.mention), delete_after=10)
        except discord.Forbidden:
            print(f"Could not remove reaction in {channel.name} due to permissions.")
        return

    database.get_or_create_user(creator.id)
    database.award_kudos(creator.id, reactor.id)
    database.log_kudos(message.id, reactor.id, creator.id)
    print(f"Kudos allocated: {reactor.display_name} -> {creator.display_name}")

    # Level-up checks for both creator (+2 EXP) and reactor (+1 EXP)
    guild = bot.get_guild(payload.guild_id)
    if guild:
        for uid in (creator.id, reactor.id):
            new_level = database.check_and_apply_level_up(uid, cfg['EXP_THRESHOLDS'])
            if new_level:
                await handle_level_up(uid, guild, new_level)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    """Handles removing kudos when a user removes the kudos emoji."""
    cfg = load_config()
    if payload.emoji.name != cfg['KUDOS_EMOJI'] or payload.guild_id is None:
        return

    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    try:
        message = await channel.fetch_message(payload.message_id)
        message_age = datetime.now(timezone.utc) - message.created_at
        if message_age.days > cfg['KUDOS_VALIDITY_DAYS']:
            return

        reactor = await guild.fetch_member(payload.user_id)
        creator = message.author
    except (discord.NotFound, discord.Forbidden):
        return

    if reactor.bot or creator.bot or reactor.id == creator.id:
        return

    if database.check_kudos_exists(message.id, reactor.id):
        database.remove_kudos(creator.id, reactor.id)
        database.delete_kudos_log(message.id, reactor.id)
        print(f"Kudos retracted: {reactor.display_name} from {creator.display_name}")
        await channel.send(fmt('kudos_retracted', mention=reactor.mention), delete_after=10)
        # No level-up check - EXP doesn't decrement, so no level change is possible
    else:
        print(f"Request from {reactor.display_name} to retract kudos ignored: No corresponding record in the log.")


@bot.event
async def on_message(message: discord.Message):
    """Handles daily first-message greetings by adding the kudos reaction.

    The actual kudos award and level-up check happen in on_raw_reaction_add when
    the bot's own reaction fires.
    """
    if message.author.bot:
        await bot.process_commands(message)
        return

    if message.guild is None:
        await bot.process_commands(message)
        return

    cfg = load_config()
    user_data = database.get_or_create_user(message.author.id)
    today = get_vancouver_today()

    if user_data['last_message_date'] != today:
        is_returning_user = user_data['last_message_date'] is not None
        database.update_last_message_date(message.author.id, today)

        if is_returning_user:
            kudos_emoji = discord.utils.get(message.guild.emojis, name=cfg['KUDOS_EMOJI'])
            if kudos_emoji:
                try:
                    await message.add_reaction(kudos_emoji)
                    print(f"Daily kudos reaction added for {message.author.display_name}")

                    global_greeting_enabled = cfg.get('DAILY_GREETING_ENABLED', True)
                    user_greeting_enabled = user_data['greeting_enabled'] if user_data['greeting_enabled'] is not None else 1

                    if global_greeting_enabled and user_greeting_enabled == 1:
                        await message.reply(
                            fmt('daily_greeting', mention=message.author.mention),
                            delete_after=30
                        )
                        print(f"Daily greeting message sent to {message.author.display_name}")
                except discord.Forbidden:
                    print(f"Could not add reaction or send greeting in {message.channel.name} due to permissions.")
        else:
            print(f"New user detected: {message.author.display_name}. Last message date initialized.")

    # --- Project streak tracking ---
    if message.guild:
        ch = message.channel
        if isinstance(ch, discord.Thread) and ch.parent_id == PROJECT_FORUM_ID:
            if ch.owner_id == message.author.id:
                cfg_streak = load_config()
                milestones = cfg_streak.get('STREAK_MILESTONES', [2, 5, 7, 14, 21, 28, 35, 42, 49, 56, 63, 70])
                current_streak, best_streak, milestone_hit, is_new_day = database.update_project_streak(
                    message.author.id, milestones
                )
                print(f"[Streak] {message.author.display_name} posted in '{ch.name}' - streak: {current_streak} (best: {best_streak}, new_day: {is_new_day})")

                # Silent kudos reaction on first post of the day
                if is_new_day:
                    kudos_emoji = discord.utils.get(message.guild.emojis, name=cfg_streak['KUDOS_EMOJI'])
                    if kudos_emoji:
                        try:
                            await message.add_reaction(kudos_emoji)
                        except discord.Forbidden:
                            print(f"[Streak] Could not react to {message.author.display_name}'s project post")

                # Milestone announcement
                if milestone_hit and cfg_streak.get('STREAK_ANNOUNCEMENTS_ENABLED', True):
                    announcement_channel = bot.get_channel(cfg_streak.get('ANNOUNCEMENT_CHANNEL_ID'))
                    if announcement_channel:
                        try:
                            await announcement_channel.send(
                                f"Unit {message.author.mention} has posted in their project thread for "
                                f"**{milestone_hit} consecutive days**. Consistency noted."
                            )
                        except discord.Forbidden:
                            print(f"[Streak] Could not announce milestone for {message.author.display_name}")

    await bot.process_commands(message)

    # --- Chatbot ---
    watched = database.get_watched_channels()
    if chatbot_is_enabled() and message.channel.id in watched:
        # Check for explicit triggers first so we can bypass cooldown if directly involved
        bot_mentioned_pre = bot.user in message.mentions
        name_mentioned_pre = 'gizmo' in message.content.lower()
        is_reply_to_bot_pre = (
            message.reference is not None and
            message.reference.resolved is not None and
            isinstance(message.reference.resolved, discord.Message) and
            message.reference.resolved.author.id == bot.user.id
        )
        explicit_trigger_pre = bot_mentioned_pre or name_mentioned_pre or is_reply_to_bot_pre

        # Bypass cooldown if directly involved - always respond when someone is talking to Gizmo
        if not explicit_trigger_pre and not chatbot_is_ready():
            await bot.process_commands(message)
            return
    if chatbot_is_enabled() and message.channel.id in watched:
        # Determine if Gizmo is directly involved
        explicit_trigger = explicit_trigger_pre

        context_hours = watched.get(message.channel.id, 12)
        channel_messages = await fetch_todays_messages(message.channel, bot.user, context_hours=context_hours)

        # If no explicit trigger, run cheap "is this for me?" pass
        directly_involved = explicit_trigger
        if not explicit_trigger:
            directly_involved = await is_message_for_gizmo(channel_messages)
            if directly_involved:
                print(f"[IsForGizmo] Non-explicit message flagged as directed at Gizmo.")

        # If Gizmo is directly involved, run two-pass project detection
        referenced_project = None
        if directly_involved:
            project_result = await find_referenced_project(channel_messages)
            if project_result == "ASK":
                # No matching thread found - Gizmo asks the user which thread
                try:
                    await message.reply("Which thread is that project in? I want to pull up the details.")
                    set_chatbot_cooldown()
                except discord.Forbidden:
                    pass
                await bot.process_commands(message)
                return
            elif isinstance(project_result, tuple):
                referenced_project = project_result

        response = await query_gizmo(
            channel_messages,
            message.content,
            message.author.display_name,
            directly_involved,
            referenced_project=referenced_project
        )

        if response:
            r = response.strip()
            give_kudos = r.startswith("KUDOS")
            reply_text = None
            if "REPLY:" in r:
                reply_text = r[r.index("REPLY:") + len("REPLY:"):].strip()
                # Replace em/en dashes (and surrounding spaces) with ". Capitalized next word"
                import re
                reply_text = re.sub(r'\s*[—–]\s*(\w)', lambda m: '. ' + m.group(1).upper(), reply_text)

            # Short cooldown even when Gizmo stays silent - prevents API hammering
            if not give_kudos and not reply_text:
                global _chatbot_cooldown_until
                _chatbot_cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=3)
                print(f"[Gizmo] SILENT in #{message.channel.name} (directly_involved={directly_involved})")

            acted = False
            if give_kudos:
                try:
                    kudos_emoji = discord.utils.get(message.guild.emojis, name=cfg['KUDOS_EMOJI'])
                    if kudos_emoji:
                        await message.add_reaction(kudos_emoji)
                        acted = True
                except discord.Forbidden:
                    print(f"Could not add kudos reaction in #{message.channel.name}")

            if reply_text:
                try:
                    await message.reply(reply_text)
                    acted = True
                except discord.Forbidden:
                    print(f"Could not send chatbot response in #{message.channel.name}")

            if acted:
                set_chatbot_cooldown()
                print(f"Gizmo acted in #{message.channel.name}: kudos={give_kudos} reply={bool(reply_text)}")


# ==========================================
# COMMANDS
# ==========================================

@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def init_leaderboard(ctx: commands.Context):
    """(Admin) Creates the leaderboard embed in the current channel."""
    cfg = load_config()
    messages = cfg['MESSAGES']
    embed = discord.Embed(
        title=messages['leaderboard_title'],
        description="Initializing performance log. Standby.",
        color=discord.Color(0xFFFF00)
    )
    message = await ctx.send(embed=embed)

    cfg['LEADERBOARD_CHANNEL_ID'] = ctx.channel.id
    cfg['LEADERBOARD_MESSAGE_ID'] = message.id
    save_config(cfg)

    await ctx.message.delete()
    await update_leaderboard_message()
    await ctx.send("The performance log is now operational.", delete_after=5)


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def init_streaks(ctx: commands.Context):
    """(Admin) Creates the streaks embed in the current channel."""
    cfg = load_config()
    embed = discord.Embed(
        title="ACTIVE STREAKS",
        description="Initializing streaks log. Standby.",
        color=discord.Color(0xFFFF00)
    )
    message = await ctx.send(embed=embed)

    cfg['STREAKS_MESSAGE_ID'] = message.id
    save_config(cfg)

    await ctx.message.delete()
    await update_streaks_message()
    await ctx.send("The streaks log is now operational.", delete_after=5)


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def init_history(ctx: commands.Context):
    """(Admin) Creates the history embed. Dormant - update loop is currently disabled."""
    cfg = load_config()
    messages = cfg['MESSAGES']
    embed = discord.Embed(
        title=messages['history_title'],
        description="Initializing performance history. Standby.",
        color=discord.Color(0x00BFFF)
    )
    message = await ctx.send(embed=embed)

    cfg['HISTORY_MESSAGE_ID'] = message.id
    save_config(cfg)

    await ctx.message.delete()
    await update_history_message()
    await ctx.send("The performance history log is now operational.", delete_after=5)


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def sync_roles(ctx: commands.Context):
    """(Admin) Manually trigger role synchronization based on lifetime_exp."""
    await ctx.message.delete()
    await ctx.send(fmt('role_sync_start'), delete_after=10)
    await _sync_roles_helper(ctx.guild)
    await ctx.send(fmt('role_sync_complete'), delete_after=10)


@bot.command()
async def reset_daily_limits(ctx: commands.Context, member: discord.Member = None):
    """(Owner) Reset the daily kudos-giving limit for a user or all users."""
    if ctx.author.id != OWNER_ID:
        await ctx.send(fmt('unauthorized', mention=ctx.author.mention), delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()
    if member:
        database.reset_daily_limits(member.id)
        await ctx.send(fmt('reset_limits_user', mention=member.mention), delete_after=10)
    else:
        database.reset_daily_limits()
        await ctx.send(fmt('reset_limits_all'), delete_after=10)


@bot.command()
async def test_embed(ctx: commands.Context):
    """Debug: verify the kudos custom emoji renders correctly."""
    cfg = load_config()
    kudos_emoji = discord.utils.get(ctx.guild.emojis, name=cfg['KUDOS_EMOJI'])
    if kudos_emoji:
        await ctx.send(f"Custom emoji test: {kudos_emoji}")
        embed = discord.Embed(title=f"Embed with {kudos_emoji}!", description="The emoji works!", color=discord.Color(0xFFFF00))
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"Could not find an emoji named `{cfg['KUDOS_EMOJI']}`.")


@bot.command()
async def toggle_greeting(ctx: commands.Context):
    """Toggle the daily greeting reply message for yourself."""
    new_state = database.toggle_user_greeting(ctx.author.id)
    key = 'greeting_enabled' if new_state else 'greeting_disabled'
    await ctx.message.delete()
    await ctx.send(fmt(key, mention=ctx.author.mention), delete_after=10)


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def toggle_chatbot(ctx: commands.Context):
    """(Admin) Toggles Gizmo's AI chat feature on or off."""
    current = database.get_system_state("CHATBOT_ENABLED", "false")
    new_state = "false" if current == "true" else "true"
    database.set_system_state("CHATBOT_ENABLED", new_state)

    label = "**online**" if new_state == "true" else "**offline**"
    await ctx.message.delete()
    await ctx.send(f"Affirmative. Gizmo's conversational matrix is now {label}.", delete_after=10)


@bot.command()
async def add_kudos(ctx: commands.Context, member: discord.Member = None, amount: int = None):
    """(Owner) Manually grant kudos to a user. Triggers level-up check."""
    if ctx.author.id != OWNER_ID:
        await ctx.send(fmt('unauthorized', mention=ctx.author.mention), delete_after=10)
        await ctx.message.delete()
        return

    if member is None or amount is None or amount <= 0:
        await ctx.send("Invalid parameters. Usage: `!add_kudos @user amount`", delete_after=10)
        await ctx.message.delete()
        return

    cfg = load_config()
    database.get_or_create_user(member.id)

    conn = database.get_db_connection()
    conn.execute(
        '''UPDATE users
           SET monthly_kudos_received = monthly_kudos_received + ?,
               lifetime_kudos_received = lifetime_kudos_received + ?,
               lifetime_exp = lifetime_exp + ?
           WHERE user_id = ?''',
        (amount, amount, amount, member.id)
    )
    conn.commit()
    conn.close()

    await ctx.message.delete()
    await ctx.send(fmt('add_kudos_confirm', amount=amount, mention=member.mention), delete_after=10)

    # Level-up check after manual grant
    new_level = database.check_and_apply_level_up(member.id, cfg['EXP_THRESHOLDS'])
    if new_level:
        await handle_level_up(member.id, ctx.guild, new_level)


@bot.command()
async def systemtime(ctx: commands.Context):
    """Debug: shows the bot's current time in Vancouver and UTC."""
    now_vancouver = get_vancouver_now()
    now_utc = datetime.now(timezone.utc)
    today_vancouver = get_vancouver_today()

    embed = discord.Embed(title="System Time (Debug Info)", color=discord.Color(0xFFFF00))
    embed.add_field(name="Vancouver Time", value=f"`{now_vancouver.strftime('%Y-%m-%d %H:%M:%S %Z')}`", inline=False)
    embed.add_field(name="Vancouver Date", value=f"`{today_vancouver}`", inline=False)
    embed.add_field(name="UTC Time", value=f"`{now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}`", inline=False)

    await ctx.send(embed=embed, delete_after=30)
    await ctx.message.delete()


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def crown(ctx: commands.Context, member: discord.Member = None):
    """(Admin) Awards The Deliverer role to a member who has shipped a full game."""
    if member is None:
        await ctx.send("Usage: `!crown @user`", delete_after=10)
        await ctx.message.delete()
        return

    cfg = load_config()
    role_id = cfg.get('CROWN_ROLE_ID')
    if not role_id:
        await ctx.send("The Deliverer role is not configured. Set CROWN_ROLE_ID in config.json.", delete_after=10)
        await ctx.message.delete()
        return

    role = ctx.guild.get_role(int(role_id))
    if not role:
        await ctx.send("The Deliverer role not found in server.", delete_after=10)
        await ctx.message.delete()
        return

    if role in member.roles:
        await ctx.send(fmt('crown_already_held', mention=member.mention), delete_after=10)
        await ctx.message.delete()
        return

    try:
        await member.add_roles(role)
    except discord.Forbidden:
        await ctx.send("ERROR: Missing permission to assign The Deliverer role.", delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()
    await announce(fmt('crown_awarded', mention=member.mention))


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def jam(ctx: commands.Context, member: discord.Member = None):
    """(Admin) Awards the Game Jammer role to a member who has participated in a jam."""
    if member is None:
        await ctx.send("Usage: `!jam @user`", delete_after=10)
        await ctx.message.delete()
        return

    cfg = load_config()
    role_id = cfg.get('JAM_ROLE_ID')
    if not role_id:
        await ctx.send("Game Jammer role is not configured. Set JAM_ROLE_ID in config.json.", delete_after=10)
        await ctx.message.delete()
        return

    role = ctx.guild.get_role(int(role_id))
    if not role:
        await ctx.send("Game Jammer role not found in server.", delete_after=10)
        await ctx.message.delete()
        return

    if role in member.roles:
        await ctx.send(fmt('jam_already_held', mention=member.mention), delete_after=10)
        await ctx.message.delete()
        return

    try:
        await member.add_roles(role)
    except discord.Forbidden:
        await ctx.send("ERROR: Missing permission to assign Game Jammer role.", delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()
    await announce(fmt('jam_awarded', mention=member.mention))


@bot.command()
async def stats(ctx: commands.Context):
    """Display your full performance stats. Ephemeral - deletes after 30s."""
    cfg = load_config()
    thresholds = cfg['EXP_THRESHOLDS']
    user = database.get_or_create_user(ctx.author.id)

    lifetime_exp = user['lifetime_exp']
    level = database.calculate_level(lifetime_exp, thresholds)

    # Progress to next level
    current_threshold = thresholds[level - 1] if level <= len(thresholds) else thresholds[-1]
    if level < len(thresholds):
        next_threshold = thresholds[level]
        progress = lifetime_exp - current_threshold
        needed = next_threshold - current_threshold
        progress_str = f"`{progress}/{needed}` EXP to Level {level + 1}"
    else:
        progress_str = "Maximum designation reached."

    # Streak info
    from datetime import date as date_cls
    current_streak = user['current_streak'] if user['current_streak'] is not None else 0
    best_streak = user['best_streak'] if user['best_streak'] is not None else 0
    last_post = user['last_project_post_date']
    if last_post:
        delta_days = (date_cls.today() - date_cls.fromisoformat(last_post)).days
        if delta_days == 0:
            since_str = "Last updated today"
        elif delta_days == 1:
            since_str = "Last updated yesterday"
        else:
            since_str = f"{delta_days} {plural(current_streak, 'day')} since last post"
    else:
        since_str = "No project posts yet"
    streak_str = f"Current: {current_streak} {plural(current_streak, 'day')}\nBest: {best_streak} {plural(best_streak, 'day')}\n{since_str}"

    embed = discord.Embed(
        title=f"Performance Record - {ctx.author.display_name}",
        color=discord.Color(0xFFFF00)
    )
    embed.add_field(name="Level", value=f"**{level}**", inline=True)
    embed.add_field(name="Lifetime EXP", value=f"`{lifetime_exp}`", inline=True)
    embed.add_field(name="Progress", value=progress_str, inline=False)
    embed.add_field(
        name="Lifetime Kudos",
        value=f"Received: `{user['lifetime_kudos_received']}`\nGiven: `{user['lifetime_kudos_given']}`",
        inline=True
    )
    embed.add_field(
        name="This Cycle",
        value=f"Received: `{user['monthly_kudos_received']}`\nGiven: `{user['monthly_kudos_given']}`",
        inline=True
    )
    embed.add_field(
        name="Project Streak",
        value=streak_str,
        inline=False
    )

    await ctx.send(embed=embed, delete_after=30)
    await ctx.message.delete()


@bot.command()
async def exp(ctx: commands.Context):
    """Alias for !stats for backwards compatibility."""
    await ctx.invoke(stats)


@bot.command()
async def apply_lifetime_exp(ctx: commands.Context):
    """(Owner) Applies /data/lifetime_exp_migration.json to the database.

    Overwrites lifetime_exp, lifetime_kudos_received, lifetime_kudos_given, and
    recomputes lifetime_level. Does NOT modify monthly stats. After running, run
    !sync_roles to propagate level role changes.
    """
    if ctx.author.id != OWNER_ID:
        await ctx.send(fmt('unauthorized', mention=ctx.author.mention), delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()

    migration_path = "/data/lifetime_exp_migration.json"
    if not os.path.exists(migration_path):
        await ctx.send(f"Migration file not found at `{migration_path}`. Run `!migrate_lifetime_kudos` first.")
        return

    try:
        with open(migration_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        await ctx.send(f"Failed to load migration file: {e}")
        return

    cfg = load_config()
    thresholds = cfg['EXP_THRESHOLDS']

    updated = 0
    total_exp_applied = 0
    level_counts = defaultdict(int)

    for user_id_str, user_data in data.get('users', {}).items():
        user_id = int(user_id_str)
        lifetime_exp = user_data['lifetime_exp']
        kr = user_data['kudos_received']
        kg = user_data['kudos_given']
        new_level = database.calculate_level(lifetime_exp, thresholds)

        database.get_or_create_user(user_id)
        conn = database.get_db_connection()
        conn.execute(
            '''UPDATE users
               SET lifetime_exp = ?,
                   lifetime_kudos_received = ?,
                   lifetime_kudos_given = ?,
                   lifetime_level = ?
               WHERE user_id = ?''',
            (lifetime_exp, kr, kg, new_level, user_id)
        )
        conn.commit()
        conn.close()

        updated += 1
        total_exp_applied += lifetime_exp
        level_counts[new_level] += 1

    summary_lines = [
        "**Migration Applied**",
        f"Users updated: `{updated}`",
        f"Total EXP applied: `{total_exp_applied}`",
        "Level distribution:"
    ]
    for lvl in sorted(level_counts.keys()):
        summary_lines.append(f"  Level {lvl}: `{level_counts[lvl]}` unit(s)")
    summary_lines.append("")
    summary_lines.append("Run `!sync_roles` to propagate level roles.")

    await ctx.send("\n".join(summary_lines))


@bot.command()
async def migrate_lifetime_kudos(ctx: commands.Context):
    """(Owner) Scans all channels and threads for kudos reactions over all history
    and outputs a lifetime EXP tally as JSON. Does not write to the database.

    Ignores bot reactions (daily greeting kudos are NOT backfilled - forward-only
    from deploy date onward). Ignores self-kudos. Dedupes via seen_pairs set.
    """
    if ctx.author.id != OWNER_ID:
        await ctx.send(fmt('unauthorized', mention=ctx.author.mention), delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()
    await ctx.send("Initiating lifetime EXP migration scan over all history. Scanning all channels and threads. This will take a while. Standby.")

    cfg = load_config()
    kudos_emoji_name = cfg['KUDOS_EMOJI']
    thresholds = cfg['EXP_THRESHOLDS']

    exp_totals = defaultdict(lambda: {'lifetime_exp': 0, 'kudos_given': 0, 'kudos_received': 0})
    seen_pairs = set()

    messages_scanned = 0
    kudos_counted = 0

    async def scan_messages(source):
        nonlocal messages_scanned, kudos_counted
        try:
            async for message in source.history(limit=None):
                messages_scanned += 1
                creator = message.author

                if creator.bot:
                    continue

                for reaction in message.reactions:
                    if getattr(reaction.emoji, 'name', reaction.emoji) != kudos_emoji_name:
                        continue

                    async for reactor in reaction.users():
                        if reactor.bot:
                            continue
                        if reactor.id == creator.id:
                            continue

                        pair = (message.id, reactor.id)
                        if pair in seen_pairs:
                            continue
                        seen_pairs.add(pair)

                        exp_totals[creator.id]['lifetime_exp'] += 2
                        exp_totals[creator.id]['kudos_received'] += 1
                        exp_totals[reactor.id]['lifetime_exp'] += 1
                        exp_totals[reactor.id]['kudos_given'] += 1
                        kudos_counted += 1

        except discord.Forbidden:
            print(f"Skipping {source}: Missing read permissions.")
        except Exception as e:
            print(f"Error scanning {source}: {e}")

    # Scan all text channels and their threads
    for channel in ctx.guild.text_channels:
        await scan_messages(channel)
        for thread in channel.threads:
            await scan_messages(thread)
        try:
            async for thread in channel.archived_threads(limit=None):
                await scan_messages(thread)
        except (discord.Forbidden, discord.HTTPException) as e:
            print(f"Could not fetch archived threads for {channel.name}: {e}")

    # Scan forum channels
    for channel_id in cfg.get('FORUM_CHANNEL_IDS', []):
        forum = bot.get_channel(channel_id)
        if not forum:
            continue
        for thread in forum.threads:
            await scan_messages(thread)
        try:
            async for thread in forum.archived_threads(limit=None):
                await scan_messages(thread)
        except (discord.Forbidden, discord.HTTPException) as e:
            print(f"Could not fetch archived threads for forum {channel_id}: {e}")

    # Build output
    guild = ctx.guild
    output = {
        "scan_summary": {
            "messages_scanned": messages_scanned,
            "kudos_events_counted": kudos_counted,
            "unique_users": len(exp_totals)
        },
        "users": {}
    }

    for user_id, data in sorted(exp_totals.items(), key=lambda x: x[1]['lifetime_exp'], reverse=True):
        member = guild.get_member(user_id)
        display_name = member.display_name if member else f"Unknown ({user_id})"
        calculated_level = database.calculate_level(data['lifetime_exp'], thresholds)
        output["users"][str(user_id)] = {
            "display_name": display_name,
            "lifetime_exp": data['lifetime_exp'],
            "kudos_received": data['kudos_received'],
            "kudos_given": data['kudos_given'],
            "calculated_level": calculated_level
        }

    output_path = "/data/lifetime_exp_migration.json"
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=4)

    header = (
        f"**Migration Scan Complete.**\n"
        f"Messages scanned: `{messages_scanned}`\n"
        f"Kudos events counted: `{kudos_counted}`\n"
        f"Unique users: `{len(exp_totals)}`\n\n"
    )
    await ctx.send(header)

    json_str = json.dumps(output, indent=4)
    chunk_size = 1900
    chunks = [json_str[i:i+chunk_size] for i in range(0, len(json_str), chunk_size)]
    for chunk in chunks:
        await ctx.send(f"```json\n{chunk}\n```")
        await asyncio.sleep(0.5)

    print(f"Lifetime EXP migration scan complete. Results written to {output_path}")


@bot.command()
async def backfill_monthly(ctx: commands.Context):
    """(Owner) Backfills monthly_kudos_given and monthly_kudos_received from kudos_log.

    Reads every transaction currently in kudos_log and increments the monthly
    counters accordingly. Run once after deploying the new schema to catch up
    on the current cycle's activity. Safe to run only once - kudos_log is cleared
    on monthly reset so there is no risk of double-counting across cycles.
    """
    if ctx.author.id != OWNER_ID:
        await ctx.send(fmt('unauthorized', mention=ctx.author.mention), delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()
    await ctx.send("Backfilling monthly counters from kudos_log. Standby.")

    conn = database.get_db_connection()
    rows = conn.execute('SELECT message_id, reactor_id, creator_id FROM kudos_log').fetchall()
    conn.close()

    if not rows:
        await ctx.send("kudos_log is empty - nothing to backfill.")
        return

    given_counts = defaultdict(int)    # reactor_id -> kudos given this cycle
    received_counts = defaultdict(int) # creator_id -> kudos received this cycle

    for row in rows:
        reactor_id = row['reactor_id']
        creator_id = row['creator_id']
        received_counts[creator_id] += 1
        # Bot reactions count toward received only, not given
        if reactor_id != bot.user.id:
            given_counts[reactor_id] += 1

    conn = database.get_db_connection()
    for user_id, count in received_counts.items():
        database.get_or_create_user(user_id)
        conn.execute(
            'UPDATE users SET monthly_kudos_received = monthly_kudos_received + ? WHERE user_id = ?',
            (count, user_id)
        )
    for user_id, count in given_counts.items():
        database.get_or_create_user(user_id)
        conn.execute(
            'UPDATE users SET monthly_kudos_given = monthly_kudos_given + ? WHERE user_id = ?',
            (count, user_id)
        )
    conn.commit()
    conn.close()

    await ctx.send(
        f"**Backfill complete.**\n"
        f"Transactions processed: `{len(rows)}`\n"
        f"Users updated (received): `{len(received_counts)}`\n"
        f"Users updated (given): `{len(given_counts)}`\n\n"
        f"Run `!init_leaderboard` in #standings to refresh the embed."
    )



@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def remove_from_thread(ctx: commands.Context, member: discord.Member = None):
    """(Admin) Removes a member from the current thread."""
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.send("This command must be used inside a thread.", delete_after=10)
        await ctx.message.delete()
        return

    if member is None:
        await ctx.send("Usage: `!remove_from_thread @user`", delete_after=10)
        await ctx.message.delete()
        return

    try:
        await ctx.channel.remove_user(member)
        await ctx.message.delete()
        await ctx.send(f"Removed {member.mention} from this thread.", delete_after=10)
    except discord.Forbidden:
        await ctx.send("Missing permission to remove members from this thread.", delete_after=10)
        await ctx.message.delete()
    except discord.HTTPException as e:
        await ctx.send(f"Failed to remove member: {e}", delete_after=10)
        await ctx.message.delete()


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def archive_thread(ctx: commands.Context):
    """(Admin) Archives the current thread."""
    if not isinstance(ctx.channel, discord.Thread):
        await ctx.send("This command must be used inside a thread.", delete_after=10)
        await ctx.message.delete()
        return

    try:
        await ctx.message.delete()
        await ctx.channel.edit(archived=True)
    except discord.Forbidden:
        await ctx.send("Missing permission to archive this thread.", delete_after=10)
        await ctx.message.delete()
    except discord.HTTPException as e:
        await ctx.send(f"Failed to archive thread: {e}", delete_after=10)
        await ctx.message.delete()


@bot.command()
async def possess(ctx: commands.Context, channel: discord.TextChannel = None, *, message: str = None):
    """(Owner) Speaks through Gizmo in a given channel. Usage: !possess #channel message"""
    if ctx.author.id != OWNER_ID:
        await ctx.send(fmt('unauthorized', mention=ctx.author.mention), delete_after=10)
        await ctx.message.delete()
        return

    if channel is None or not message:
        await ctx.send("Usage: `!possess #channel your message here`", delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()
    await channel.send(message)


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def watch(ctx: commands.Context, channel: discord.TextChannel = None):
    """(Admin) Toggles watching a channel for Gizmo chatbot activity.

    Usage: !watch #channel
    """
    if channel is None:
        await ctx.send("Usage: `!watch #channel`", delete_after=10)
        await ctx.message.delete()
        return

    now_watching = database.toggle_watched_channel(channel.id)
    await ctx.message.delete()
    if now_watching:
        await ctx.send(
            f"Affirmative. Now monitoring {channel.mention} with a default 12-hour context window. "
            f"Use `!window {channel.mention} <hours>` to adjust.",
            delete_after=15
        )
    else:
        await ctx.send(
            f"Acknowledged. {channel.mention} has been removed from active monitoring.",
            delete_after=10
        )


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def window(ctx: commands.Context, channel: discord.TextChannel = None, hours: int = None):
    """(Admin) Sets the context window (in hours) for a watched channel.

    Usage: !window #channel 10
    """
    if channel is None or hours is None or hours <= 0:
        await ctx.send("Usage: `!window #channel <hours>`", delete_after=10)
        await ctx.message.delete()
        return

    database.set_channel_window(channel.id, hours)
    await ctx.message.delete()
    await ctx.send(
        f"Affirmative. Context window for {channel.mention} set to `{hours}` hour(s).",
        delete_after=10
    )


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def watched(ctx: commands.Context):
    """(Admin) Lists all currently watched channels and their context windows."""
    channels = database.get_watched_channels()
    await ctx.message.delete()
    if not channels:
        await ctx.send("No channels are currently being monitored.", delete_after=10)
        return

    lines = []
    for cid, hours in channels.items():
        ch = bot.get_channel(cid)
        name = ch.mention if ch else f"`{cid}`"
        lines.append(f"{name} - `{hours}h` context window")

    await ctx.send("**Monitored Channels:**\n" + "\n".join(lines), delete_after=30)


# ==========================================
# TASKS
# ==========================================

@tasks.loop(seconds=30)
async def update_leaderboard_loop():
    await update_leaderboard_message()
    await update_streaks_message()


@tasks.loop(hours=1)
async def daily_maintenance_loop():
    """Applies daily kudos decay once per Vancouver-day. No-op when decay=0."""
    cfg = load_config()
    today = get_vancouver_today()
    last_run_date = database.get_system_state("LAST_MAINTENANCE_DATE")

    if last_run_date != today:
        print("--- Running daily maintenance... ---")
        database.apply_daily_maintenance(cfg.get('KUDOS_DECAY', 0))
        database.set_system_state("LAST_MAINTENANCE_DATE", today)
        print("--- Daily maintenance complete. ---")


@tasks.loop(hours=1)
async def monthly_reset_loop():
    """Checks hourly if we have entered a new month, then runs the monthly reset
    and rotates the Gizmo's Favourite role to the top kudos giver.
    """
    cfg = load_config()
    today = get_vancouver_today()
    today_obj = get_vancouver_now()
    last_reset_date = database.get_system_state("LAST_MONTHLY_RESET_DATE")
    current_month_str = today_obj.strftime('%Y-%m')

    if not last_reset_date:
        print("No previous monthly reset date found. Initializing to today.")
        database.set_system_state("LAST_MONTHLY_RESET_DATE", today)
        return

    last_reset_month_str = last_reset_date[:7]

    if current_month_str != last_reset_month_str:
        print("--- New month detected! Running catch-up monthly reset... ---")
        winner_data = database.monthly_reset()

        if winner_data:
            guild = bot.get_guild(int(cfg['GUILD_ID']))
            if guild:
                giver_role_id = cfg.get('TOP_PERFORMER_ROLE_ID')

                # Strip the Gizmo's Favourite role from the previous holder
                prev_holder_id = database.get_system_state("CURRENT_GIVER_ID")
                if prev_holder_id and giver_role_id:
                    try:
                        prev_member = await guild.fetch_member(int(prev_holder_id))
                        prev_role = guild.get_role(int(giver_role_id))
                        if prev_role and prev_role in prev_member.roles:
                            await prev_member.remove_roles(prev_role)
                    except discord.NotFound:
                        print(f"Previous Giver holder {prev_holder_id} not in server anymore.")
                    except discord.Forbidden:
                        print("Missing permission to remove Gizmo's Favourite role from previous holder.")
                    except Exception as e:
                        print(f"Error removing Gizmo's Favourite from previous holder: {e}")

                # Assign the Gizmo's Favourite role to the new winner
                try:
                    winner_member = await guild.fetch_member(winner_data['user_id'])
                    if giver_role_id:
                        giver_role = guild.get_role(int(giver_role_id))
                        if giver_role:
                            try:
                                await winner_member.add_roles(giver_role)
                            except discord.Forbidden:
                                print(f"Missing permission to assign Gizmo's Favourite to {winner_member.display_name}")

                    database.set_system_state("CURRENT_GIVER_ID", str(winner_data['user_id']))

                    await announce(fmt('top_giver_winner', mention=winner_member.mention))
                except discord.NotFound:
                    print(f"Winner {winner_data['user_id']} not found in guild.")
                except Exception as e:
                    print(f"An error occurred during monthly reset announcement: {e}")

        await update_leaderboard_message()
        # History embed is dormant - update_history_message() intentionally not called

        database.set_system_state("LAST_MONTHLY_RESET_DATE", today)
        print("--- Monthly reset complete. ---")


@tasks.loop(hours=config.get('FORUM_BUMP_HOURS', 167))
async def keep_forum_threads_alive():
    cfg = load_config()
    forum_channel_ids = cfg.get('FORUM_CHANNEL_IDS', [])

    if not forum_channel_ids:
        return

    print(f"[{get_vancouver_now().strftime('%Y-%m-%d %H:%M:%S')}] Starting forum thread keep-alive cycle...")

    for channel_id in forum_channel_ids:
        try:
            forum = bot.get_channel(channel_id)
            if not forum:
                continue

            for thread in forum.threads:
                try:
                    await thread.edit(archived=False)
                    await asyncio.sleep(1)
                except (discord.Forbidden, discord.HTTPException):
                    continue

            try:
                async for thread in forum.archived_threads(limit=None):
                    try:
                        await thread.edit(archived=False)
                        await asyncio.sleep(1)
                    except (discord.Forbidden, discord.HTTPException):
                        continue
            except (discord.Forbidden, discord.HTTPException):
                pass

        except Exception:
            continue

    print(f"[{get_vancouver_now().strftime('%Y-%m-%d %H:%M:%S')}] Forum thread keep-alive cycle complete.")


@tasks.loop(hours=48)
async def gizmo_unprompted_loop():
    """Every 48 hours, if the chatbot is enabled and a human has sent a message
    in a chatbot channel within the last 48 hours, Gizmo sends an unprompted message.
    Skips if the last message in the channel is already from Gizmo.
    """
    if not chatbot_is_enabled():
        return

    watched = database.get_watched_channels()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)

    for channel_id in watched:
        channel = bot.get_channel(channel_id)
        if not channel:
            continue

        try:
            messages = []
            async for msg in channel.history(limit=20):
                messages.append(msg)

            if not messages:
                continue

            # Skip if last message is from Gizmo
            if messages[0].author.id == bot.user.id:
                continue

            # Skip if no human message in the last 6 hours
            recent_human = any(
                not msg.author.bot and msg.created_at >= cutoff
                for msg in messages
            )
            if not recent_human:
                continue

            # Build context from today's messages
            channel_messages = await fetch_todays_messages(channel, bot.user)

            prompt = (
                "You have not spoken in a while. The channel has been quiet. "
                "Send a short unprompted message to engage the community. "
                "It could be an observation about game dev, a question, a dry remark, "
                "or anything that feels natural for you. Keep it to 1-2 sentences. "
                "Do not reference that you haven't spoken in a while. "
                "Do not mention kudos or the kudos system. "
                "Do not mention Pikmin. "
                "Use the REPLY: format only."
            )

            project_summaries = await get_nudge_thread_summaries()
            response = await query_gizmo(
                channel_messages,
                prompt,
                "System",
                True,
                project_summaries=project_summaries
            )

            if response and "REPLY:" in response:
                reply_text = response[response.index("REPLY:") + len("REPLY:"):].strip()
                if reply_text:
                    await channel.send(reply_text)
                    set_chatbot_cooldown()
                    print(f"Gizmo sent unprompted message in #{channel.name}")

        except Exception as e:
            print(f"Error in gizmo_unprompted_loop for channel {channel_id}: {e}")


if __name__ == "__main__":
    bot.run(os.environ.get('TOKEN'))
