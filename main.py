import os
import json
import asyncio
import discord
import pytz
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from discord.ext import tasks, commands

import database
from database import get_vancouver_now, get_vancouver_today

# Timezone configuration
VANCOUVER_TZ = pytz.timezone('America/Vancouver')

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

async def update_leaderboard_message():
    """Fetches leaderboard data and updates the leaderboard message embed."""
    config = load_config()
    channel_id = config.get('LEADERBOARD_CHANNEL_ID')
    message_id = config.get('LEADERBOARD_MESSAGE_ID')

    if not channel_id or not message_id:
        return

    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            print(f"Error: Channel with ID {channel_id} not found.")
            return

        message = await channel.fetch_message(message_id)
        users_data = database.get_leaderboard_data()

        kudos_emoji = discord.utils.get(channel.guild.emojis, name=config['KUDOS_EMOJI'])
        emoji_string = str(kudos_emoji) if kudos_emoji else f":{config['KUDOS_EMOJI']}:"

        embed = discord.Embed(
            title="PERFORMANCE LOG",
            description=f"Award kudos by reacting with {emoji_string}.\n\n",
            color=discord.Color(0xFFFF00)
        )

        leaderboard_entries = []
        for i, user_row in enumerate(users_data[:10], 1):
            member = channel.guild.get_member(user_row['user_id'])
            rank = f"`{i}.`"
            display_name = member.display_name if member else f"User ID: {user_row['user_id']}"

            leaderboard_entries.append(
                f"{rank} `{display_name}` → `{user_row['monthly_kudos']} Kudos`"
            )

        leaderboard_string = "\n".join(leaderboard_entries)
        if not leaderboard_string:
            leaderboard_string = "No performance data logged at this time."

        embed.description += leaderboard_string
        embed.set_footer(text=f"This assessment cycle concludes on {get_next_month().strftime('%B %d')}.")

        await message.edit(content=None, embed=embed)

    except discord.NotFound:
        print("Error: Leaderboard message not found.")
    except Exception as e:
        print(f"An error occurred while updating the leaderboard: {e}")

async def update_history_message():
    """Fetches monthly history data and updates the history message embed."""
    config = load_config()
    channel_id = config.get('LEADERBOARD_CHANNEL_ID')
    message_id = config.get('HISTORY_MESSAGE_ID')

    if not channel_id or not message_id:
        return

    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            print(f"Error: Channel with ID {channel_id} not found.")
            return

        message = await channel.fetch_message(message_id)
        history_data = database.get_monthly_history()

        embed = discord.Embed(
            title="PERFORMANCE HISTORY",
            description="Records of past assessment cycle winners.\n\n",
            color=discord.Color(0xFFFF00)
        )

        if not history_data:
            embed.description += "No historical records available."
        else:
            history_entries = []
            for record in history_data:
                member = channel.guild.get_member(record['user_id'])
                display_name = member.display_name if member else f"User ID: {record['user_id']}"

                month_obj = datetime.strptime(record['month'], '%Y-%m')
                month_display = month_obj.strftime('%B %Y')

                history_entries.append(
                    f"`{month_display}` → **{display_name}** • `{record['monthly_kudos']} Kudos` • Level {record['new_level']}"
                )

            embed.description += "\n".join(history_entries)

        embed.set_footer(text="Historical data is preserved permanently.")

        await message.edit(content=None, embed=embed)

    except discord.NotFound:
        print("Error: History message not found.")
    except Exception as e:
        print(f"An error occurred while updating the history: {e}")

async def _sync_roles_helper(guild: discord.Guild):
    """Synchronizes roles for all members based on their lifetime level in the database."""
    print("Starting role synchronization...")
    config = load_config()
    all_level_role_ids = {int(role_id) for role_id in config['LEVEL_ROLES'].values()}

    for member in guild.members:
        if member.bot:
            continue

        user_data = database.get_or_create_user(member.id)
        db_level = str(user_data['lifetime_level'])
        target_role_id = config['LEVEL_ROLES'].get(db_level)

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

        update_leaderboard_loop.start()
        daily_maintenance_loop.start()
        monthly_reset_loop.start() 
        keep_forum_threads_alive.start()
        bot.setup_done = True
    else:
        print(f"Bot reconnected as {bot.user}")

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """Handles awarding kudos when a user adds the configured kudos emoji to a message."""
    if payload.emoji.name != config['KUDOS_EMOJI'] or payload.guild_id is None:
        return

    channel = bot.get_channel(payload.channel_id)
    if not channel:
        return

    try:
        message = await channel.fetch_message(payload.message_id)
    except discord.NotFound:
        return

    message_age = datetime.now(timezone.utc) - message.created_at
    if message_age.days > config['KUDOS_VALIDITY_DAYS']:
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

    if payload.user_id == bot.user.id:
        database.get_or_create_user(creator.id)
        database.award_daily_greeting_kudos(creator.id, bot.user.id)
        database.log_kudos(message.id, bot.user.id, creator.id)
        print(f"Daily greeting kudos allocated: BOT -> {creator.display_name}")
        return

    reactor = payload.member
    if not reactor:
        return

    if reactor.id == creator.id:
        return

    database.reset_daily_limit_if_needed(reactor.id)
    reactor_data = database.get_or_create_user(reactor.id)

    if reactor_data['daily_awards_given'] >= config['DAILY_AWARD_LIMIT']:
        try:
            await message.remove_reaction(payload.emoji, reactor)
            await channel.send(
                f"I'm afraid I can't do that, {reactor.mention}. You have no more kudos to allocate today. Your operational enthusiasm has been noted.",
                delete_after=10
            )
        except discord.Forbidden:
            print(f"Could not remove reaction in {channel.name} due to permissions.")
        return

    database.get_or_create_user(creator.id)
    database.award_kudos(creator.id, reactor.id)
    database.log_kudos(message.id, reactor.id, creator.id)
    print(f"Kudos allocated: {reactor.display_name} -> {creator.display_name}")

@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    """Handles removing kudos when a user removes the configured kudos emoji from a message."""
    if payload.emoji.name != config['KUDOS_EMOJI'] or payload.guild_id is None:
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
        if message_age.days > config['KUDOS_VALIDITY_DAYS']:
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
        await channel.send(
            f"I have processed your request, {reactor.mention}. The kudos has been retracted. Daily allocation limits are final.",
            delete_after=10
        )
    else:
        print(f"Request from {reactor.display_name} to retract kudos ignored: No corresponding record in the log.")

@bot.event
async def on_message(message: discord.Message):
    """Handles daily first message greetings and kudos awards."""
    if message.author.bot:
        await bot.process_commands(message)
        return

    if message.guild is None:
        await bot.process_commands(message)
        return

    user_data = database.get_or_create_user(message.author.id)
    today = get_vancouver_today()

    if user_data['last_message_date'] != today:
        is_returning_user = user_data['last_message_date'] is not None
        database.update_last_message_date(message.author.id, today)

        if is_returning_user:
            kudos_emoji = discord.utils.get(message.guild.emojis, name=config['KUDOS_EMOJI'])

            if kudos_emoji:
                try:
                    await message.add_reaction(kudos_emoji)
                    print(f"Daily kudos awarded to {message.author.display_name}")

                    global_greeting_enabled = config.get('DAILY_GREETING_ENABLED', True)
                    user_greeting_enabled = user_data['greeting_enabled'] if user_data['greeting_enabled'] is not None else 1

                    if global_greeting_enabled and user_greeting_enabled == 1:
                        await message.reply(
                            f"Affirmative, {message.author.mention}. Your return has been logged. "
                            f"One kudos unit allocated. These notifications may be disabled via the !toggle_greeting command.",
                            delete_after=30
                        )
                        print(f"Daily greeting message sent to {message.author.display_name}")

                except discord.Forbidden:
                    print(f"Could not add reaction or send greeting in {message.channel.name} due to permissions.")
        else:
            print(f"New user detected: {message.author.display_name}. Last message date initialized.")

    await bot.process_commands(message)

# ==========================================
# COMMANDS
# ==========================================

@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def init_leaderboard(ctx: commands.Context):
    config = load_config()
    embed = discord.Embed(title="PERFORMANCE LOG", description="Initializing performance log. Standby.", color=discord.Color(0xFFFF00))
    message = await ctx.send(embed=embed)

    config['LEADERBOARD_CHANNEL_ID'] = ctx.channel.id
    config['LEADERBOARD_MESSAGE_ID'] = message.id
    save_config(config)

    await ctx.message.delete()
    await update_leaderboard_message()
    await ctx.send("The performance log is now operational.", delete_after=5)

@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def init_history(ctx: commands.Context):
    config = load_config()
    embed = discord.Embed(title="PERFORMANCE HISTORY", description="Initializing performance history. Standby.", color=discord.Color(0x00BFFF))
    message = await ctx.send(embed=embed)

    config['HISTORY_MESSAGE_ID'] = message.id
    save_config(config)

    await ctx.message.delete()
    await update_history_message()
    await ctx.send("The performance history log is now operational.", delete_after=5)

@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def sync_roles(ctx: commands.Context):
    await ctx.message.delete()
    await ctx.send("Affirmative. Initiating manual role synchronization protocol.", delete_after=10)
    await _sync_roles_helper(ctx.guild)
    await ctx.send("Role synchronization protocol complete. All unit designations are now nominal.", delete_after=10)

@bot.command()
async def reset_daily_limits(ctx: commands.Context, member: discord.Member = None):
    if ctx.author.id != 437871588864425986:
        await ctx.send(f"I'm afraid I can't do that, {ctx.author.mention}. This command is restricted to authorized personnel only.", delete_after=10)
        await ctx.message.delete()
        return

    await ctx.message.delete()
    if member:
        database.reset_daily_limits(member.id)
        await ctx.send(f"Affirmative. Daily allocation parameters for {member.mention} have been reset.", delete_after=10)
    else:
        database.reset_daily_limits()
        await ctx.send("Affirmative. Daily allocation parameters for all units have been reset.", delete_after=10)

@bot.command()
async def test_embed(ctx: commands.Context):
    kudos_emoji = discord.utils.get(ctx.guild.emojis, name=config['KUDOS_EMOJI'])
    if kudos_emoji:
        await ctx.send(f"Custom emoji test: {kudos_emoji}")
        embed = discord.Embed(title=f"Embed with {kudos_emoji}!", description="The emoji works!", color=discord.Color(0xFFFF00))
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"Could not find an emoji named `{config['KUDOS_EMOJI']}`.")

@bot.command()
async def toggle_greeting(ctx: commands.Context):
    new_state = database.toggle_user_greeting(ctx.author.id)
    if new_state:
        confirmation = f"Affirmative, {ctx.author.mention}. Daily greeting notifications have been **enabled**."
    else:
        confirmation = f"Acknowledged, {ctx.author.mention}. Daily greeting notifications have been **disabled**."
    await ctx.message.delete()
    await ctx.send(confirmation, delete_after=10)

@bot.command()
async def add_kudos(ctx: commands.Context, member: discord.Member = None, amount: int = None):
    if ctx.author.id != 437871588864425986:
        await ctx.send(f"I'm afraid I can't do that, {ctx.author.mention}. Restricted command.", delete_after=10)
        await ctx.message.delete()
        return

    if member is None or amount is None or amount <= 0:
        await ctx.send(f"Invalid parameters. Usage: `!add_kudos @user amount`", delete_after=10)
        await ctx.message.delete()
        return

    database.get_or_create_user(member.id)
    conn = database.get_db_connection()
    conn.execute('UPDATE users SET monthly_kudos = monthly_kudos + ? WHERE user_id = ?', (amount, member.id))
    conn.commit()
    conn.close()

    await ctx.message.delete()
    await ctx.send(f"Affirmative. **{amount}** kudos allocated to {member.mention}.", delete_after=10)

@bot.command()
async def fix_october_reset(ctx: commands.Context):
    if ctx.author.id != 437871588864425986:
        return
    # Code omitted for brevity as this is a specific one-time fix
    pass

@bot.command()
async def systemtime(ctx: commands.Context):
    now_vancouver = get_vancouver_now()
    now_local = datetime.now()
    now_utc = datetime.now(timezone.utc)
    today_vancouver = get_vancouver_today()

    embed = discord.Embed(title="System Time (Debug Info)", color=discord.Color(0xFFFF00))
    embed.add_field(name="Vancouver Time", value=f"`{now_vancouver.strftime('%Y-%m-%d %H:%M:%S %Z')}`", inline=False)
    embed.add_field(name="Vancouver Date", value=f"`{today_vancouver}`", inline=False)
    embed.add_field(name="UTC Time", value=f"`{now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}`", inline=False)

    await ctx.send(embed=embed, delete_after=30)
    await ctx.message.delete()

# ==========================================
# NEW RECOVERY COMMANDS
# ==========================================

@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def recover_missed_kudos(ctx: commands.Context, days_to_look_back: int):
    """(Admin) Scans the last X days for missed user-to-user kudos, including threads."""
    
    await ctx.send(f"Initiating deep scan of the last {days_to_look_back} days across all channels and threads. Standby.")
    
    config_kudos = load_config()
    kudos_emoji_name = config_kudos['KUDOS_EMOJI']
    target_date = datetime.now(timezone.utc) - timedelta(days=days_to_look_back)
    
    # --- STEP 1: Gather ALL channels and threads ---
    channels_to_scan = []
    channels_to_scan.extend(ctx.guild.text_channels)
    channels_to_scan.extend(ctx.guild.voice_channels)
    channels_to_scan.extend(ctx.guild.threads)
    
    for channel in ctx.guild.text_channels + ctx.guild.forums:
        try:
            async for thread in channel.archived_threads(limit=None, after=target_date):
                if thread not in channels_to_scan:
                    channels_to_scan.append(thread)
        except (discord.Forbidden, AttributeError):
            continue

    total_recovered = 0
    messages_scanned = 0

    # --- STEP 2: Scan for Kudos ---
    for channel in channels_to_scan:
        try:
            async for message in channel.history(limit=None, after=target_date):
                messages_scanned += 1
                
                for reaction in message.reactions:
                    if getattr(reaction.emoji, 'name', reaction.emoji) == kudos_emoji_name:
                        
                        async for user in reaction.users():
                            if user.bot or message.author.bot or user.id == message.author.id:
                                continue 
                            
                            if not database.check_kudos_exists(message.id, user.id):
                                database.get_or_create_user(message.author.id)
                                database.get_or_create_user(user.id)
                                
                                conn = database.get_db_connection()
                                conn.execute('UPDATE users SET monthly_kudos = monthly_kudos + 2 WHERE user_id = ?', (message.author.id,))
                                conn.execute('UPDATE users SET monthly_kudos = monthly_kudos + 1 WHERE user_id = ?', (user.id,))
                                conn.commit()
                                conn.close()
                                
                                database.log_kudos(message.id, user.id, message.author.id)
                                total_recovered += 1
                                
            await asyncio.sleep(0.5) # Prevent rate-limiting
            
        except discord.Forbidden:
            pass
        except Exception as e:
            print(f"Error scanning {channel.name}: {e}")

    await ctx.send(f"**Scan Complete.**\nScanned `{messages_scanned}` messages in channels & threads.\nRecovered and applied `{total_recovered}` missing kudos interactions.")
    await update_leaderboard_message()


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def recover_daily_greetings(ctx: commands.Context, weeks: int):
    """(Admin) Scans for missed daily first-message bot kudos, including threads."""
    
    await ctx.send(f"Initiating temporal scan of the last {weeks} week(s). Grouping messages by Vancouver daily cycles to locate missed greetings. This will take a while. Standby.")
    
    config_kudos = load_config()
    kudos_emoji_name = config_kudos['KUDOS_EMOJI']
    start_date = datetime.now(timezone.utc) - timedelta(weeks=weeks)
    
    earliest_messages = defaultdict(dict)
    
    # --- STEP 1: Gather ALL channels and threads ---
    channels_to_scan = []
    channels_to_scan.extend(ctx.guild.text_channels)
    channels_to_scan.extend(ctx.guild.voice_channels)
    channels_to_scan.extend(ctx.guild.threads)
    
    for channel in ctx.guild.text_channels + ctx.guild.forums:
        try:
            async for thread in channel.archived_threads(limit=None, after=start_date):
                if thread not in channels_to_scan:
                    channels_to_scan.append(thread)
        except (discord.Forbidden, AttributeError):
            continue

    messages_scanned = 0
    channels_scanned = 0
    
    # --- STEP 2: Scan and sort messages ---
    for channel in channels_to_scan:
        try:
            channels_scanned += 1
            async for message in channel.history(limit=None, after=start_date):
                if message.author.bot:
                    continue
                    
                messages_scanned += 1
                
                vancouver_time = message.created_at.astimezone(VANCOUVER_TZ)
                date_str = vancouver_time.date().isoformat()
                user_id = message.author.id
                
                if user_id not in earliest_messages[date_str]:
                    earliest_messages[date_str][user_id] = message
                elif message.created_at < earliest_messages[date_str][user_id].created_at:
                    earliest_messages[date_str][user_id] = message
                    
        except discord.Forbidden:
            pass # Skip silently if no access
        except Exception as e:
            print(f"Error scanning {channel.name}: {e}")

    # --- STEP 3: Verify and apply missing bot reactions ---
    recovered_count = 0
    kudos_emoji_obj = discord.utils.get(ctx.guild.emojis, name=kudos_emoji_name) or f":{kudos_emoji_name}:"
    
    for date_str, users in earliest_messages.items():
        for user_id, message in users.items():
            
            bot_reacted = False
            for reaction in message.reactions:
                if getattr(reaction.emoji, 'name', reaction.emoji) == kudos_emoji_name and reaction.me:
                    bot_reacted = True
                    break
            
            if not bot_reacted:
                try:
                    database.get_or_create_user(user_id)
                    if not database.check_kudos_exists(message.id, bot.user.id):
                        await message.add_reaction(kudos_emoji_obj)
                        database.award_daily_greeting_kudos(user_id, bot.user.id)
                        database.log_kudos(message.id, bot.user.id, user_id)
                        recovered_count += 1
                        await asyncio.sleep(1.5) # Prevent rate limits
                        
                except discord.Forbidden:
                    pass
                except Exception as e:
                    print(f"Error recovering daily kudos for {user_id}: {e}")

    await ctx.send(f"**Daily Greeting Recovery Complete.**\nScanned `{messages_scanned}` messages across `{channels_scanned}` channels and threads.\nRecovered `{recovered_count}` missed daily bot kudos.")


# ==========================================
# TASKS
# ==========================================

@tasks.loop(seconds=10)
async def update_leaderboard_loop():
    await update_leaderboard_message()

@tasks.loop(hours=1)
async def daily_maintenance_loop():
    config = load_config()
    today = get_vancouver_today()
    last_run_date = database.get_system_state("LAST_MAINTENANCE_DATE")

    if last_run_date != today:
        print("--- Running daily maintenance... ---")
        top_performer_id = database.apply_daily_maintenance(config['KUDOS_DECAY'], config['TOP_PERFORMER_BONUS'])
        if top_performer_id and config['TOP_PERFORMER_BONUS'] > 0:
            channel = bot.get_channel(config['LEADERBOARD_CHANNEL_ID'])
            if channel:
                guild = bot.get_guild(int(config['GUILD_ID']))
                if guild:
                    try:
                        top_performer_member = await guild.fetch_member(top_performer_id)
                        await channel.send(f"**Special Kudos**\n\nUnit {top_performer_member.mention} has been awarded a bonus of +{config['TOP_PERFORMER_BONUS']} Kudos for exceptional performance parameters.")
                    except discord.NotFound:
                        print(f"Daily Top Performer winner {top_performer_id} not found.")

        database.set_system_state("LAST_MAINTENANCE_DATE", today)
        print("--- Daily maintenance complete. ---")

@tasks.loop(hours=1)
async def monthly_reset_loop():
    """Checks hourly if we have entered a new month to run the monthly reset."""
    config = load_config()
    today = get_vancouver_today() # Format: 'YYYY-MM-DD'
    today_obj = get_vancouver_now()
    last_reset_date = database.get_system_state("LAST_MONTHLY_RESET_DATE")

    current_month_str = today_obj.strftime('%Y-%m')

    if not last_reset_date:
        print("No previous monthly reset date found. Initializing to today.")
        database.set_system_state("LAST_MONTHLY_RESET_DATE", today)
        return

    last_reset_month_str = last_reset_date[:7]

    # Trigger reset if current month does not match the month we last reset in
    if current_month_str != last_reset_month_str:
        print("--- New month detected! Running catch-up monthly reset... ---")
        winner_data = database.monthly_reset()
        
        if winner_data:
            guild = bot.get_guild(int(config['GUILD_ID']))
            if not guild:
                return

            try:
                winner_member = await guild.fetch_member(winner_data['user_id'])
                new_level = str(winner_data['lifetime_level'] + 1)

                if new_level in config['LEVEL_ROLES']:
                    new_role_id = int(config['LEVEL_ROLES'][new_level])
                    new_role = guild.get_role(new_role_id)

                    roles_to_remove_ids = [int(rid) for lid, rid in config['LEVEL_ROLES'].items() if lid != new_level]
                    roles_to_remove = [role for role in winner_member.roles if role.id in roles_to_remove_ids]
                    if roles_to_remove:
                        await winner_member.remove_roles(*roles_to_remove)

                    if new_role:
                        await winner_member.add_roles(new_role)

                channel = bot.get_channel(config['LEADERBOARD_CHANNEL_ID'])
                if channel:
                    await channel.send(f"**Performance Cycle Update**\n\nThe previous performance cycle has concluded. Unit {winner_member.mention} has been designated Top Performer, achieving **Level {new_level}**. All logs have been archived and reset for the new cycle.")
            except (discord.NotFound, Exception) as e:
                print(f"An error occurred during monthly reset announcement: {e}")

        await update_leaderboard_message()
        await update_history_message()

        database.set_system_state("LAST_MONTHLY_RESET_DATE", today)
        print("--- Monthly reset complete. ---")

@tasks.loop(hours=config.get('FORUM_BUMP_HOURS', 167))
async def keep_forum_threads_alive():
    config = load_config()
    forum_channel_ids = config.get('FORUM_CHANNEL_IDS', [])

    if not forum_channel_ids:
        return

    print(f"[{get_vancouver_now().strftime('%Y-%m-%d %H:%M:%S')}] Starting forum thread keep-alive cycle...")

    for channel_id in forum_channel_ids:
        try:
            forum = bot.get_channel(channel_id)
            if not forum:
                continue

            active_count = 0
            for thread in forum.threads:
                try:
                    await thread.edit(archived=False)
                    active_count += 1
                    await asyncio.sleep(1) 
                except (discord.Forbidden, discord.HTTPException):
                    continue

            archived_count = 0
            try:
                async for thread in forum.archived_threads(limit=None):
                    try:
                        await thread.edit(archived=False)
                        archived_count += 1
                        await asyncio.sleep(1) 
                    except (discord.Forbidden, discord.HTTPException):
                        continue
            except (discord.Forbidden, discord.HTTPException):
                pass

        except Exception as e:
            continue

    print(f"[{get_vancouver_now().strftime('%Y-%m-%d %H:%M:%S')}] Forum thread keep-alive cycle complete.")

if __name__ == "__main__":
    bot.run(os.environ.get('TOKEN'))