import os
import json
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import asyncio
import discord
from discord.ext import tasks, commands
import database
from database import get_vancouver_now, get_vancouver_today


def load_config():
    """Loads the configuration from config.json."""
    with open('config.json', 'r') as f:
        return json.load(f)

def save_config(data):
    """Saves the given data to config.json.

    Args:
        data (dict): The configuration data to save.
    """
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
    """Calculates the datetime for the start of the next month in Vancouver timezone.

    Returns:
        datetime: The datetime object for the first day of the next month in Vancouver timezone.
    """
    today = get_vancouver_now()
    return (today.replace(day=1) + relativedelta(months=1)).replace(hour=0, minute=0, second=0)

async def update_leaderboard_message():
    """Fetches leaderboard data and updates the leaderboard message embed.
    
    If the leaderboard channel or message is not configured, this function does nothing.
    """
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
        rank_emojis = {1: "🥇", 2: "🥈", 3: "🥉"}
        
        for i, user_row in enumerate(users_data[:10], 1):
            member = channel.guild.get_member(user_row['user_id'])
            #rank = rank_emojis.get(i, f"**{i}.**")
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
    """Fetches monthly history data and updates the history message embed.

    If the history message is not configured, this function does nothing.
    """
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

                # Parse month (YYYY-MM) and format it nicely
                from datetime import datetime
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
    """Synchronizes roles for all members based on their lifetime level in the database.

    This function ensures each user has the correct level role and removes any incorrect ones.

    Args:
        guild (discord.Guild): The guild to sync roles in.
    """
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
    """Handles bot startup, initial setup, and task launching. 
    
    This event is triggered once the bot is logged in and ready. It ensures that
    database setup, role synchronization, and background tasks are started correctly.
    """
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
    """Handles awarding kudos when a user adds the configured kudos emoji to a message.

    Args:
        payload (discord.RawReactionActionEvent): The event payload for the reaction.
    """
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

    # Don't allow bots to receive kudos
    if creator.bot:
        return

    # Special handling for bot reactions (daily greeting system)
    # Check user_id directly since payload.member might be None for bot
    if payload.user_id == bot.user.id:
        database.get_or_create_user(creator.id)
        database.award_daily_greeting_kudos(creator.id, bot.user.id)
        database.log_kudos(message.id, bot.user.id, creator.id)
        print(f"Daily greeting kudos allocated: BOT -> {creator.display_name}")
        return

    reactor = payload.member
    if not reactor:
        return

    # Don't allow self-kudos
    if reactor.id == creator.id:
        return

    # Normal user reaction handling
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
    """Handles removing kudos when a user removes the configured kudos emoji from a message.

    Args:
        payload (discord.RawReactionActionEvent): The event payload for the reaction.
    """
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
    """Handles daily first message greetings and kudos awards.

    When a user sends their first message of a new day, the bot awards them kudos
    and optionally sends a greeting message (if the user has greetings enabled).

    Args:
        message (discord.Message): The message object.
    """
    # Skip bot messages
    if message.author.bot:
        await bot.process_commands(message)
        return

    # Skip DMs (only process guild messages)
    if message.guild is None:
        await bot.process_commands(message)
        return

    # Get or create user
    user_data = database.get_or_create_user(message.author.id)
    today = get_vancouver_today()

    # Check if this is the user's first message of the day (but not their first message ever)
    if user_data['last_message_date'] != today:
        # Only award kudos/greeting if user has messaged before (returning user)
        is_returning_user = user_data['last_message_date'] is not None

        # Always update last message date
        database.update_last_message_date(message.author.id, today)

        # Only award kudos and send greeting for returning users
        if is_returning_user:
            # Get the kudos emoji
            kudos_emoji = discord.utils.get(message.guild.emojis, name=config['KUDOS_EMOJI'])

            if kudos_emoji:
                try:
                    # Always add kudos reaction (triggers on_raw_reaction_add with bot logic)
                    await message.add_reaction(kudos_emoji)
                    print(f"Daily kudos awarded to {message.author.display_name}")

                    # Send greeting message only if both config and user settings allow it
                    global_greeting_enabled = config.get('DAILY_GREETING_ENABLED', True)
                    user_greeting_enabled = user_data['greeting_enabled'] if user_data['greeting_enabled'] is not None else 1

                    if global_greeting_enabled and user_greeting_enabled == 1:
                        greeting_message = await message.reply(
                            f"Affirmative, {message.author.mention}. Your return has been logged. "
                            f"One kudos unit allocated. These notifications may be disabled via the !toggle_greeting command.",
                            delete_after=30
                        )
                        print(f"Daily greeting message sent to {message.author.display_name}")

                except discord.Forbidden:
                    print(f"Could not add reaction or send greeting in {message.channel.name} due to permissions.")
        else:
            print(f"New user detected: {message.author.display_name}. Last message date initialized.")

    # Important: Process commands
    await bot.process_commands(message)


@bot.command()
@commands.has_role(int(config['ADMIN_ROLE_ID']))
async def init_leaderboard(ctx: commands.Context):
    """(Admin) Initializes the PERFORMANCE LOG in the current channel.

    This command creates the log message and saves its ID and channel ID to the config.

    Args:
        ctx (commands.Context): The context of the command invocation.
    """
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
    """(Admin) Initializes the PERFORMANCE HISTORY message in the current channel.

    This command creates the history message and saves its ID to the config.

    Args:
        ctx (commands.Context): The context of the command invocation.
    """
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
    """(Admin) Manually triggers a synchronization of roles for all members.

    Args:
        ctx (commands.Context): The context of the command invocation.
    """
    await ctx.message.delete()
    await ctx.send("Affirmative. Initiating manual role synchronization protocol.", delete_after=10)
    await _sync_roles_helper(ctx.guild)
    await ctx.send("Role synchronization protocol complete. All unit designations are now nominal.", delete_after=10)

@bot.command()
async def reset_daily_limits(ctx: commands.Context, member: discord.Member = None):
    """(Owner Only) Resets daily award limits for a user or all users.

    This command can only be used by the bot owner (user ID: 437871588864425986).
    This allows users to give kudos again immediately by resetting their
    daily_awards_given to 0 and last_award_date to NULL.

    Usage:
    - !reset_daily_limits @user (resets specific user)
    - !reset_daily_limits (resets all users)

    Args:
        ctx (commands.Context): The context of the command invocation.
        member (discord.Member): Optional member to reset. If None, resets all users.
    """
    # Check if the user is authorized
    if ctx.author.id != 437871588864425986:
        await ctx.send(
            f"I'm afraid I can't do that, {ctx.author.mention}. This command is restricted to authorized personnel only.",
            delete_after=10
        )
        await ctx.message.delete()
        return

    await ctx.message.delete()

    if member:
        # Reset specific user
        database.reset_daily_limits(member.id)
        await ctx.send(
            f"Affirmative. Daily allocation parameters for {member.mention} have been reset. "
            f"Unit may now allocate kudos.",
            delete_after=10
        )
        print(f"Daily limits reset for {member.display_name} by {ctx.author.display_name}")
    else:
        # Reset all users
        database.reset_daily_limits()
        await ctx.send(
            f"Affirmative. Daily allocation parameters for all units have been reset. "
            f"All personnel may now allocate kudos.",
            delete_after=10
        )
        print(f"Daily limits reset for ALL users by {ctx.author.display_name}")

@bot.command()
async def test_embed(ctx: commands.Context):
    """A test command to check if the custom kudos emoji can be found and displayed.

    Args:
        ctx (commands.Context): The context of the command invocation.
    """
    kudos_emoji = discord.utils.get(ctx.guild.emojis, name=config['KUDOS_EMOJI'])
    if kudos_emoji:
        await ctx.send(f"Custom emoji test: {kudos_emoji}")
        embed = discord.Embed(title=f"Embed with {kudos_emoji}!", description="The emoji works!", color=discord.Color(0xFFFF00))
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"Could not find an emoji named `{config['KUDOS_EMOJI']}`.")

@bot.command()
async def toggle_greeting(ctx: commands.Context):
    """Toggles daily greeting notifications for the user.

    When enabled, the bot will send a greeting message when the user sends their
    first message of a new day. Kudos are still awarded regardless of this setting.

    Args:
        ctx (commands.Context): The context of the command invocation.
    """
    new_state = database.toggle_user_greeting(ctx.author.id)

    if new_state:
        status_message = "enabled"
        confirmation = f"Affirmative, {ctx.author.mention}. Daily greeting notifications have been **enabled**. You will receive acknowledgment messages for your first transmission of each day."
    else:
        status_message = "disabled"
        confirmation = f"Acknowledged, {ctx.author.mention}. Daily greeting notifications have been **disabled**. Kudos allocation will continue without verbal acknowledgment."

    await ctx.message.delete()
    await ctx.send(confirmation, delete_after=10)
    print(f"Greeting notifications {status_message} for {ctx.author.display_name}")

@bot.command()
async def add_kudos(ctx: commands.Context, member: discord.Member = None, amount: int = None):
    """(Owner Only) Manually adds kudos to a specified user.

    This command can only be used by the bot owner (user ID: 437871588864425986).

    Usage: !add_kudos @user amount

    Args:
        ctx (commands.Context): The context of the command invocation.
        member (discord.Member): The member to award kudos to.
        amount (int): The number of kudos to add.
    """
    # Check if the user is authorized
    if ctx.author.id != 437871588864425986:
        await ctx.send(
            f"I'm afraid I can't do that, {ctx.author.mention}. This command is restricted to authorized personnel only.",
            delete_after=10
        )
        await ctx.message.delete()
        return

    # Validate parameters
    if member is None or amount is None:
        await ctx.send(
            f"Invalid parameters, {ctx.author.mention}. Usage: `!add_kudos @user amount`",
            delete_after=10
        )
        await ctx.message.delete()
        return

    if amount <= 0:
        await ctx.send(
            f"Error: Kudos amount must be a positive integer.",
            delete_after=10
        )
        await ctx.message.delete()
        return

    # Add kudos
    database.get_or_create_user(member.id)
    conn = database.get_db_connection()
    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos + ? WHERE user_id = ?',
        (amount, member.id)
    )
    conn.commit()
    conn.close()

    # Send confirmation
    await ctx.message.delete()
    await ctx.send(
        f"Affirmative. **{amount}** kudos unit{'s' if amount != 1 else ''} allocated to {member.mention}. Operation complete.",
        delete_after=10
    )
    print(f"Manual kudos: {ctx.author.display_name} added {amount} to {member.display_name}")

@bot.command()
async def systemtime(ctx: commands.Context):
    """Displays the current system time for debugging purposes.

    Shows the system time in multiple formats including local time, UTC,
    and ISO format for timezone debugging.

    Args:
        ctx (commands.Context): The context of the command invocation.
    """
    now_vancouver = get_vancouver_now()
    now_local = datetime.now()
    now_utc = datetime.now(timezone.utc)
    today_vancouver = get_vancouver_today()

    embed = discord.Embed(
        title="System Time (Debug Info)",
        color=discord.Color(0xFFFF00)
    )
    embed.add_field(name="Vancouver Time (Bot Timezone)", value=f"`{now_vancouver.strftime('%Y-%m-%d %H:%M:%S %Z')}`", inline=False)
    embed.add_field(name="Vancouver Date (Used for daily reset)", value=f"`{today_vancouver}`", inline=False)
    embed.add_field(name="Server Local Time", value=f"`{now_local.strftime('%Y-%m-%d %H:%M:%S')}`", inline=False)
    embed.add_field(name="UTC Time", value=f"`{now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}`", inline=False)
    embed.add_field(name="Unix Timestamp", value=f"`{int(now_utc.timestamp())}`", inline=False)

    await ctx.send(embed=embed, delete_after=30)
    await ctx.message.delete()
    print(f"System time debug info requested by {ctx.author.display_name}")


@tasks.loop(seconds=10)
async def update_leaderboard_loop():
    """Periodically updates the leaderboard message every 30 seconds."""
    await update_leaderboard_message()

@tasks.loop(hours=1)
async def daily_maintenance_loop():
    """Runs daily maintenance tasks, such as kudos decay and the Top Performer bonus."""
    config = load_config()
    today = get_vancouver_today()
    last_run_date = config.get("LAST_MAINTENANCE_DATE")

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
        
        config["LAST_MAINTENANCE_DATE"] = today
        save_config(config)
        print("--- Daily maintenance complete. ---")

@tasks.loop(hours=1)
async def monthly_reset_loop():
    """Checks hourly if it's the first of the month to run the monthly reset."""
    config = load_config()
    today = get_vancouver_today()
    today_obj = get_vancouver_now()
    last_reset_date = config.get("LAST_MONTHLY_RESET_DATE")

    # Only reset if it's the 1st of the month AND we haven't reset this month yet
    if today_obj.day == 1 and last_reset_date != today:
        print("--- Running monthly reset... ---")
        winner_data = database.monthly_reset()
        if winner_data:
            guild = bot.get_guild(int(config['GUILD_ID']))
            if not guild:
                return

            try:
                winner_member = await guild.fetch_member(winner_data['user_id'])
                # Get the NEW level (database increments it, so winner_data has old value + 1)
                new_level = str(winner_data['lifetime_level'] + 1)

                # Update roles
                if new_level in config['LEVEL_ROLES']:
                    new_role_id = int(config['LEVEL_ROLES'][new_level])
                    new_role = guild.get_role(new_role_id)

                    # Remove all other level roles
                    roles_to_remove_ids = [int(rid) for lid, rid in config['LEVEL_ROLES'].items() if lid != new_level]
                    roles_to_remove = [role for role in winner_member.roles if role.id in roles_to_remove_ids]
                    if roles_to_remove:
                        await winner_member.remove_roles(*roles_to_remove)

                    if new_role:
                        await winner_member.add_roles(new_role)

                # Announce winner
                channel = bot.get_channel(config['LEADERBOARD_CHANNEL_ID'])
                if channel:
                    await channel.send(f"**Performance Cycle Update**\n\nThe previous performance cycle has concluded. Unit {winner_member.mention} has been designated Top Performer, achieving **Level {new_level}**. All logs have been archived and reset for the new cycle.")
            except (discord.NotFound, Exception) as e:
                print(f"An error occurred during monthly reset announcement: {e}")

        await update_leaderboard_message()
        await update_history_message()

        config["LAST_MONTHLY_RESET_DATE"] = today
        save_config(config)
        print("--- Monthly reset complete. ---")

@tasks.loop(hours=config.get('FORUM_BUMP_HOURS', 167))
async def keep_forum_threads_alive():
    """Automatically bumps all threads in configured forum channels to prevent auto-archiving.

    This task runs every X hours (configured by FORUM_BUMP_HOURS) and:
    - Bumps all active threads with ⏰ emoji
    - Unarchives and bumps all archived threads with 🔄 emoji
    - Deletes bump messages immediately after sending
    """
    config = load_config()
    forum_channel_ids = config.get('FORUM_CHANNEL_IDS', [])

    if not forum_channel_ids:
        return

    print(f"[{get_vancouver_now().strftime('%Y-%m-%d %H:%M:%S')}] Starting forum thread bump cycle...")

    for channel_id in forum_channel_ids:
        try:
            forum = bot.get_channel(channel_id)
            if not forum:
                print(f"Warning: Forum channel {channel_id} not found. Skipping.")
                continue

            # Bump all active threads
            active_count = 0
            for thread in forum.threads:
                try:
                    msg = await thread.send("⏰")
                    await msg.delete()
                    active_count += 1
                    await asyncio.sleep(1)
                except (discord.Forbidden, discord.HTTPException) as e:
                    print(f"Error bumping active thread {thread.name} in forum {channel_id}: {e}")
                    continue

            # Unarchive and bump all archived threads
            archived_count = 0
            try:
                async for thread in forum.archived_threads(limit=None):
                    try:
                        msg = await thread.send("🔄")
                        await msg.delete()
                        archived_count += 1
                        await asyncio.sleep(1)
                    except (discord.Forbidden, discord.HTTPException) as e:
                        print(f"Error bumping archived thread {thread.name} in forum {channel_id}: {e}")
                        continue
            except (discord.Forbidden, discord.HTTPException) as e:
                print(f"Error fetching archived threads for forum {channel_id}: {e}")

            if active_count > 0 or archived_count > 0:
                print(f"Forum {channel_id}: Bumped {active_count} active, {archived_count} archived threads")

        except Exception as e:
            print(f"Error processing forum channel {channel_id}: {e}")
            continue

    print(f"[{get_vancouver_now().strftime('%Y-%m-%d %H:%M:%S')}] Forum thread bump cycle complete.")

if __name__ == "__main__":
    bot.run(os.environ.get('TOKEN'))
