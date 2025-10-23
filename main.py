import os
import json
from datetime import datetime, timezone, date
from dateutil.relativedelta import relativedelta
import discord
from discord.ext import tasks, commands
import database


def load_config():
    """Loads the configuration from config.json."""
    with open('config.json', 'r') as f:
        return json.load(f)

def save_config(data):
    """Saves the given data to config.json.

    Args:
        data (dict): The configuration data to save.
    """
    with open('config.json', 'w') as f:
        json.dump(data, f, indent=4)

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
    """Calculates the datetime for the start of the next month.

    Returns:
        datetime: The datetime object for the first day of the next month.
    """
    today = datetime.now()
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
            description=f"Award commendations by reacting with {emoji_string}.\n\n",
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
                f"{rank} `{display_name}` → `{user_row['monthly_kudos']} Commendations`"
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

    reactor = payload.member
    creator = message.author

    if reactor.bot or creator.bot or reactor.id == creator.id:
        return
    
    database.reset_daily_limit_if_needed(reactor.id)
    reactor_data = database.get_or_create_user(reactor.id)
    
    if reactor_data['daily_awards_given'] >= config['DAILY_AWARD_LIMIT']:
        try:
            await message.remove_reaction(payload.emoji, reactor)
            await channel.send(
                f"I'm afraid I can't do that, {reactor.mention}. You have no more commendations to allocate today. Your operational enthusiasm has been noted.", 
                delete_after=10
            )
        except discord.Forbidden:
            print(f"Could not remove reaction in {channel.name} due to permissions.")
        return

    database.get_or_create_user(creator.id)
    database.award_kudos(creator.id, reactor.id)
    database.log_kudos(message.id, reactor.id, creator.id)
    print(f"Commendation allocated: {reactor.display_name} -> {creator.display_name}")

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
        print(f"Commendation retracted: {reactor.display_name} from {creator.display_name}")
        await channel.send(
            f"I have processed your request, {reactor.mention}. The commendation has been retracted. Daily allocation limits are final.",
            delete_after=10
        )
    else:
        print(f"Request from {reactor.display_name} to retract commendation ignored: No corresponding record in the log.")


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


@tasks.loop(seconds=10)
async def update_leaderboard_loop():
    """Periodically updates the leaderboard message every 30 seconds."""
    await update_leaderboard_message()

@tasks.loop(hours=1)
async def daily_maintenance_loop():
    """Runs daily maintenance tasks, such as kudos decay and the Top Performer bonus."""
    config = load_config()
    today = str(date.today())
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
                        await channel.send(f"**Special Commendation**\n\nUnit {top_performer_member.mention} has been awarded a bonus of +{config['TOP_PERFORMER_BONUS']} Kudos for exceptional performance parameters.")
                    except discord.NotFound:
                        print(f"Daily Top Performer winner {top_performer_id} not found.")
        
        config["LAST_MAINTENANCE_DATE"] = today
        save_config(config)
        print("--- Daily maintenance complete. ---")

@tasks.loop(hours=24)
async def monthly_reset_loop():
    """Checks daily if it's the first of the month to run the monthly reset."""
    today = datetime.now()
    if today.day == 1:
        print("Running monthly reset...")
        winner_data = database.monthly_reset()
        if winner_data:
            config = load_config()
            guild = bot.get_guild(int(config['GUILD_ID']))
            if not guild:
                return

            try:
                winner_member = await guild.fetch_member(winner_data['user_id'])
                new_level = str(winner_data['lifetime_level'])
                
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

if __name__ == "__main__":
    bot.run(os.environ.get('TOKEN'))
