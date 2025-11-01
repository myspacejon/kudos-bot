import sqlite3
from datetime import datetime
import os
import pytz

DB_FILE = "/data/kudos_bot.db"

# Timezone configuration for America/Vancouver (PST/PDT)
VANCOUVER_TZ = pytz.timezone('America/Vancouver')

def get_vancouver_now():
    """Returns the current datetime in America/Vancouver timezone.

    Returns:
        datetime: Timezone-aware datetime object for Vancouver.
    """
    return datetime.now(VANCOUVER_TZ)

def get_vancouver_today():
    """Returns today's date in America/Vancouver timezone as an ISO string.

    Returns:
        str: Today's date in ISO format (YYYY-MM-DD) in Vancouver timezone.
    """
    return get_vancouver_now().date().isoformat()

def get_db_connection():
    """Establishes a connection to the SQLite database.

    Returns:
        sqlite3.Connection: A database connection object with row_factory set to sqlite3.Row.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def setup_database():
    """Sets up the database by creating tables if they don't exist."""
    conn = get_db_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            monthly_kudos INTEGER DEFAULT 0,
            lifetime_level INTEGER DEFAULT 1,
            daily_awards_given INTEGER DEFAULT 0,
            last_award_date TEXT,
            last_message_date TEXT,
            greeting_enabled INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kudos_log (
            message_id INTEGER,
            reactor_id INTEGER,
            creator_id INTEGER,
            PRIMARY KEY (message_id, reactor_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_history (
            month TEXT PRIMARY KEY,
            user_id INTEGER,
            monthly_kudos INTEGER,
            new_level INTEGER,
            timestamp TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Migration: Add new columns to existing tables if they don't exist
    try:
        conn.execute("ALTER TABLE users ADD COLUMN last_message_date TEXT")
        print("Added last_message_date column to users table.")
    except sqlite3.OperationalError:
        pass  # Column already exists

    try:
        conn.execute("ALTER TABLE users ADD COLUMN greeting_enabled INTEGER DEFAULT 1")
        print("Added greeting_enabled column to users table.")
    except sqlite3.OperationalError:
        pass  # Column already exists

    conn.commit()
    conn.close()
    print("Database setup complete.")

def get_or_create_user(user_id):
    """Retrieves a user from the database or creates a new one if they don't exist.

    Args:
        user_id (int): The Discord user's ID.

    Returns:
        sqlite3.Row: The user's data.
    """
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    if user is None:
        conn.execute('INSERT INTO users (user_id) VALUES (?)', (user_id,))
        conn.commit()
        user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    conn.close()
    return user

def award_kudos(creator_id, reactor_id):
    """Awards kudos to a message creator and the user who reacted.

    Args:
        creator_id (int): The ID of the user who created the message.
        reactor_id (int): The ID of the user who added the reaction.
    """
    print("Attempting to commit changes to the database...")
    conn = get_db_connection()
    today = get_vancouver_today()
    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos + 2 WHERE user_id = ?', 
        (creator_id,)
    )
    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos + 1, daily_awards_given = daily_awards_given + 1, last_award_date = ? WHERE user_id = ?',
        (today, reactor_id)
    )
    conn.commit()
    conn.close()

def reset_daily_limit_if_needed(user_id):
    """Resets a user's daily award limit if the current date is different from their last award date.

    Args:
        user_id (int): The Discord user's ID.
    """
    user = get_or_create_user(user_id)
    today = get_vancouver_today()
    if user['last_award_date'] != today:
        conn = get_db_connection()
        conn.execute('UPDATE users SET daily_awards_given = 0, last_award_date = ? WHERE user_id = ?', (today, user_id))
        conn.commit()
        conn.close()

def get_leaderboard_data():
    """Fetches all users with kudos, ordered by their monthly kudos in descending order.

    Returns:
        list[sqlite3.Row]: A list of user data.
    """
    conn = get_db_connection()
    users = conn.execute('SELECT * FROM users WHERE monthly_kudos >= 0 ORDER BY monthly_kudos DESC').fetchall()
    conn.close()
    return users

def apply_daily_maintenance(decay: int, bonus: int):
    """Applies daily kudos decay to all users and awards a bonus to the top user.

    Args:
        decay (int): The amount of kudos to remove from each user. If 0, no decay is applied.
        bonus (int): The amount of bonus kudos to give to the top user.

    Returns:
        int | None: The ID of the top user who received the bonus, or None if no users have kudos.
    """
    conn = get_db_connection()

    # Only apply decay if decay > 0
    if decay > 0:
        conn.execute('UPDATE users SET monthly_kudos = monthly_kudos - ? WHERE monthly_kudos > ?', (decay, decay-1))

    top_user = conn.execute('SELECT user_id FROM users ORDER BY monthly_kudos DESC LIMIT 1').fetchone()
    if top_user and bonus > 0:
        conn.execute('UPDATE users SET monthly_kudos = monthly_kudos + ? WHERE user_id = ?', (bonus, top_user['user_id']))

    conn.commit()
    conn.close()
    if top_user:
        return top_user['user_id']
    return None

def monthly_reset():
    """Resets monthly kudos for all users, promotes the winner, and clears the kudos log.

    Returns:
        sqlite3.Row | None: The data of the winning user, or None if no users had kudos.
    """
    conn = get_db_connection()
    winner = conn.execute('SELECT * FROM users ORDER BY monthly_kudos DESC LIMIT 1').fetchone()

    if winner:
        new_level = winner['lifetime_level'] + 1
        conn.execute('UPDATE users SET lifetime_level = ? WHERE user_id = ?', (new_level, winner['user_id']))

        # Save winner to monthly history
        now = get_vancouver_now()
        # Use previous month for the history entry (since we're resetting at start of new month)
        from dateutil.relativedelta import relativedelta
        last_month = now - relativedelta(months=1)
        month_key = last_month.strftime('%Y-%m')

        conn.execute(
            'INSERT OR REPLACE INTO monthly_history (month, user_id, monthly_kudos, new_level, timestamp) VALUES (?, ?, ?, ?, ?)',
            (month_key, winner['user_id'], winner['monthly_kudos'], new_level, now.isoformat())
        )

    conn.execute('UPDATE users SET monthly_kudos = 0')
    conn.execute('DELETE FROM kudos_log') # Enforces the "Immutable Past"
    conn.commit()
    conn.close()

    return winner

def remove_kudos(creator_id, reactor_id):
    """Removes kudos from a message creator and the user who reacted.

    This function is the inverse of award_kudos. Per the Fire-and-Forget principle,
    it does NOT refund the daily award credit.

    Args:
        creator_id (int): The ID of the user who created the message.
        reactor_id (int): The ID of the user who removed the reaction.
    """
    conn = get_db_connection()
    # Reverse the +2 kudos for the creator
    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos - 2 WHERE user_id = ? AND monthly_kudos >= 2', 
        (creator_id,)
    )
    # Reverse the +1 kudos for the reactor. The daily award is NOT refunded.
    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos - 1 WHERE user_id = ? AND monthly_kudos >= 1',
        (reactor_id,)
    )
    conn.commit()
    conn.close()

def log_kudos(message_id, reactor_id, creator_id):
    """Logs a kudos transaction in the database.

    Args:
        message_id (int): The ID of the message that was reacted to.
        reactor_id (int): The ID of the user who gave the kudos.
        creator_id (int): The ID of the user who received the kudos.
    """
    conn = get_db_connection()
    conn.execute(
        'INSERT OR IGNORE INTO kudos_log (message_id, reactor_id, creator_id) VALUES (?, ?, ?)',
        (message_id, reactor_id, creator_id)
    )
    conn.commit()
    conn.close()

def check_kudos_exists(message_id, reactor_id):
    """Checks if a specific kudos transaction exists in the log.

    Args:
        message_id (int): The ID of the message.
        reactor_id (int): The ID of the user who reacted.

    Returns:
        bool: True if the kudos exists, False otherwise.
    """
    conn = get_db_connection()
    log = conn.execute(
        'SELECT 1 FROM kudos_log WHERE message_id = ? AND reactor_id = ?',
        (message_id, reactor_id)
    ).fetchone()
    conn.close()
    return log is not None

def delete_kudos_log(message_id, reactor_id):
    """Deletes a kudos transaction from the log.

    Args:
        message_id (int): The ID of the message.
        reactor_id (int): The ID of the user who reacted.
    """
    conn = get_db_connection()
    conn.execute(
        'DELETE FROM kudos_log WHERE message_id = ? AND reactor_id = ?',
        (message_id, reactor_id)
    )
    conn.commit()
    conn.close()

def update_last_message_date(user_id, message_date):
    """Updates the last message date for a user.

    Args:
        user_id (int): The Discord user's ID.
        message_date (str): The date in ISO format (YYYY-MM-DD).
    """
    conn = get_db_connection()
    conn.execute(
        'UPDATE users SET last_message_date = ? WHERE user_id = ?',
        (message_date, user_id)
    )
    conn.commit()
    conn.close()

def toggle_user_greeting(user_id):
    """Toggles the greeting_enabled setting for a user.

    Args:
        user_id (int): The Discord user's ID.

    Returns:
        bool: The new state of greeting_enabled (True if enabled, False if disabled).
    """
    user = get_or_create_user(user_id)
    current_state = user['greeting_enabled'] if user['greeting_enabled'] is not None else 1
    new_state = 0 if current_state == 1 else 1

    conn = get_db_connection()
    conn.execute(
        'UPDATE users SET greeting_enabled = ? WHERE user_id = ?',
        (new_state, user_id)
    )
    conn.commit()
    conn.close()

    return new_state == 1

def award_daily_greeting_kudos(creator_id, bot_id):
    """Awards kudos for daily first message (bot gives kudos with infinite supply).

    Awards +1 kudos to the message creator, +0 to the bot.
    This is a special version of award_kudos for the daily greeting feature.

    Args:
        creator_id (int): The ID of the user who created the message.
        bot_id (int): The ID of the bot (not used for kudos, just for logging).
    """
    conn = get_db_connection()
    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos + 1 WHERE user_id = ?',
        (creator_id,)
    )
    conn.commit()
    conn.close()

def reset_daily_limits(user_id=None):
    """Resets daily award limits for a specific user or all users.

    This sets daily_awards_given to 0 and last_award_date to NULL, allowing
    users to give kudos again immediately.

    Args:
        user_id (int, optional): The Discord user's ID. If None, resets all users.

    Returns:
        int: The number of users affected.
    """
    conn = get_db_connection()

    if user_id is not None:
        conn.execute(
            'UPDATE users SET daily_awards_given = 0, last_award_date = NULL WHERE user_id = ?',
            (user_id,)
        )
    else:
        conn.execute('UPDATE users SET daily_awards_given = 0, last_award_date = NULL')

    affected_rows = conn.total_changes
    conn.commit()
    conn.close()

    return affected_rows

def get_monthly_history():
    """Retrieves all monthly history records, ordered by month descending (newest first).

    Returns:
        list[sqlite3.Row]: A list of monthly history records.
    """
    conn = get_db_connection()
    history = conn.execute('SELECT * FROM monthly_history ORDER BY month DESC').fetchall()
    conn.close()
    return history

def get_system_state(key, default=None):
    """Retrieves a system state value from the database.

    Args:
        key (str): The state key to retrieve.
        default: The default value if key doesn't exist.

    Returns:
        str | None: The state value, or default if not found.
    """
    conn = get_db_connection()
    result = conn.execute('SELECT value FROM system_state WHERE key = ?', (key,)).fetchone()
    conn.close()
    return result['value'] if result else default

def set_system_state(key, value):
    """Sets a system state value in the database.

    Args:
        key (str): The state key to set.
        value (str): The value to store.
    """
    conn = get_db_connection()
    conn.execute(
        'INSERT OR REPLACE INTO system_state (key, value) VALUES (?, ?)',
        (key, value)
    )
    conn.commit()
    conn.close()