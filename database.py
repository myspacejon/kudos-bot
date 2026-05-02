import sqlite3
from datetime import datetime
import os
import pytz

DB_FILE = "/data/kudos_bot.db"

# Timezone configuration for America/Vancouver (PST/PDT)
VANCOUVER_TZ = pytz.timezone('America/Vancouver')


def calculate_level(lifetime_exp, thresholds):
    """Calculates the level for a given lifetime EXP total using provided thresholds."""
    level = 1
    for i, threshold in enumerate(thresholds):
        if lifetime_exp >= threshold:
            level = i + 1
    return level


def check_and_apply_level_up(user_id, thresholds):
    """Recomputes level from stored lifetime_exp and updates lifetime_level if different."""
    conn = get_db_connection()
    user = conn.execute(
        'SELECT lifetime_exp, lifetime_level FROM users WHERE user_id = ?', (user_id,)
    ).fetchone()
    if user is None:
        conn.close()
        return None
    new_level = calculate_level(user['lifetime_exp'], thresholds)
    if new_level != user['lifetime_level']:
        conn.execute(
            'UPDATE users SET lifetime_level = ? WHERE user_id = ?',
            (new_level, user_id)
        )
        conn.commit()
        conn.close()
        return new_level
    conn.close()
    return None


def get_vancouver_now():
    """Returns the current datetime in America/Vancouver timezone."""
    return datetime.now(VANCOUVER_TZ)


def get_vancouver_today():
    """Returns today's date in America/Vancouver timezone as an ISO string."""
    return get_vancouver_now().date().isoformat()


def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def setup_database():
    """Sets up the database, creating tables and applying schema migrations."""
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS watched_channels (
            channel_id INTEGER PRIMARY KEY,
            context_hours INTEGER DEFAULT 12
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS thread_summaries (
            thread_id INTEGER PRIMARY KEY,
            thread_name TEXT,
            summary TEXT,
            updated_at TEXT
        )
    """)

    # Apply schema migrations for columns added over time
    migrations = [
        "ALTER TABLE users ADD COLUMN last_message_date TEXT",
        "ALTER TABLE users ADD COLUMN greeting_enabled INTEGER DEFAULT 1",
        "ALTER TABLE users ADD COLUMN monthly_kudos_received INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN monthly_kudos_given INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN lifetime_kudos_received INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN lifetime_kudos_given INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN lifetime_exp INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN current_streak INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN best_streak INTEGER DEFAULT 0",
        "ALTER TABLE users ADD COLUMN last_project_post_date TEXT",
        "ALTER TABLE users ADD COLUMN color_override INTEGER DEFAULT NULL",
    ]
    for migration in migrations:
        try:
            conn.execute(migration)
            print(f"Migration applied: {migration}")
        except sqlite3.OperationalError:
            pass  # Column already exists

    conn.commit()
    conn.close()
    print("Database setup complete.")


def get_or_create_user(user_id):
    """Retrieves a user from the database or creates a new one if they don't exist."""
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    if user is None:
        conn.execute('INSERT INTO users (user_id) VALUES (?)', (user_id,))
        conn.commit()
        user = conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)).fetchone()
    conn.close()
    return user


def award_kudos(creator_id, reactor_id):
    """Awards kudos to a message creator and the user who reacted."""
    conn = get_db_connection()
    today = get_vancouver_today()
    conn.execute(
        '''UPDATE users
           SET monthly_kudos_received = monthly_kudos_received + 1,
               lifetime_kudos_received = lifetime_kudos_received + 1,
               lifetime_exp = lifetime_exp + 2
           WHERE user_id = ?''',
        (creator_id,)
    )
    conn.execute(
        '''UPDATE users
           SET monthly_kudos_given = monthly_kudos_given + 1,
               lifetime_kudos_given = lifetime_kudos_given + 1,
               lifetime_exp = lifetime_exp + 1,
               daily_awards_given = daily_awards_given + 1,
               last_award_date = ?
           WHERE user_id = ?''',
        (today, reactor_id)
    )
    conn.commit()
    conn.close()


def award_daily_greeting_kudos(creator_id, bot_id):
    """Awards daily first-message kudos from the bot (infinite supply)."""
    conn = get_db_connection()
    conn.execute(
        '''UPDATE users
           SET monthly_kudos_received = monthly_kudos_received + 1,
               lifetime_kudos_received = lifetime_kudos_received + 1,
               lifetime_exp = lifetime_exp + 2
           WHERE user_id = ?''',
        (creator_id,)
    )
    conn.commit()
    conn.close()


def remove_kudos(creator_id, reactor_id):
    """Removes kudos when a reaction is retracted. EXP is never decremented."""
    conn = get_db_connection()
    conn.execute(
        '''UPDATE users
           SET monthly_kudos_received = MAX(0, monthly_kudos_received - 1),
               lifetime_kudos_received = MAX(0, lifetime_kudos_received - 1)
           WHERE user_id = ?''',
        (creator_id,)
    )
    conn.execute(
        '''UPDATE users
           SET monthly_kudos_given = MAX(0, monthly_kudos_given - 1),
               lifetime_kudos_given = MAX(0, lifetime_kudos_given - 1)
           WHERE user_id = ?''',
        (reactor_id,)
    )
    conn.commit()
    conn.close()


def reset_daily_limit_if_needed(user_id):
    """Resets a user's daily award limit if a new Vancouver-day has started."""
    user = get_or_create_user(user_id)
    today = get_vancouver_today()
    if user['last_award_date'] != today:
        conn = get_db_connection()
        conn.execute(
            'UPDATE users SET daily_awards_given = 0, last_award_date = ? WHERE user_id = ?',
            (today, user_id)
        )
        conn.commit()
        conn.close()


def get_leaderboard_data():
    """Returns users with monthly_kudos_given > 0, sorted by monthly_kudos_given DESC."""
    conn = get_db_connection()
    users = conn.execute(
        'SELECT * FROM users WHERE monthly_kudos_given > 0 ORDER BY monthly_kudos_given DESC'
    ).fetchall()
    conn.close()
    return users


def apply_daily_maintenance(decay):
    """Applies daily kudos decay to monthly_kudos_given totals."""
    if decay <= 0:
        return
    conn = get_db_connection()
    conn.execute(
        'UPDATE users SET monthly_kudos_given = monthly_kudos_given - ? WHERE monthly_kudos_given > ?',
        (decay, decay - 1)
    )
    conn.commit()
    conn.close()


def monthly_reset():
    """Resets monthly kudos counters and records the top giver as winner."""
    conn = get_db_connection()
    winner = conn.execute(
        'SELECT * FROM users WHERE monthly_kudos_given > 0 ORDER BY monthly_kudos_given DESC LIMIT 1'
    ).fetchone()

    if winner:
        from dateutil.relativedelta import relativedelta
        now = get_vancouver_now()
        last_month = now - relativedelta(months=1)
        month_key = last_month.strftime('%Y-%m')

        conn.execute(
            '''INSERT OR REPLACE INTO monthly_history
               (month, user_id, monthly_kudos, new_level, timestamp)
               VALUES (?, ?, ?, ?, ?)''',
            (month_key, winner['user_id'], winner['monthly_kudos_given'],
             winner['lifetime_level'], now.isoformat())
        )

    conn.execute('UPDATE users SET monthly_kudos_received = 0, monthly_kudos_given = 0')
    conn.execute('DELETE FROM kudos_log')
    conn.commit()
    conn.close()

    return winner


def log_kudos(message_id, reactor_id, creator_id):
    """Logs a kudos transaction in the database."""
    conn = get_db_connection()
    conn.execute(
        'INSERT OR IGNORE INTO kudos_log (message_id, reactor_id, creator_id) VALUES (?, ?, ?)',
        (message_id, reactor_id, creator_id)
    )
    conn.commit()
    conn.close()


def check_kudos_exists(message_id, reactor_id):
    """Checks if a specific kudos transaction exists in the log."""
    conn = get_db_connection()
    log = conn.execute(
        'SELECT 1 FROM kudos_log WHERE message_id = ? AND reactor_id = ?',
        (message_id, reactor_id)
    ).fetchone()
    conn.close()
    return log is not None


def delete_kudos_log(message_id, reactor_id):
    """Deletes a kudos transaction from the log."""
    conn = get_db_connection()
    conn.execute(
        'DELETE FROM kudos_log WHERE message_id = ? AND reactor_id = ?',
        (message_id, reactor_id)
    )
    conn.commit()
    conn.close()


def update_last_message_date(user_id, message_date):
    """Updates the last message date for a user."""
    conn = get_db_connection()
    conn.execute(
        'UPDATE users SET last_message_date = ? WHERE user_id = ?',
        (message_date, user_id)
    )
    conn.commit()
    conn.close()


def toggle_user_greeting(user_id):
    """Toggles the greeting_enabled setting for a user. Returns the new state."""
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


def reset_daily_limits(user_id=None):
    """Resets daily award limits for a specific user or all users."""
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
    """Retrieves all monthly history records, ordered by month descending."""
    conn = get_db_connection()
    history = conn.execute('SELECT * FROM monthly_history ORDER BY month DESC').fetchall()
    conn.close()
    return history


def get_watched_channels():
    """Returns all watched channels as a dict of {channel_id: context_hours}."""
    conn = get_db_connection()
    rows = conn.execute('SELECT channel_id, context_hours FROM watched_channels').fetchall()
    conn.close()
    return {row['channel_id']: row['context_hours'] for row in rows}


def toggle_watched_channel(channel_id, default_hours=12):
    """Toggles a channel in the watched list. Returns True if now watching, False if removed."""
    conn = get_db_connection()
    existing = conn.execute('SELECT 1 FROM watched_channels WHERE channel_id = ?', (channel_id,)).fetchone()
    if existing:
        conn.execute('DELETE FROM watched_channels WHERE channel_id = ?', (channel_id,))
        conn.commit()
        conn.close()
        return False
    else:
        conn.execute('INSERT INTO watched_channels (channel_id, context_hours) VALUES (?, ?)', (channel_id, default_hours))
        conn.commit()
        conn.close()
        return True


def set_channel_window(channel_id, hours):
    """Sets the context window for a watched channel. Adds it if not already watched."""
    conn = get_db_connection()
    conn.execute(
        'INSERT OR REPLACE INTO watched_channels (channel_id, context_hours) VALUES (?, ?)',
        (channel_id, hours)
    )
    conn.commit()
    conn.close()


def get_thread_summary(thread_id):
    """Retrieves a stored thread summary."""
    conn = get_db_connection()
    result = conn.execute(
        'SELECT summary, updated_at FROM thread_summaries WHERE thread_id = ?',
        (thread_id,)
    ).fetchone()
    conn.close()
    return result


def set_thread_summary(thread_id, thread_name, summary):
    """Stores or updates a thread summary."""
    conn = get_db_connection()
    conn.execute(
        'INSERT OR REPLACE INTO thread_summaries (thread_id, thread_name, summary, updated_at) VALUES (?, ?, ?, ?)',
        (thread_id, thread_name, summary, datetime.utcnow().isoformat())
    )
    conn.commit()
    conn.close()


def set_color_override(user_id, level):
    """Stores the user's chosen colour override level. Pass None to clear."""
    conn = get_db_connection()
    conn.execute(
        'UPDATE users SET color_override = ? WHERE user_id = ?',
        (level, user_id)
    )
    conn.commit()
    conn.close()


def update_project_streak(user_id, milestones):
    """Updates the project thread posting streak for a user.

    Grace period: missing 1 day is forgiven (streak survives) but does not
    advance the streak count. Missing 2+ days resets the streak to 1.

    Returns:
        tuple: (current_streak, best_streak, milestone_hit, is_new_day)
    """
    from datetime import date as date_type
    conn = get_db_connection()
    user = conn.execute(
        'SELECT current_streak, best_streak, last_project_post_date FROM users WHERE user_id = ?',
        (user_id,)
    ).fetchone()

    if user is None:
        conn.close()
        return (0, 0, None, False)

    today = get_vancouver_today()
    last_date = user['last_project_post_date']
    current_streak = user['current_streak'] or 0
    best_streak = user['best_streak'] or 0
    milestone_hit = None

    if last_date == today:
        conn.close()
        return (current_streak, best_streak, None, False)

    is_new_day = True

    if last_date is None:
        current_streak = 1
    else:
        last = date_type.fromisoformat(last_date)
        today_date = date_type.fromisoformat(today)
        delta = (today_date - last).days

        if delta == 1:
            current_streak += 1
        elif delta == 2:
            pass  # Grace day - streak survives but does not advance
        else:
            current_streak = 1

    if current_streak > best_streak:
        best_streak = current_streak

    if current_streak in milestones:
        milestone_hit = current_streak

    conn.execute(
        '''UPDATE users
           SET current_streak = ?,
               best_streak = ?,
               last_project_post_date = ?
           WHERE user_id = ?''',
        (current_streak, best_streak, today, user_id)
    )
    conn.commit()
    conn.close()

    return (current_streak, best_streak, milestone_hit, is_new_day)


def get_streak_leaderboard():
    """Returns users with an active streak > 0, sorted by current_streak DESC."""
    conn = get_db_connection()
    users = conn.execute(
        'SELECT * FROM users WHERE current_streak > 0 ORDER BY current_streak DESC'
    ).fetchall()
    conn.close()
    return users


def get_system_state(key, default=None):
    """Retrieves a system state value from the database."""
    conn = get_db_connection()
    result = conn.execute('SELECT value FROM system_state WHERE key = ?', (key,)).fetchone()
    conn.close()
    return result['value'] if result else default


def set_system_state(key, value):
    """Sets a system state value in the database."""
    conn = get_db_connection()
    conn.execute(
        'INSERT OR REPLACE INTO system_state (key, value) VALUES (?, ?)',
        (key, value)
    )
    conn.commit()
    conn.close()
