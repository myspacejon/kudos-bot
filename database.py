import sqlite3
from datetime import date
import os

DB_FILE = "kudos_bot.db"

def get_db_connection():
    """Establishes a connection to the SQLite database.

    Returns:
        sqlite3.Connection: A database connection object with row_factory set to sqlite3.Row.
    """
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def setup_database():
    """Sets up the database by creating the 'users' table if it doesn't exist."""
    if os.path.exists(DB_FILE):
        return
    
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            monthly_kudos INTEGER DEFAULT 0,
            lifetime_level INTEGER DEFAULT 1,
            daily_awards_given INTEGER DEFAULT 0,
            last_award_date TEXT
        )
    ''')
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
    today = date.today().isoformat()
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
    today = date.today().isoformat()
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
    users = conn.execute('SELECT * FROM users WHERE monthly_kudos > 0 ORDER BY monthly_kudos DESC').fetchall()
    conn.close()
    return users

def apply_daily_maintenance(decay: int, bonus: int):
    """Applies daily kudos decay to all users and awards a bonus to the top user.

    Args:
        decay (int): The amount of kudos to remove from each user.
        bonus (int): The amount of bonus kudos to give to the top user.

    Returns:
        int | None: The ID of the top user who received the bonus, or None if no users have kudos.
    """
    conn = get_db_connection()
    conn.execute('UPDATE users SET monthly_kudos = monthly_kudos - ? WHERE monthly_kudos > ?', (decay, decay-1))
    
    top_user = conn.execute('SELECT user_id FROM users ORDER BY monthly_kudos DESC LIMIT 1').fetchone()
    if top_user:
        conn.execute('UPDATE users SET monthly_kudos = monthly_kudos + ? WHERE user_id = ?', (bonus, top_user['user_id']))
        
    conn.commit()
    conn.close()
    if top_user:
        return top_user['user_id']
    return None

def monthly_reset():
    """Resets monthly kudos for all users and promotes the winner to the next lifetime level.

    Returns:
        sqlite3.Row | None: The data of the winning user, or None if no users had kudos.
    """
    conn = get_db_connection()
    winner = conn.execute('SELECT * FROM users ORDER BY monthly_kudos DESC LIMIT 1').fetchone()

    if winner:
        new_level = winner['lifetime_level'] + 1
        conn.execute('UPDATE users SET lifetime_level = ? WHERE user_id = ?', (new_level, winner['user_id']))
    
    conn.execute('UPDATE users SET monthly_kudos = 0')
    conn.commit()
    conn.close()
    
    return winner

def remove_kudos(creator_id, reactor_id):
    """Removes kudos from a message creator and the user who reacted.

    Args:
        creator_id (int): The ID of the user who created the message.
        reactor_id (int): The ID of the user who removed the reaction.
    """
    conn = get_db_connection()

    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos - 2 WHERE user_id = ?', 
        (creator_id,)
    )
    conn.execute(
        'UPDATE users SET monthly_kudos = monthly_kudos - 1 WHERE user_id = ?',
        (reactor_id,)
    )
    conn.commit()
    conn.close()
