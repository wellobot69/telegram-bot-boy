import logging
import os
from telegram import Update, MessageEntity, InputMediaPhoto, InputMediaVideo, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from datetime import datetime, timedelta
import pytz
import json
import os
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiofiles

# Bot Configuration
BOT_TOKEN = os.getenv(
    "BOT_TOKEN") or "7590580923:AAEiaCkGXLhLdvyMvFBJc_wePJLR_9ZtkV4"
ADMIN_IDS = [
    int(id.strip()) for id in os.getenv("ADMIN_IDS", "7489624146").split(",")
    if id.strip()
]
SCHEDULED_MESSAGES_FILE = "scheduled_messages.json"
SUBSCRIBERS_FILE = "subscribers.json"
TOP_POSTS_FILE = "top_posts.json"

# Create lock for message sending
send_lock = asyncio.Lock()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(),
              logging.FileHandler('bot.log')])

# Initialize scheduler with timezone
cairo_tz = pytz.timezone('Africa/Cairo')
scheduler = AsyncIOScheduler(timezone=cairo_tz)
selected_times = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23]

# Constants
COLLECTED_POSTS_FILE = "collected_posts.json"
FAILED_SUBSCRIBERS_FILE = "failed_subscribers.json"
SENT_POSTS_TRACKING_FILE = "sent_posts_tracking.json"
DAILY_ANALYTICS_FILE = "daily_analytics.json"

# Store channel link and archive button
CHANNEL_LINK_FILE = "channel_link.txt"
current_channel_link = "https://t.me/+1t-w4sxo8t00ZTk0"
archive_button_url = "https://t.me/+1t-w4sxo8t00ZTk0"
archive_button_name = "Open Channel"

# Conversation states
waiting_for_channel_link = set(
)  # Store user IDs waiting to provide channel link


async def save_channel_link(link):
    """Save channel link to file"""
    global current_channel_link, archive_button_url, archive_button_name
    try:
        async with aiofiles.open(CHANNEL_LINK_FILE, 'w') as f:
            await f.write(link)
        current_channel_link = link
        archive_button_url = link
        archive_button_name = "Open Channel"
        logging.info(f"Saved channel link: {link}")
    except Exception as e:
        logging.error(f"Failed to save channel link: {e}")


async def load_channel_link():
    """Load channel link from file"""
    global current_channel_link, archive_button_url, archive_button_name
    try:
        if os.path.exists(CHANNEL_LINK_FILE):
            async with aiofiles.open(CHANNEL_LINK_FILE, 'r') as f:
                link = await f.read()
                if link.strip():
                    current_channel_link = link.strip()
                    archive_button_url = link.strip()
                    archive_button_name = "Open Channel"
                    logging.info(
                        f"Loaded channel link: {current_channel_link}")
    except Exception as e:
        logging.error(f"Failed to load channel link: {e}")


# Store scheduled messages and target channels
def save_scheduled_messages():
    """Save scheduled messages to JSON file synchronously"""
    try:
        data = {
            "messages": [msg.to_dict() for msg in scheduled_messages],
            "target_channels": list(target_channels),
            "auto_scheduling_active": auto_scheduling_active
        }
        with open(SCHEDULED_MESSAGES_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved {len(scheduled_messages)} scheduled messages")
    except Exception as e:
        logging.error(f"Failed to save scheduled messages: {e}")


def load_scheduled_messages():
    """Load scheduled messages from JSON file synchronously"""
    global scheduled_messages, target_channels, auto_scheduling_active
    try:
        if not os.path.exists(SCHEDULED_MESSAGES_FILE):
            return
        with open(SCHEDULED_MESSAGES_FILE, 'r') as f:
            content = f.read()
            if content.strip():
                data = json.loads(content)
                scheduled_messages = [
                    ScheduledMessage.from_dict(msg_data)
                    for msg_data in data.get("messages", [])
                ]
                target_channels.update(data.get("target_channels", []))
                auto_scheduling_active = data.get("auto_scheduling_active",
                                                  False)
                logging.info(
                    f"Loaded {len(scheduled_messages)} scheduled messages")
    except Exception as e:
        logging.error(f"Failed to load scheduled messages: {e}")


class ScheduledMessage:

    def __init__(self, media_group_id=None):
        self.text = None
        self.media = []
        self.media_group_id = media_group_id
        self.entities = None
        self.created_at = datetime.now(pytz.timezone('Europe/London'))
        self.send_time = None
        self.local_files = []  # Store paths to locally saved media files
        self.buttons = None  # Store button data if any

    def add_media(self,
                  file_id,
                  type,
                  caption=None,
                  caption_entities=None,
                  local_file_path=None):
        media_data = {
            "file_id": file_id,
            "type": type,
            "caption": caption,
            "caption_entities": caption_entities
        }
        if local_file_path:
            media_data["local_file_path"] = local_file_path
            self.local_files.append(local_file_path)
        self.media.append(media_data)

    def set_text(self, text, entities=None):
        self.text = text
        self.entities = entities

    def set_buttons(self, buttons):
        self.buttons = buttons

    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        # Convert MessageEntity objects to dictionaries
        entities_dict = None
        if self.entities:
            entities_dict = []
            for entity in self.entities:
                entities_dict.append({
                    "type":
                    entity.type,
                    "offset":
                    entity.offset,
                    "length":
                    entity.length,
                    "url":
                    entity.url if hasattr(entity, 'url') else None,
                    "user":
                    entity.user.to_dict()
                    if hasattr(entity, 'user') and entity.user else None,
                    "language":
                    entity.language if hasattr(entity, 'language') else None
                })

        return {
            "text": self.text,
            "media": self.media,
            "media_group_id": self.media_group_id,
            "entities": entities_dict,
            "created_at": self.created_at.isoformat(),
            "send_time":
            self.send_time.isoformat() if self.send_time else None,
            "local_files": self.local_files,
            "buttons": self.buttons
        }

    @classmethod
    def from_dict(cls, data):
        """Create instance from dictionary"""
        msg = cls(data.get("media_group_id"))
        msg.text = data.get("text")
        msg.media = data.get("media", [])

        # Convert dictionaries back to MessageEntity objects
        if data.get("entities"):
            msg.entities = []
            for entity_data in data["entities"]:
                entity = MessageEntity(
                    type=entity_data["type"],
                    offset=entity_data["offset"],
                    length=entity_data["length"],
                    url=entity_data.get("url"),
                    user=None,  # User objects are complex, skip for now
                    language=entity_data.get("language"))
                msg.entities.append(entity)
        else:
            msg.entities = None

        msg.created_at = datetime.fromisoformat(data["created_at"])
        msg.send_time = datetime.fromisoformat(
            data["send_time"]) if data.get("send_time") else None
        msg.local_files = data.get("local_files", [])
        msg.buttons = data.get("buttons")
        return msg

    def cleanup_local_files(self):
        """Delete local media files"""
        for file_path in self.local_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logging.info(f"Deleted local file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete local file {file_path}: {e}")

    def __str__(self):
        time_str = self.created_at.strftime("%Y-%m-%d %H:%M")
        if self.text:
            preview = (self.text[:30] +
                       '...') if len(self.text) > 30 else self.text
            return f"[{time_str}] Text: {preview}"
        elif self.media:
            media_types = [m['type'] for m in self.media]
            caption = self.media[0].get('caption', '')
            preview = (caption[:30] +
                       '...') if caption and len(caption) > 30 else caption
            return f"[{time_str}] Media ({', '.join(media_types)}): {preview}"
        return f"[{time_str}] Empty message"

    def is_duplicate_of(self, other):
        """Compare two scheduled messages for duplicate detection"""
        # If one has text and other doesn't, they're different
        if bool(self.text) != bool(other.text):
            return False

        # Compare text messages
        if self.text and other.text:
            return self.text == other.text

        # If media counts don't match, they're different
        if len(self.media) != len(other.media):
            return False

        # Compare media file_ids
        self_ids = sorted(m['file_id'] for m in self.media)
        other_ids = sorted(m['file_id'] for m in other.media)
        return self_ids == other_ids


# Subscriber system variables
subscribers = set()  # Using set for fast lookup
top_posts = []


# Subscriber persistence functions
async def auto_add_subscriber(user_id):
    """Auto-add subscriber on any interaction (excludes admins)"""
    global subscribers
    if user_id not in subscribers and not is_admin(user_id):
        old_count = len(subscribers)
        subscribers.add(user_id)
        new_count = len(subscribers)
        logging.info(
            f"Auto-added subscriber: {user_id} (count: {old_count} -> {new_count})"
        )
        save_subscribers()

        # Verify the subscriber was actually added and saved
        if os.path.exists(SUBSCRIBERS_FILE):
            try:
                async with aiofiles.open(SUBSCRIBERS_FILE, 'r') as f:
                    content = await f.read()
                    if content.strip():
                        data = json.loads(content)
                        saved_users = set(data.get("user_ids", []))
                        if user_id in saved_users:
                            logging.info(
                                f"Verified subscriber {user_id} was saved successfully - now has bot access"
                            )
                        else:
                            logging.error(
                                f"Subscriber {user_id} was NOT saved to file!")
            except Exception as e:
                logging.error(f"Failed to verify subscriber save: {e}")
        else:
            logging.error(
                f"Subscribers file {SUBSCRIBERS_FILE} does not exist after save attempt!"
            )
    elif user_id in subscribers:
        logging.info(f"User {user_id} is already a verified subscriber")


def save_subscribers():
    """Save subscribers to JSON file synchronously"""
    try:
        current_dir = os.getcwd()
        logging.info(f"Current working directory: {current_dir}")
        logging.info(
            f"Attempting to save {len(subscribers)} subscribers to {SUBSCRIBERS_FILE}"
        )

        data = {"user_ids": list(subscribers)}

        with open(SUBSCRIBERS_FILE, 'w') as f:
            json.dump(data, f, indent=2)

        logging.info(
            f"Successfully saved {len(subscribers)} subscribers to {SUBSCRIBERS_FILE}"
        )

        # Verify the file was actually written
        if os.path.exists(SUBSCRIBERS_FILE):
            file_size = os.path.getsize(SUBSCRIBERS_FILE)
            logging.info(
                f"File {SUBSCRIBERS_FILE} size after save: {file_size} bytes")
        else:
            logging.error(f"File {SUBSCRIBERS_FILE} was not created!")

    except Exception as e:
        logging.error(f"Failed to save subscribers: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")


async def load_subscribers():
    """Load subscribers from JSON file"""
    global subscribers
    try:
        current_dir = os.getcwd()
        logging.info(
            f"Loading subscribers from {SUBSCRIBERS_FILE} in directory {current_dir}"
        )

        if not os.path.exists(SUBSCRIBERS_FILE):
            logging.info(
                f"Subscribers file {SUBSCRIBERS_FILE} does not exist, starting with empty set"
            )
            return

        file_size = os.path.getsize(SUBSCRIBERS_FILE)
        logging.info(f"Found subscribers file, size: {file_size} bytes")

        async with aiofiles.open(SUBSCRIBERS_FILE, 'r') as f:
            content = await f.read()
            logging.info(
                f"Read {len(content)} characters from subscribers file")

            if content.strip():
                data = json.loads(content)
                subscribers = set(data.get("user_ids", []))
                logging.info(
                    f"Successfully loaded {len(subscribers)} subscribers: {list(subscribers)}"
                )
            else:
                logging.info("Subscribers file is empty")

    except Exception as e:
        logging.error(f"Failed to load subscribers: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")


def save_top_posts():
    """Save top posts to JSON file synchronously"""
    try:
        data = {"top_posts": [post.to_dict() for post in top_posts]}
        with open(TOP_POSTS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Saved {len(top_posts)} top posts")
    except Exception as e:
        logging.error(f"Failed to save top posts: {e}")


def load_top_posts():
    """Load top posts from JSON file synchronously"""
    global top_posts
    try:
        if not os.path.exists(TOP_POSTS_FILE):
            return
        with open(TOP_POSTS_FILE, 'r') as f:
            content = f.read()
            if content.strip():
                data = json.loads(content)
                top_posts = [
                    ScheduledMessage.from_dict(post_data)
                    for post_data in data.get("top_posts", [])
                ]
                logging.info(f"Loaded {len(top_posts)} top posts")
    except Exception as e:
        logging.error(f"Failed to load top posts: {e}")


scheduled_messages = []
target_channels = {-1002554306424, -1002613672782}  # Updated channel ID
auto_scheduling_active = False
media_group_buffer = {}

# Channel membership checking
REQUIRED_CHANNEL_ID = -1002613672782  # Updated channel ID


async def check_channel_membership(context: ContextTypes.DEFAULT_TYPE,
                                   user_id):
    """Check if user is a member of the required channel"""
    try:
        # First check if user is already a verified subscriber
        if user_id in subscribers:
            logging.info(f"User {user_id} is already a verified subscriber")
            return True

        member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID,
                                                   user_id=user_id)
        is_member = member.status in ['member', 'administrator', 'creator']

        if is_member:
            logging.info(
                f"User {user_id} verified as channel member with status: {member.status}"
            )
        else:
            logging.info(
                f"User {user_id} has status: {member.status} (not a member)")

        return is_member

    except Exception as e:
        # Handle different error cases
        error_msg = str(e).lower()

        if "user not found" in error_msg or "chat not found" in error_msg:
            logging.warning(
                f"Channel or user not found when checking membership for {user_id}: {e}"
            )
            # For public channels, if we can't verify membership, allow access for existing subscribers
            if user_id in subscribers:
                logging.info(
                    f"Allowing access for existing subscriber {user_id} despite verification error"
                )
                return True
            return False

        elif "bot was blocked" in error_msg or "forbidden" in error_msg:
            logging.warning(
                f"Bot doesn't have permission to check membership for user {user_id}: {e}"
            )
            # For existing subscribers, grant access even if we can't verify
            if user_id in subscribers:
                logging.info(
                    f"Allowing access for existing subscriber {user_id} despite permission error"
                )
                return True
            return False

        else:
            logging.error(
                f"Unexpected error checking channel membership for user {user_id}: {e}"
            )
            # For existing subscribers, be permissive
            if user_id in subscribers:
                logging.info(
                    f"Allowing access for existing subscriber {user_id} despite unexpected error"
                )
                return True
            return False


# Subscriber detection functions
def is_admin(user_id):
    """Check if user is admin"""
    return user_id in ADMIN_IDS


def is_subscriber(user_id):
    """Check if user is a subscriber (not admin)"""
    return user_id not in ADMIN_IDS


# Menu functions
def get_main_menu():
    """Get main menu keyboard"""
    global archive_button_name, archive_button_url
    keyboard = [
        [InlineKeyboardButton("🔥 TOP", callback_data="show_top_posts")],
        [InlineKeyboardButton("📚 Menu", callback_data="show_menu")],
        [
            InlineKeyboardButton("⚡️✅ How to pass the links",
                                 url="https://t.me/howto_passlinks")
        ], [InlineKeyboardButton(archive_button_name, url=archive_button_url)],
        [InlineKeyboardButton("Latest", callback_data="show_latest_posts")]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_navigation_buttons():
    """Get navigation buttons for subscriber messages"""
    keyboard = [[InlineKeyboardButton("📚 Menu", callback_data="show_menu")]]
    return InlineKeyboardMarkup(keyboard)


# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if is_admin(user_id):
        await update.message.reply_text("👋 Welcome to the Scheduler Bot!\n\n"
                                        "Use /help to see available commands.")
        return

    # Auto-add subscriber on any interaction (for non-admins)
    await auto_add_subscriber(user_id)

    # Check channel membership with improved logic
    is_member = await check_channel_membership(context, user_id)

    if not is_member:
        keyboard = [[
            InlineKeyboardButton("🔗 Join Channel First",
                                 url=current_channel_link)
        ]]
        await update.message.reply_text(
            "⚠️ You must join our channel first to use this bot!\n\n"
            "👆 Click the button above to join, then send /start again.",
            reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # Show main menu
    menu_text = (f"🔞 Welcome to Archive Bot! 🔞\n\n"
                 f"✅ You are now subscribed to content updates!\n\n"
                 "Choose an option below:")
    await update.message.reply_text(menu_text, reply_markup=get_main_menu())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Help command is now admin-only
    if not is_admin(user_id):
        return  # Silently ignore non-admin requests

    help_text = (
        "📋 Available Commands:\n\n"
        "/start - Start the bot\n"
        "/help - Show this help message\n"
        "/admin - Verify admin status\n"
        "/id - Get current chat ID\n\n"
        "🤖 Automatic Operation:\n"
        "/a - Start automatic message scheduling at odd hours\n"
        "/x - Stop automatic message scheduling\n\n"
        "📅 Scheduling Commands:\n"
        "/s - Show list of scheduled messages\n"
        "/r [number] - Remove a scheduled message\n"
        "/sadd - Add a replied message to scheduled messages\n"
        "/show [number] - Show preview of specific scheduled message\n\n"
        "📤 Sending Commands:\n"
        "/p - Send a message immediately (reply to a message)\n"
        "/send - Send first scheduled message immediately\n"
        "/delete_latest - Delete the last sent item from everywhere\n\n"
        "📢 Channel Management:\n"
        "/t - View all target channels\n"
        "/t add [chat_id] - Add a target channel\n"
        "/t remove [chat_id] - Remove a target channel\n"
        "/u - Update required channel link and archive button\n\n"
        "🗂️ Content Management:\n"
        "/collect - Collect posts from channels for search functionality\n"
        "/top - Mark a replied message as TOP post\n"
        "/rtop - Remove a TOP post (reply to message or use number)\n"
        "/utop <id> - Replace top post at given index\n"
        "/top_r <id> - Remove a top post by ID\n"
        "/tops - Export top posts data\n"
        "/receive_tops - Import top posts from file\n\n"
        "👥 Subscriber Management:\n"
        "/subs - Export current subscriber IDs as .txt file\n"
        "/d_failed - Manage failed subscribers\n"
        "/su - Daily summary and analytics\n\n"
        "💾 Persistent Scheduling:\n"
        "• Messages are automatically saved to survive bot restarts\n"
        "• Media files are cleaned up after successful sending\n"
        "• Scheduling state is preserved across restarts\n\n"
        "ℹ️ How Automatic Operation Works:\n"
        "• Each message is scheduled for its exact calculated time (shown in /s)\n"
        "• Messages are sent automatically at their scheduled times\n"
        "• Each sent message is automatically removed from the queue\n"
        "• Operation continues until all messages are sent\n"
        "• Admin receives notifications for each successful send")

    await update.message.reply_text(help_text)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show main menu"""
    user_id = update.effective_user.id

    if is_admin(user_id):
        await update.message.reply_text(
            "⛔ Admins cannot use subscriber features.")
        return

    # Auto-add subscriber on any interaction (for non-admins)
    await auto_add_subscriber(user_id)

    # Check channel membership with improved logic
    is_member = await check_channel_membership(context, user_id)
    if not is_member:
        keyboard = [[
            InlineKeyboardButton("🔗 Join Channel First",
                                 url=current_channel_link)
        ]]
        await update.message.reply_text(
            "⚠️ You must join our channel first to use this bot!\n\n"
            "👆 Click the button above to join, then send /start again.",
            reply_markup=InlineKeyboardMarkup(keyboard))
        return

    menu_text = ("🔞 Archive Bot Menu 🔞\n\n"
                 "Choose an option below:")
    await update.message.reply_text(menu_text, reply_markup=get_main_menu())


async def show_top_posts_page(context_object, page=0, is_edit=False):
    """Unified function to show top posts with pagination"""
    # Reload top posts from file to ensure consistency
    load_top_posts()

    if not top_posts:
        message_text = ("📭 No top posts available yet.\n"
                        "Admins haven't marked any posts as TOP.")
        reply_markup = get_navigation_buttons()

        if is_edit:
            await context_object.edit_message_text(message_text,
                                                   reply_markup=reply_markup)
        else:
            await context_object.reply_text(message_text,
                                            reply_markup=reply_markup)
        return

    # Pagination settings
    posts_per_page = 10
    total_pages = (len(top_posts) + posts_per_page - 1) // posts_per_page

    # Ensure page is within valid range
    page = max(0, min(page, total_pages - 1))

    start_idx = page * posts_per_page
    end_idx = min(start_idx + posts_per_page, len(top_posts))

    # Create buttons for current page posts
    keyboard = []
    for idx in range(start_idx, end_idx):
        top_post = top_posts[idx]
        # Get first line/100 characters for button text
        button_text = ""
        if top_post.text:
            button_text = top_post.text.split('\n')[0][:100]
        elif top_post.media and top_post.media[0].get('caption'):
            button_text = top_post.media[0]['caption'].split('\n')[0][:100]
        else:
            button_text = f"Media Post #{idx + 1}"

        # Truncate if too long and add ellipsis
        if len(button_text) > 100:
            button_text = button_text[:97] + "..."

        keyboard.append([
            InlineKeyboardButton(f"{idx + 1}. {button_text}",
                                 callback_data=f"top_post_{idx}")
        ])

    # Add pagination buttons if needed
    nav_buttons = []
    if page > 0:
        nav_buttons.append(
            InlineKeyboardButton("⬅️ Previous",
                                 callback_data=f"top_page_{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(
            InlineKeyboardButton("Next ➡️",
                                 callback_data=f"top_page_{page+1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Add menu button
    keyboard.append(
        [InlineKeyboardButton("📚 Menu", callback_data="show_menu")])

    message_text = (
        f"🔥 TOP Posts (Page {page + 1}/{total_pages}):\n\n"
        f"📝 Showing {end_idx - start_idx} of {len(top_posts)} posts")
    reply_markup = InlineKeyboardMarkup(keyboard)

    if is_edit:
        await context_object.edit_message_text(message_text,
                                               reply_markup=reply_markup)
    else:
        await context_object.reply_text(message_text,
                                        reply_markup=reply_markup)


async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show top posts or mark as top (admin only)"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    if is_admin(user_id) and update.message.reply_to_message:
        # Admin marking a message as top
        await mark_as_top(update, context)
        return

    # Use unified pagination function
    await show_top_posts_page(update.message, page=0, is_edit=False)


async def subs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show subscriber count and export IDs to file (admin only)"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return

    if not subscribers:
        await update.message.reply_text("📊 No subscribers found.")
        return

    try:
        # Sort subscriber IDs for consistent display
        sorted_subscribers = sorted(list(subscribers))

        # Create file with subscriber IDs
        file_content = "\n".join(str(sub_id) for sub_id in sorted_subscribers)

        # Write to temporary file
        filename = "subscriber_ids.txt"
        with open(filename, 'w') as f:
            f.write(file_content)

        # Send the file to admin
        with open(filename, 'rb') as f:
            await context.bot.send_document(
                chat_id=user_id,
                document=f,
                filename=filename,
                caption=f"📊 Subscriber Database Export\n\n"
                f"👥 Total subscribers: {len(subscribers)}\n"
                f"📁 File contains: {len(sorted_subscribers)} subscriber IDs\n"
                f"📅 Exported at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        # Clean up the temporary file
        os.remove(filename)

        logging.info(
            f"Admin {user_id} exported {len(subscribers)} subscriber IDs to file"
        )

    except Exception as e:
        logging.error(f"Error creating subscriber export file: {e}")
        await update.message.reply_text(
            "❌ Error creating subscriber export file. Please try again.")

        # Fallback to text message if file creation fails
        subs_message = f"📊 Current subscribers: {len(subscribers)}\n\n"
        subs_message += "👥 Subscriber IDs:\n"

        # Add each subscriber ID
        for idx, sub_id in enumerate(sorted_subscribers, 1):
            subs_message += f"{idx}. {sub_id}\n"

        # Check if message is too long (Telegram limit is ~4096 characters)
        if len(subs_message) > 4000:
            # Split into multiple messages if too long
            await update.message.reply_text(
                f"📊 Current subscribers: {len(subscribers)}")

            # Send IDs in chunks
            chunk_size = 50  # IDs per message
            for i in range(0, len(sorted_subscribers), chunk_size):
                chunk = sorted_subscribers[i:i + chunk_size]
                chunk_message = f"👥 Subscriber IDs ({i+1}-{min(i+chunk_size, len(sorted_subscribers))}):\n"
                for idx, sub_id in enumerate(chunk, i + 1):
                    chunk_message += f"{idx}. {sub_id}\n"
                await update.message.reply_text(chunk_message)
        else:
            await update.message.reply_text(subs_message)


async def remove_top_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a top post (admin only)"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return

    # Reload top posts to ensure consistency
    load_top_posts()

    # If replying to a message, remove it from top posts
    if update.message.reply_to_message:
        reply = update.message.reply_to_message
        removed_count = 0

        # Create a temporary message to compare with existing top posts
        temp_msg = ScheduledMessage()
        if reply.text:
            temp_msg.set_text(reply.text, reply.entities)
        elif reply.photo:
            temp_msg.add_media(reply.photo[-1].file_id, 'photo', reply.caption,
                               reply.caption_entities)
        elif reply.video:
            temp_msg.add_media(reply.video.file_id, 'video', reply.caption,
                               reply.caption_entities)
        else:
            await update.message.reply_text(
                "⚠️ Only text, photo, or video messages can be removed from TOP"
            )
            return

        # Remove matching posts
        for i in range(len(top_posts) - 1, -1, -1):
            if temp_msg.is_duplicate_of(top_posts[i]):
                top_posts.pop(i)
                removed_count += 1

        if removed_count > 0:
            save_top_posts()
            await update.message.reply_text(
                f"✅ Removed {removed_count} matching post(s) from TOP! Total remaining: {len(top_posts)}"
            )
        else:
            await update.message.reply_text(
                "⚠️ This message is not in the TOP posts")
        return

    # If using number argument, remove by index
    if context.args and len(context.args) == 1:
        try:
            post_index = int(context.args[0]) - 1  # Convert to 0-based index
            if 0 <= post_index < len(top_posts):
                removed_post = top_posts.pop(post_index)
                save_top_posts()
                await update.message.reply_text(
                    f"✅ Removed TOP post #{post_index + 1}! Total remaining: {len(top_posts)}"
                )
            else:
                await update.message.reply_text(
                    f"⚠️ Invalid post number. Use 1-{len(top_posts)}")
        except ValueError:
            await update.message.reply_text("⚠️ Please provide a valid number")
        return

    # Show current top posts with numbers for removal
    if not top_posts:
        await update.message.reply_text("📭 No top posts to remove.")
        return

    posts_list = "🗑️ TOP Posts - Choose number to remove:\n\n"
    for idx, post in enumerate(top_posts, 1):
        content = ""
        if post.text:
            content = post.text[:50] + "..." if len(
                post.text) > 50 else post.text
        elif post.media and post.media[0].get('caption'):
            content = post.media[0]['caption'][:50] + "..." if len(
                post.media[0]['caption']) > 50 else post.media[0]['caption']
        else:
            content = f"Media post"

        posts_list += f"{idx}. {content}\n"

    posts_list += f"\nUse: /rtop [number] to remove\nOr reply to a message with /rtop"
    await update.message.reply_text(posts_list)


async def post_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ Please reply to the message you want to send immediately.")
        return

    reply = update.message.reply_to_message
    sent_to = []
    failed_to = []

    for channel in target_channels:
        try:
            if reply.media_group_id:
                # Create media group from current message
                media_group = []
                if reply.photo:
                    media_group.append(
                        InputMediaPhoto(
                            media=reply.photo[-1].file_id,
                            caption=reply.caption,
                            caption_entities=reply.caption_entities))
                elif reply.video:
                    media_group.append(
                        InputMediaVideo(
                            media=reply.video.file_id,
                            caption=reply.caption,
                            caption_entities=reply.caption_entities))
                if media_group:
                    await context.bot.send_media_group(chat_id=channel,
                                                       media=media_group)
            elif reply.photo:
                await context.bot.send_photo(
                    chat_id=channel,
                    photo=reply.photo[-1].file_id,
                    caption=reply.caption or "",
                    caption_entities=reply.caption_entities)
            elif reply.video:
                await context.bot.send_video(
                    chat_id=channel,
                    video=reply.video.file_id,
                    caption=reply.caption or "",
                    caption_entities=reply.caption_entities)
            elif reply.audio:
                await context.bot.send_audio(chat_id=channel,
                                             audio=reply.audio.file_id,
                                             caption=reply.caption or "")
            elif reply.voice:
                await context.bot.send_voice(chat_id=channel,
                                             voice=reply.voice.file_id,
                                             caption=reply.caption or "")
            elif reply.document:
                await context.bot.send_document(
                    chat_id=channel,
                    document=reply.document.file_id,
                    caption=reply.caption or "")
            elif reply.text:
                await context.bot.send_message(chat_id=channel,
                                               text=reply.text,
                                               entities=reply.entities)
            sent_to.append(channel)
        except Exception as e:
            failed_to.append((channel, str(e)))

    report = f"✅ Sent to: {', '.join(map(str, sent_to))}" if sent_to else ""
    if failed_to:
        report += f"\n⚠️ Failed: " + ", ".join(
            [f"{ch} ({err})" for ch, err in failed_to])
    await update.message.reply_text(report or "❌ Failed to send.")


async def show_scheduled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    if not scheduled_messages:
        await update.message.reply_text(
            "📭 No messages are currently scheduled.")
        return

    messages_list = "📋 Scheduled Messages:\n\n"
    egypt_tz = pytz.timezone('Africa/Cairo')
    current_time = datetime.now(egypt_tz)

    # Calculate next odd hour
    current_hour = current_time.hour
    if current_hour % 2 == 0:
        next_odd_hour = current_hour + 1
    else:
        next_odd_hour = current_hour + 2

    if next_odd_hour >= 24:
        next_odd_hour = 1
        next_send_time = current_time.replace(
            hour=next_odd_hour, minute=0, second=0) + timedelta(days=1)
    else:
        next_send_time = current_time.replace(hour=next_odd_hour,
                                              minute=0,
                                              second=0)

    for idx, msg in enumerate(scheduled_messages, 1):
        # Calculate this message's send time
        msg_send_time = next_send_time + timedelta(hours=2 * (idx - 1))
        time_str = msg_send_time.strftime("%Y-%m-%d %I:%M %p")

        # Get content
        content = msg.text
        if not content and msg.media:
            content = msg.media[0].get('caption', '')

        if content:
            preview = content.split('\n')[0]
            messages_list += f"{idx}. [Will send at: {time_str}] {preview}\n"

    await update.message.reply_text(messages_list)


async def auto_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    global auto_scheduling_active
    try:
        if auto_scheduling_active:
            await update.message.reply_text(
                "⚠️ Automatic scheduling is already active!")
            return

        if not scheduled_messages:
            await update.message.reply_text(
                "⚠️ No messages scheduled. Please add messages first!")
            return

        if not target_channels:
            await update.message.reply_text(
                "⚠️ No target channels configured. Please add channels first!")
            return

        auto_scheduling_active = True

        # Schedule all messages for their calculated times
        await schedule_all_messages(context)

        await update.message.reply_text(
            "🤖 Automatic operation started!\n\n"
            "✅ The bot will now:\n"
            "• Send messages automatically at their scheduled times\n"
            "• Remove sent messages from queue automatically\n"
            "• Continue until all messages are sent\n"
            "• Send admin notifications about successful sends\n\n"
            f"📅 {len(scheduled_messages)} messages have been scheduled for automatic sending!"
        )

    except Exception as e:
        logging.error(f"Error starting scheduler: {str(e)}")
        await update.message.reply_text(
            "⚠️ Error starting scheduler. Please try again.")
        auto_scheduling_active = False


async def auto_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    global auto_scheduling_active
    if not auto_scheduling_active:
        await update.message.reply_text(
            "⚠️ Automatic scheduling is already stopped!")
        return

    auto_scheduling_active = False

    # Clear all auto-send jobs
    jobs_cleared = 0
    for job in context.job_queue.jobs():
        if job.name and job.name.startswith("auto_send_message_"):
            job.schedule_removal()
            jobs_cleared += 1

    await update.message.reply_text(
        f"🛑 Automatic message scheduling has been stopped!\n"
        f"📅 Cleared {jobs_cleared} scheduled jobs.\n"
        f"📋 {len(scheduled_messages)} messages remain in queue.\n"
        f"🔄 Use /a to restart automatic scheduling.")


async def send_immediate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ Please reply to a message to send it immediately.")
        return

    reply_msg = update.message.reply_to_message

    # Handle text messages with entities (hyperlinks)
    if reply_msg.text:
        await context.bot.send_message(chat_id=update.effective_chat.id,
                                       text=reply_msg.text,
                                       entities=reply_msg.entities)

    # Handle media messages
    if reply_msg.photo:
        await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=reply_msg.photo[-1].file_id,
            caption=reply_msg.caption,
            caption_entities=reply_msg.caption_entities)
    elif reply_msg.video:
        await context.bot.send_video(
            chat_id=update.effective_chat.id,
            video=reply_msg.video.file_id,
            caption=reply_msg.caption,
            caption_entities=reply_msg.caption_entities)

    await update.message.reply_text("✅ Message sent immediately!")


async def mark_as_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    """Mark a replied message as top post"""
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ Please reply to a message to mark it as TOP")
        return

    # Reload top posts to ensure consistency
    load_top_posts()

    reply = update.message.reply_to_message
    top_msg = ScheduledMessage()

    if reply.text:
        top_msg.set_text(reply.text, reply.entities)
    elif reply.photo:
        top_msg.add_media(reply.photo[-1].file_id, 'photo', reply.caption,
                          reply.caption_entities)
    elif reply.video:
        top_msg.add_media(reply.video.file_id, 'video', reply.caption,
                          reply.caption_entities)
    else:
        await update.message.reply_text(
            "⚠️ Only text, photo, or video messages can be marked as TOP")
        return

    # Check for duplicates
    for existing_top in top_posts:
        if top_msg.is_duplicate_of(existing_top):
            await update.message.reply_text(
                "⚠️ This message is already marked as TOP")
            return

    top_posts.append(top_msg)
    save_top_posts()
    await update.message.reply_text(
        f"🔥 Message marked as TOP! Total top posts: {len(top_posts)}")


async def verify_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    await update.message.reply_text(
        f"✅ Verified! You are the admin (ID: {user_id})")


def is_trigger_already_scheduled(job_queue):
    """Check if an auto_send_command job is already scheduled"""
    return any(job.name == "auto_send_command" for job in job_queue.jobs())


async def schedule_all_messages(context: ContextTypes.DEFAULT_TYPE):
    """Schedule all messages for their calculated times"""
    if not auto_scheduling_active or not scheduled_messages:
        return

    # Clear any existing auto jobs
    for job in context.job_queue.jobs():
        if job.name and job.name.startswith("auto_send_message_"):
            job.schedule_removal()

    current_time = datetime.now(cairo_tz)
    next_odd_hour = current_time.hour + (1 if current_time.hour %
                                         2 == 0 else 2)

    if next_odd_hour >= 24:
        next_odd_hour = 1
        next_send_time = current_time.replace(
            hour=next_odd_hour, minute=0, second=0,
            microsecond=0) + timedelta(days=1)
    else:
        next_send_time = current_time.replace(hour=next_odd_hour,
                                              minute=0,
                                              second=0,
                                              microsecond=0)

    # Schedule each message for its calculated time
    for idx, scheduled_msg in enumerate(scheduled_messages):
        msg_send_time = next_send_time + timedelta(hours=2 * idx)
        scheduled_msg.send_time = msg_send_time  # Store send time
        delay = (msg_send_time - current_time).total_seconds()

        if delay > 0:  # Only schedule future messages
            context.job_queue.run_once(
                send_scheduled_message_auto,
                when=delay,
                data={
                    "message_index": idx,
                    "message_id": id(scheduled_msg)
                },
                name=f"auto_send_message_{idx}_{id(scheduled_msg)}")
            logging.info(
                f"Message {idx+1} scheduled for {msg_send_time.strftime('%Y-%m-%d %H:%M:%S')} Cairo time"
            )


async def schedule_next_message(context: ContextTypes.DEFAULT_TYPE):
    """Legacy function - now redirects to schedule_all_messages"""
    if auto_scheduling_active:
        await schedule_all_messages(context)


async def handle_callback_query(update: Update,
                                context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks for subscribers"""
    query = update.callback_query
    user_id = query.from_user.id

    # Handle admin-only callback actions first
    if query.data.startswith("delete_failed_") or query.data == "clear_all_failed":
        if not is_admin(user_id):
            await query.answer("❌ Admin only")
            return
        # Continue processing admin actions below
    elif is_admin(user_id):
        await query.answer("⚠️ Admins cannot use subscriber features")
        return
    else:
        # Auto-add subscriber on any interaction (for non-admins)
        await auto_add_subscriber(user_id)

    await query.answer()

    # Check channel membership with improved logic
    is_member = await check_channel_membership(context, user_id)
    if not is_member:
        keyboard = [[
            InlineKeyboardButton("🔗 Join Channel First",
                                 url=current_channel_link)
        ]]
        await query.edit_message_text(
            "⚠️ You must join our channel first to use this bot!\n\n"
            "👆 Click the button above to join, then send /start again.",
            reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if query.data == "show_menu":
        menu_text = ("🔞 Archive Bot Menu 🔞\n\n"
                     "Choose an option below:")
        await query.edit_message_text(menu_text, reply_markup=get_main_menu())

    elif query.data == "show_top_posts" or query.data.startswith("top_page_"):
        # Get page number
        page = 0
        if query.data.startswith("top_page_"):
            page = int(query.data.split("_")[2])

        # Use unified pagination function
        await show_top_posts_page(query, page=page, is_edit=True)

    elif query.data.startswith("top_post_"):
        # Handle individual top post selection
        try:
            post_index = int(query.data.split("_")[2])
            if 0 <= post_index < len(top_posts):
                selected_post = top_posts[post_index]

                await query.answer("📤 Sending selected top post...")

                # Send the selected top post
                if selected_post.text and not selected_post.media:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=selected_post.text,
                        entities=selected_post.entities,
                        reply_markup=get_navigation_buttons())
                elif selected_post.media:
                    if len(selected_post.media) == 1:
                        media = selected_post.media[0]
                        if media['type'] == 'photo':
                            await context.bot.send_photo(
                                chat_id=user_id,
                                photo=media['file_id'],
                                caption=media['caption'],
                                caption_entities=media['caption_entities'],
                                reply_markup=get_navigation_buttons())
                        elif media['type'] == 'video':
                            await context.bot.send_video(
                                chat_id=user_id,
                                video=media['file_id'],
                                caption=media['caption'],
                                caption_entities=media['caption_entities'],
                                reply_markup=get_navigation_buttons())
                    else:
                        # Media group
                        media_group = []
                        for idx, media in enumerate(selected_post.media):
                            if media['type'] == 'photo':
                                media_group.append(
                                    InputMediaPhoto(media=media['file_id'],
                                                    caption=media['caption']
                                                    if idx == 0 else None,
                                                    caption_entities=media[
                                                        'caption_entities']
                                                    if idx == 0 else None))
                            elif media['type'] == 'video':
                                media_group.append(
                                    InputMediaVideo(media=media['file_id'],
                                                    caption=media['caption']
                                                    if idx == 0 else None,
                                                    caption_entities=media[
                                                        'caption_entities']
                                                    if idx == 0 else None))
                        if media_group:
                            await context.bot.send_media_group(
                                chat_id=user_id, media=media_group)
                            # Send navigation buttons after media group
                            await context.bot.send_message(
                                chat_id=user_id,
                                text="",
                                reply_markup=get_navigation_buttons())
            else:
                await query.answer("❌ Invalid post selection")
        except Exception as e:
            logging.error(f"Error sending top post: {e}")
            await query.answer("❌ Error sending post")

    elif query.data == "show_latest_posts":
        # Get the latest posts
        collected_posts = await load_collected_posts()

        if not collected_posts:
            await query.edit_message_text(
                "📭 No latest posts available yet.",
                reply_markup=get_navigation_buttons())
            return

        # Create buttons for each latest post
        keyboard = []
        for idx, post in enumerate(
                collected_posts[:10]):  # Limit to the latest 10 posts
            # Get first line/100 characters for button text
            button_text = ""
            if post.get('text'):
                button_text = post['text'].split('\n')[0][:100]
            elif post.get('caption'):
                button_text = post['caption'].split('\n')[0][:100]
            else:
                button_text = f"Post #{idx + 1}"

            # Truncate if too long and add ellipsis
            if len(button_text) > 100:
                button_text = button_text[:97] + "..."

            keyboard.append([
                InlineKeyboardButton(f"{idx + 1}. {button_text}",
                                     callback_data=f"latest_post_{idx}")
            ])

        # Add navigation button
        keyboard.append(
            [InlineKeyboardButton("📚 Menu", callback_data="show_menu")])

        await query.edit_message_text(
            f"🔥 Choose a latest post to view:\n\n"
            f"📝 {len(collected_posts)} latest posts available",
            reply_markup=InlineKeyboardMarkup(keyboard))

    elif query.data.startswith("latest_post_"):
        # Handle individual latest post selection
        try:
            collected_posts = await load_collected_posts()
            post_index = int(query.data.split("_")[2])
            if 0 <= post_index < len(collected_posts):
                selected_post = collected_posts[post_index]

                await query.answer("📤 Sending selected latest post...")

                # Send the selected latest post
                if selected_post.get('text') and not selected_post.get(
                        'has_photo') and not selected_post.get('has_video'):
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=selected_post['text'],
                        reply_markup=get_navigation_buttons(),
                        protect_content=True)
                elif selected_post.get('has_photo'):
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=selected_post['photo'],
                        caption=selected_post['caption'],
                        reply_markup=get_navigation_buttons(),
                        protect_content=True)
                elif selected_post.get('has_video'):
                    await context.bot.send_video(
                        chat_id=user_id,
                        video=selected_post['video'],
                        caption=selected_post['caption'],
                        reply_markup=get_navigation_buttons(),
                        protect_content=True)
                else:
                    await query.answer("❌ Could not retrieve post content")
            else:
                await query.answer("❌ Invalid post selection")
        except Exception as e:
            logging.error(f"Error sending latest post: {e}")
            await query.answer("❌ Error sending post")

    elif query.data.startswith("delete_failed_"):
        # Handle failed subscriber deletion (admin only)
        if not is_admin(user_id):
            await query.answer("❌ Admin only")
            return

        sub_id_to_delete = query.data.split("_")[2]

        # Remove from failed subscribers
        if sub_id_to_delete in failed_subscribers:
            del failed_subscribers[sub_id_to_delete]
            save_failed_subscribers()

        # Remove from main subscribers
        try:
            subscribers.discard(int(sub_id_to_delete))
            save_subscribers()
        except:
            pass

        await query.answer("✅ Subscriber deleted")
        await query.edit_message_text(
            "✅ Failed subscriber has been permanently deleted.")

    elif query.data == "clear_all_failed":
        # Clear all failed subscribers (admin only)
        if not is_admin(user_id):
            await query.answer("❌ Admin only")
            return

        failed_subscribers.clear()
        save_failed_subscribers()

        await query.answer("✅ All failed subscribers cleared")
        await query.edit_message_text(
            "✅ All failed subscriber records have been cleared.")


async def send_scheduled_message_auto(context: ContextTypes.DEFAULT_TYPE):
    """Automatically send a specific scheduled message"""
    if not auto_scheduling_active:
        return

    try:
        job_data = context.job.data
        message_index = job_data["message_index"]
        message_id = job_data["message_id"]

        # Find the message by its ID (in case the list was modified)
        scheduled_msg = None
        actual_index = None

        for idx, msg in enumerate(scheduled_messages):
            if id(msg) == message_id:
                scheduled_msg = msg
                actual_index = idx
                break

        if not scheduled_msg:
            logging.warning(f"Message with ID {message_id} not found in queue")
            return

        success = False
        sent_channels = []

        for channel_id in target_channels:
            try:
                if scheduled_msg.text and not scheduled_msg.media:
                    await context.bot.send_message(
                        chat_id=channel_id,
                        text=scheduled_msg.text,
                        entities=scheduled_msg.entities)
                    success = True
                    sent_channels.append(channel_id)
                elif scheduled_msg.media:
                    media_group = []
                    for idx, media in enumerate(scheduled_msg.media):
                        if media['type'] == 'photo':
                            media_group.append(
                                InputMediaPhoto(
                                    media=media['file_id'],
                                    caption=media['caption']
                                    if idx == 0 else None,
                                    caption_entities=media['caption_entities']
                                    if idx == 0 else None))
                        elif media['type'] == 'video':
                            media_group.append(
                                InputMediaVideo(
                                    media=media['file_id'],
                                    caption=media['caption']
                                    if idx == 0 else None,
                                    caption_entities=media['caption_entities']
                                    if idx == 0 else None))
                    if media_group:
                        await context.bot.send_media_group(chat_id=channel_id,
                                                           media=media_group)
                        success = True
                        sent_channels.append(channel_id)

            except Exception as e:
                logging.error(
                    f"Failed to send to channel {channel_id}: {str(e)}")
                continue

        # Send to subscribers after successful channel sending
        subscriber_count = 0
        failed_subscribers = 0

        if success:  # Only send to subscribers if at least one channel was successful
            for subscriber_id in subscribers:
                try:
                    if scheduled_msg.text and not scheduled_msg.media:
                        await context.bot.send_message(
                            chat_id=subscriber_id,
                            text=scheduled_msg.text,
                            entities=scheduled_msg.entities,
                            reply_markup=get_navigation_buttons(),
                            protect_content=True)
                    elif scheduled_msg.media:
                        if len(scheduled_msg.media) == 1:
                            media = scheduled_msg.media[0]
                            if media['type'] == 'photo':
                                await context.bot.send_photo(
                                    chat_id=subscriber_id,
                                    photo=media['file_id'],
                                    caption=media['caption'],
                                    caption_entities=media['caption_entities'],
                                    reply_markup=get_navigation_buttons())
                            elif media['type'] == 'video':
                                await context.bot.send_video(
                                    chat_id=subscriber_id,
                                    video=media['file_id'],
                                    caption=media['caption'],
                                    caption_entities=media['caption_entities'],
                                    reply_markup=get_navigation_buttons())
                        else:
                            # Media group
                            media_group = []
                            for idx, media in enumerate(scheduled_msg.media):
                                if media['type'] == 'photo':
                                    media_group.append(
                                        InputMediaPhoto(
                                            media=media['file_id'],
                                            caption=media['caption']
                                            if idx == 0 else None,
                                            caption_entities=media[
                                                'caption_entities']
                                            if idx == 0 else None))
                                elif media['type'] == 'video':
                                    media_group.append(
                                        InputMediaVideo(
                                            media=media['file_id'],
                                            caption=media['caption']
                                            if idx == 0 else None,
                                            caption_entities=media[
                                                'caption_entities']
                                            if idx == 0 else None))
                            if media_group:
                                await context.bot.send_media_group(
                                    chat_id=subscriber_id, media=media_group)
                                # Send navigation buttons after media group
                                await context.bot.send_message(
                                    chat_id=subscriber_id,
                                    text="",
                                    reply_markup=get_navigation_buttons())
                    subscriber_count += 1
                except Exception as e:
                    failed_subscribers += 1
                    await track_failed_subscriber(subscriber_id)
                    logging.error(
                        f"Failed to send to subscriber {subscriber_id}: {e}")

        # Remove the sent message from the queue regardless of channel/subscriber success
        if scheduled_msg in scheduled_messages:
            scheduled_messages.remove(scheduled_msg)
            save_scheduled_messages()

        if success or subscriber_count > 0:

            # Get message preview for notification
            preview = ""
            if scheduled_msg.text:
                preview = scheduled_msg.text[:50] + "..." if len(
                    scheduled_msg.text) > 50 else scheduled_msg.text
            elif scheduled_msg.media:
                caption = scheduled_msg.media[0].get('caption', '')
                preview = f"Media: {caption[:30]}..." if caption else "Media message"

            await context.bot.send_message(
                chat_id=ADMIN_IDS[0],
                text=f"✅ Scheduled message sent automatically!\n\n"
                f"📤 Sent to {len(sent_channels)} channels\n"
                f"👥 Sent to {subscriber_count} subscribers\n"
                f"❌ Failed subscribers: {failed_subscribers}\n"
                f"📝 Content: {preview}\n"
                f"🕒 Time: {datetime.now(cairo_tz).strftime('%I:%M %p, %d/%m/%Y')}\n"
                f"📋 Remaining in queue: {len(scheduled_messages)}")

            # If this was the last message, notify admin
            if not scheduled_messages:
                await context.bot.send_message(
                    chat_id=ADMIN_IDS[0],
                    text="🎉 All scheduled messages have been sent!\n"
                    "📭 Queue is now empty.\n"
                    "🤖 Automatic operation completed.")
        else:
            await context.bot.send_message(
                chat_id=ADMIN_IDS[0],
                text=f"⚠️ Failed to send scheduled message to any channels")

    except Exception as e:
        logging.error(f"Error in send_scheduled_message_auto: {str(e)}")
        await context.bot.send_message(
            chat_id=ADMIN_IDS[0],
            text=f"⚠️ Error sending scheduled message: {str(e)}")


async def trigger_send_command(context: ContextTypes.DEFAULT_TYPE):
    """Legacy function for backward compatibility"""
    if not auto_scheduling_active or not scheduled_messages:
        return

    # This function is kept for backward compatibility
    # The new system uses send_scheduled_message_auto for individual message scheduling


async def send_scheduled_message(context: ContextTypes.DEFAULT_TYPE):
    try:
        # Extract the scheduled message and target channels from the job context
        job_data = context.job.data
        scheduled_msg = job_data[0]
        target_channels = job_data[1]
        success = False

        # Attempt to send the message to all target channels
        for channel_id in target_channels:
            try:
                # Check if the bot has admin rights in the channel
                chat = await context.bot.get_chat(channel_id)
                chat_member = await context.bot.get_chat_member(
                    chat_id=channel_id, user_id=context.bot.id)

                if chat_member.status not in ['administrator', 'creator']:
                    logging.warning(
                        f"Bot is not admin in channel {chat.title} ({channel_id}). Skipping..."
                    )
                    continue

                # Send text message
                if scheduled_msg.text and not scheduled_msg.media:
                    await context.bot.send_message(
                        chat_id=channel_id,
                        text=scheduled_msg.text,
                        entities=scheduled_msg.entities)
                    success = True

                # Send media group
                elif scheduled_msg.media:
                    media_group = []
                    for idx, media in enumerate(scheduled_msg.media):
                        if media['type'] == 'photo':
                            media_group.append(
                                InputMediaPhoto(
                                    media=media['file_id'],
                                    caption=media['caption']
                                    if idx == 0 else None,
                                    caption_entities=media['caption_entities']
                                    if idx == 0 else None))
                        elif media['type'] == 'video':
                            media_group.append(
                                InputMediaVideo(
                                    media=media['file_id'],
                                    caption=media['caption']
                                    if idx == 0 else None,
                                    caption_entities=media['caption_entities']
                                    if idx == 0 else None))

                    if media_group:
                        await context.bot.send_media_group(chat_id=channel_id,
                                                           media=media_group)
                        success = True

            except Exception as e:
                logging.error(
                    f"Failed to send message to channel {channel_id}: {str(e)}"
                )
                await context.bot.send_message(
                    chat_id=ADMIN_IDS[0],
                    text=
                    f"⚠️ Failed to send message to channel {channel_id}: {str(e)}"
                )

        # If message was sent successfully to at least one channel
        if success:
            if scheduled_msg in scheduled_messages:
                scheduled_messages.remove(scheduled_msg)
                await context.bot.send_message(
                    chat_id=ADMIN_IDS[0],
                    text="✅ Scheduled message sent and removed from queue!")
                logging.info(
                    f"Message successfully sent and removed from the schedule: {scheduled_msg}"
                )

    except Exception as e:
        logging.error(f"Error in send_scheduled_message: {str(e)}")
        await context.bot.send_message(
            chat_id=ADMIN_IDS[0],
            text=f"⚠️ Error in send_scheduled_message: {str(e)}")


async def remove_scheduled(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("⚠️ Please use: /r [message_number]")
        return

    try:
        msg_id = int(context.args[0])
        if msg_id < 1 or msg_id > len(scheduled_messages):
            await update.message.reply_text(
                "⚠️ Invalid message number. Use /s to see available messages.")
            return

        removed_msg = scheduled_messages.pop(msg_id - 1)
        save_scheduled_messages()
        # Clean up local files
        removed_msg.cleanup_local_files()
        await update.message.reply_text(f"✅ Removed message: {removed_msg}")
    except ValueError:
        await update.message.reply_text("⚠️ Please provide a valid number")


def is_time_slot_taken(scheduled_time):
    """Check if any message is already scheduled for the given time"""
    scheduled_hour = scheduled_time.hour

    for msg in scheduled_messages:
        idx = scheduled_messages.index(msg)
        current_time = datetime.now(cairo_tz)
        next_odd_hour = current_time.hour + (1 if current_time.hour %
                                             2 == 0 else 2)

        if next_odd_hour >= 24:
            next_odd_hour = 1
            next_send_time = current_time.replace(
                hour=next_odd_hour, minute=0, second=0) + timedelta(days=1)
        else:
            next_send_time = current_time.replace(hour=next_odd_hour,
                                                  minute=0,
                                                  second=0)

        msg_time = next_send_time + timedelta(hours=2 * idx)

        if msg_time.hour == scheduled_hour:
            return True
    return False


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Handle cases where update.effective_user might be None
        if not update.effective_user:
            logging.warning("Received update with no effective_user")
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id if update.effective_chat else None

        if not chat_id:
            logging.warning("Received update with no effective_chat")
            return

        # Auto-add subscriber on any interaction
        await auto_add_subscriber(user_id)

        # Monitor Tester channel for automatic collection
        if chat_id == -1002554306424:  # Tester channel
            message_id = update.message.message_id
            # Automatically collect new posts from Tester channel
            await auto_collect_new_post(context, chat_id, message_id)
            return

        # Check if admin is waiting to provide channel link
        global waiting_for_channel_link
        if is_admin(user_id) and user_id in waiting_for_channel_link:
            # This message should be treated as the new channel link
            new_link = update.message.text
            if new_link and new_link.startswith("https://t.me/"):
                await save_channel_link(new_link)
                await update.message.reply_text(
                    f"✅ Channel link updated to: {new_link}")
                waiting_for_channel_link.remove(user_id)
                return
            else:
                await update.message.reply_text(
                    "⚠️ Please provide a valid Telegram link starting with https://t.me/"
                )
                waiting_for_channel_link.remove(user_id)
                return

        # Only admins can add messages to schedule
        if not is_admin(user_id):
            return

        if not update.message:
            return

        msg = update.message
        media_group_id = msg.media_group_id

        # Handle media group messages
        if media_group_id:
            # Check if this media group is already being processed
            if media_group_id not in media_group_buffer:
                media_group_buffer[media_group_id] = {
                    'message': ScheduledMessage(media_group_id),
                    'first_message_id': msg.message_id,
                    'processed': False,
                    'media_count': 0,
                    'last_update': datetime.now()
                }

            buffer_data = media_group_buffer[media_group_id]
            scheduled_msg = buffer_data['message']

            try:
                # Add media to the message
                if msg.photo:
                    scheduled_msg.add_media(
                        msg.photo[-1].file_id, 'photo', msg.caption
                        if buffer_data['media_count'] == 0 else None,
                        msg.caption_entities
                        if buffer_data['media_count'] == 0 else None)
                elif msg.video:
                    scheduled_msg.add_media(
                        msg.video.file_id, 'video', msg.caption
                        if buffer_data['media_count'] == 0 else None,
                        msg.caption_entities
                        if buffer_data['media_count'] == 0 else None)

                buffer_data['media_count'] += 1
                buffer_data['last_update'] = datetime.now()

                # Show processing message only once
                if not buffer_data['processed']:
                    buffer_data['processed'] = True
                    position = len(scheduled_messages) + 1
                    await update.message.reply_text(
                        f"✅ Processing media group...\n"
                        f"📝 Queue position: #{position}\n"
                        "🕒 Please wait while collecting all media...",
                        reply_to_message_id=buffer_data['first_message_id'])

                # Schedule finalization after a delay to ensure all media is received
                context.job_queue.run_once(
                    lambda _: finalize_media_group(media_group_id, context,
                                                   update),
                    3,  # Wait 3 seconds after the last media
                    data=None,
                    name=f'finalize_media_group_{media_group_id}')

            except Exception as e:
                logging.error(
                    f"Error processing media group message: {str(e)}")
                return

        # Handle single messages (text, photo, or video)
        else:
            scheduled_msg = ScheduledMessage()
            if msg.text:
                scheduled_msg.set_text(msg.text, msg.entities)
            elif msg.photo:
                scheduled_msg.add_media(msg.photo[-1].file_id, 'photo',
                                        msg.caption, msg.caption_entities)
            elif msg.video:
                scheduled_msg.add_media(msg.video.file_id, 'video',
                                        msg.caption, msg.caption_entities)

            # Add to scheduled messages queue instead of immediate scheduling
            position = len(scheduled_messages) + 1
            scheduled_messages.append(scheduled_msg)
            save_scheduled_messages()

            # Calculate when this message will be sent based on its position in queue
            egypt_tz = pytz.timezone('Africa/Cairo')
            current_time = datetime.now(egypt_tz)
            current_hour = current_time.hour

            if current_hour % 2 == 0:
                next_odd_hour = current_hour + 1
            else:
                next_odd_hour = current_hour + 2

            if next_odd_hour >= 24:
                next_odd_hour = 1
                next_send_time = current_time.replace(
                    hour=next_odd_hour, minute=0, second=0,
                    microsecond=0) + timedelta(days=1)
            else:
                next_send_time = current_time.replace(hour=next_odd_hour,
                                                      minute=0,
                                                      second=0,
                                                      microsecond=0)

            # Calculate send time based on position in queue
            send_time = next_send_time + timedelta(hours=2 * (position - 1))
            scheduled_msg.send_time = send_time

            await update.message.reply_text(
                f"✅ Message added to scheduled queue!\n\n"
                f"📝 Queue position: #{position}\n"
                f"📅 Will send at: {send_time.strftime('%I:%M %p, %d/%m/%Y')} (Cairo time)\n"
                f"🤖 Use /a to start automatic sending",
                reply_to_message_id=msg.message_id)

    except Exception as e:
        logging.error(f"Error handling message: {str(e)}")
        if update.message:
            await update.message.reply_text(
                "⚠️ Error processing message. Please try again.")


async def get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    try:
        chat = update.effective_chat
        # Try to send a message to get the chat ID
        if chat.type in ['channel', 'supergroup', 'group']:
            chat_title = chat.title
            await context.bot.send_message(
                chat_id=chat.id,
                text=f"🆔 Chat ID for {chat_title} ({chat.type}): {chat.id}")
        else:
            chat_title = chat.title if chat.title else "Private Chat"
            await update.message.reply_text(
                f"🆔 Chat ID for {chat_title} ({chat.type}): {chat.id}")
    except Exception as e:
        logging.error(f"Error getting chat ID: {str(e)}")
        await update.message.reply_text(
            "⚠️ Error getting chat ID. Please try again.")


async def finalize_media_group(media_group_id,
                               context: ContextTypes.DEFAULT_TYPE,
                               update: Update):
    try:
        if media_group_id in media_group_buffer:
            buffer_data = media_group_buffer[media_group_id]
            scheduled_msg = buffer_data['message']
            first_message_id = buffer_data['first_message_id']

            # Check if the media group is already scheduled
            if scheduled_msg in scheduled_messages:
                logging.info(
                    f"Media group {media_group_id} is already scheduled. Skipping."
                )
                del media_group_buffer[media_group_id]
                return

            # Calculate the send time based on the position in the queue
            current_time = datetime.now(cairo_tz)
            next_odd_hour = current_time.hour + (1 if current_time.hour %
                                                 2 == 0 else 2)

            if next_odd_hour >= 24:
                next_odd_hour = 1
                next_send_time = current_time.replace(
                    hour=next_odd_hour, minute=0, second=0,
                    microsecond=0) + timedelta(days=1)
            else:
                next_send_time = current_time.replace(hour=next_odd_hour,
                                                      minute=0,
                                                      second=0,
                                                      microsecond=0)

            # Adjust send time based on the position in the queue
            position = len(scheduled_messages) + 1
            send_time = next_send_time + timedelta(hours=2 * (position - 1))

            # Add to scheduled messages and send confirmation
            scheduled_msg.send_time = send_time
            scheduled_messages.append(scheduled_msg)

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"✅ Media group scheduled successfully!\n\n"
                f"📝 Queue position: #{position}\n"
                f"📅 Will send at: {send_time.strftime('%I:%M %p, %d/%m/%Y')} (Cairo time)\n"
                f"📎 Media items: {len(scheduled_msg.media)}",
                reply_to_message_id=first_message_id)

            # Cleanup
            del media_group_buffer[media_group_id]
            logging.info(
                f"Media group {media_group_id} scheduled for {send_time.strftime('%Y-%m-%d %H:%M:%S')}"
            )

    except Exception as e:
        logging.error(
            f"Error finalizing media group {media_group_id}: {str(e)}")
        return


async def send_message_to_channels(context: ContextTypes.DEFAULT_TYPE,
                                   scheduled_msg: ScheduledMessage,
                                   target_channels):
    try:
        success = False

        # Attempt to send the message to all target channels
        for channel_id in target_channels:
            try:
                # Check if the bot has admin rights in the channel
                chat = await context.bot.get_chat(channel_id)
                chat_member = await context.bot.get_chat_member(
                    chat_id=channel_id, user_id=context.bot.id)

                if chat_member.status not in ['administrator', 'creator']:
                    logging.warning(
                        f"Bot is not admin in channel {chat.title} ({channel_id}). Skipping..."
                    )
                    continue

                # Send text message
                if scheduled_msg.text and not scheduled_msg.media:
                    await context.bot.send_message(
                        chat_id=channel_id,
                        text=scheduled_msg.text,
                        entities=scheduled_msg.entities)
                    success = True

                # Send media group
                elif scheduled_msg.media:
                    media_group = []
                    for idx, media in enumerate(scheduled_msg.media):
                        if media['type'] == 'photo':
                            media_group.append(
                                InputMediaPhoto(
                                    media=media['file_id'],
                                    caption=media['caption']
                                    if idx == 0 else None,
                                    caption_entities=media['caption_entities']
                                    if idx == 0 else None))
                        elif media['type'] == 'video':
                            media_group.append(
                                InputMediaVideo(
                                    media=media['file_id'],
                                    caption=media['caption']
                                    if idx == 0 else None,
                                    caption_entities=media['caption_entities']
                                    if idx == 0 else None))

                    if media_group:
                        await context.bot.send_media_group(chat_id=channel_id,
                                                           media=media_group)
                        success = True

            except Exception as e:
                logging.error(
                    f"Failed to send message to channel {channel_id}: {str(e)}"
                )
                await context.bot.send_message(
                    chat_id=ADMIN_IDS[0],
                    text=
                    f"⚠️ Failed to send message to channel {channel_id}: {str(e)}"
                )

        # If message was sent successfully to at least one channel
        if success:
            await context.bot.send_message(
                chat_id=ADMIN_IDS[0],
                text="✅ Scheduled message sent to target channels!")
            logging.info(
                f"Message successfully sent to target channels: {scheduled_msg}"
            )

    except Exception as e:
        logging.error(f"Error in send_scheduled_message: {str(e)}")
        await context.bot.send_message(
            chat_id=ADMIN_IDS[0],
            text=f"⚠️ Error in send_scheduled_message: {str(e)}")


async def send_first_message(update: Update,
                             context: ContextTypes.DEFAULT_TYPE,
                             msg_id=1):  # Added msg_id parameter
    # Check if user is admin (only when called from command)
    if update:
        user_id = update.effective_user.id
        # Auto-add subscriber on any interaction
        await auto_add_subscriber(user_id)
        if not is_admin(user_id):
            await update.message.reply_text(
                "⛔ Sorry, only the admin can use this command.")
            return

    async with send_lock:
        if not scheduled_messages:
            if update:
                await update.message.reply_text(
                    "📭 No messages are currently scheduled.")
            return

    # Determine the message to send
    if update and context.args and len(context.args) == 1:
        try:
            msg_id = int(context.args[0])
            if msg_id < 1 or msg_id > len(scheduled_messages):
                await update.message.reply_text(
                    "⚠️ Invalid message number. Use /s to see available messages."
                )
                return
        except ValueError:
            await update.message.reply_text("⚠️ Please provide a valid number."
                                            )
            return

    try:
        scheduled_msg = scheduled_messages[msg_id - 1]
    except IndexError:
        if update:
            await update.message.reply_text("⚠️ No message found with that ID."
                                            )
        return

    success = False
    subscriber_count = 0
    failed_subscribers = 0

    for channel_id in target_channels:
        try:
            # Force refresh chat member status
            chat = await context.bot.get_chat(channel_id)
            chat_member = await context.bot.get_chat_member(
                chat_id=channel_id, user_id=context.bot.id)

            if chat_member.status in ['administrator', 'creator']:
                try:
                    # Send text if present and no media
                    if scheduled_msg.text and not scheduled_msg.media:
                        await context.bot.send_message(
                            chat_id=channel_id,
                            text=scheduled_msg.text,
                            entities=scheduled_msg.entities)
                        success = True

                    # Send media group if there are media files
                    if scheduled_msg.media:
                        media_group = []
                        for idx, media in enumerate(scheduled_msg.media):
                            if media['type'] == 'photo':
                                media_group.append(
                                    InputMediaPhoto(media=media['file_id'],
                                                    caption=media['caption']
                                                    if idx == 0 else None,
                                                    caption_entities=media[
                                                        'caption_entities']
                                                    if idx == 0 else None))
                            elif media['type'] == 'video':
                                media_group.append(
                                    InputMediaVideo(media=media['file_id'],
                                                    caption=media['caption']
                                                    if idx == 0 else None,
                                                    caption_entities=media[
                                                        'caption_entities']
                                                    if idx == 0 else None))
                        await context.bot.send_media_group(chat_id=channel_id,
                                                           media=media_group)
                        success = True
                except Exception as e:
                    logging.error(
                        f"Failed tosend message to channel {chat.title}: {str(e)}"
                    )
                    if update:
                        await update.message.reply_text(
                            f"⚠️ Failed to send message to channel {chat.title}: {str(e)}"
                        )
            else:
                if update:
                    await update.message.reply_text(
                        f"⚠️ Bot is not admin in channel {chat.title} ({channel_id})"
                    )
        except Exception as e:
            if update:
                await update.message.reply_text(
                    f"⚠️ Failed to send to channel {channel_id}: {str(e)}")

    # Send to subscribers after successful channel sending
    if success:  # Only send to subscribers if at least one channel was successful
        for subscriber_id in subscribers:
            try:
                if scheduled_msg.text and not scheduled_msg.media:
                    await context.bot.send_message(
                        chat_id=subscriber_id,
                        text=scheduled_msg.text,
                        entities=scheduled_msg.entities,
                        reply_markup=get_navigation_buttons())
                elif scheduled_msg.media:
                    if len(scheduled_msg.media) == 1:
                        media = scheduled_msg.media[0]
                        if media['type'] == 'photo':
                            await context.bot.send_photo(
                                chat_id=subscriber_id,
                                photo=media['file_id'],
                                caption=media['caption'],
                                caption_entities=media['caption_entities'],
                                reply_markup=get_navigation_buttons(),
                                protect_content=True)
                        elif media['type'] == 'video':
                            await context.bot.send_video(
                                chat_id=subscriber_id,
                                video=media['file_id'],
                                caption=media['caption'],
                                caption_entities=media['caption_entities'],
                                reply_markup=get_navigation_buttons(),
                                protect_content=True)
                    else:
                        # Media group
                        media_group = []
                        for idx, media in enumerate(scheduled_msg.media):
                            if media['type'] == 'photo':
                                media_group.append(
                                    InputMediaPhoto(media=media['file_id'],
                                                    caption=media['caption']
                                                    if idx == 0 else None,
                                                    caption_entities=media[
                                                        'caption_entities']
                                                    if idx == 0 else None))
                            elif media['type'] == 'video':
                                media_group.append(
                                    InputMediaVideo(media=media['file_id'],
                                                    caption=media['caption']
                                                    if idx == 0 else None,
                                                    caption_entities=media[
                                                        'caption_entities']
                                                    if idx == 0 else None))
                        if media_group:
                            await context.bot.send_media_group(
                                chat_id=subscriber_id, media=media_group)
                            # Send navigation buttons after media group
                            await context.bot.send_message(
                                chat_id=subscriber_id,
                                text="",
                                reply_markup=get_navigation_buttons())
                subscriber_count += 1
            except Exception as e:
                failed_subscribers += 1
                logging.error(
                    f"Failed to send to subscriber {subscriber_id}: {e}")

    if success or subscriber_count > 0:
        # Remove the sent message from the scheduled list
        sent_msg = scheduled_messages.pop(msg_id - 1)
        save_scheduled_messages()
        sent_msg.cleanup_local_files()
        if update:
            await update.message.reply_text(
                f"✅ Message sent successfully!\n"
                f"📤 Sent to {len([ch for ch in target_channels if success])} channels\n"
                f"👥 Sent to {subscriber_count} subscribers\n"
                f"❌ Failed subscribers: {failed_subscribers}\n"
                f"Message removed from schedule.")
        logging.info(
            f"Message successfully sent and removed from the schedule: {scheduled_msg}"
        )
    else:
        if update:
            await update.message.reply_text(
                "⚠️ Failed to send the message to channels and subscribers.")

    # Schedule the next message
    await schedule_next_message(context)


async def show_message_preview(update: Update,
                               context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("⚠️ Please use: /show [message_number]"
                                        )
        return

    try:
        msg_id = int(context.args[0])
        if msg_id < 1 or msg_id > len(scheduled_messages):
            await update.message.reply_text(
                "⚠️ Invalid message number. Use /s to see available messages.")
            return

        msg = scheduled_messages[msg_id - 1]

        if msg.text and not msg.media:
            await context.bot.send_message(chat_id=update.effective_chat.id,
                                           text=msg.text,
                                           entities=msg.entities)
        elif msg.media:
            media_group = []
            for idx, media in enumerate(msg.media):
                if media['type'] == 'photo':
                    media_group.append(
                        InputMediaPhoto(
                            media=media['file_id'],
                            caption=media['caption'] if idx == 0 else None,
                            caption_entities=media['caption_entities']
                            if idx == 0 else None))
                elif media['type'] == 'video':
                    media_group.append(
                        InputMediaVideo(
                            media=media['file_id'],
                            caption=media['caption'] if idx == 0 else None,
                            caption_entities=media['caption_entities']
                            if idx == 0 else None))

            if len(media_group) > 1:
                await context.bot.send_media_group(
                    chat_id=update.effective_chat.id, media=media_group)
            else:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=msg.media[0]['file_id'],
                    caption=msg.media[0]['caption'],
                    caption_entities=msg.media[0]['caption_entities']
                ) if msg.media[0][
                    'type'] == 'photo' else await context.bot.send_video(
                        chat_id=update.effective_chat.id,
                        video=msg.media[0]['file_id'],
                        caption=msg.media[0]['caption'],
                        caption_entities=msg.media[0]['caption_entities'])
    except ValueError:
        await update.message.reply_text("⚠️ Please provide a valid number")


async def handle_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    if not context.args:
        if not target_channels:
            await update.message.reply_text("📭 No target channels configured.")
            return
        channels_list = "📋 Target Channels:\n\n"
        for channel in target_channels:
            try:
                chat = await context.bot.get_chat(channel)
                channels_list += f"ID: {channel} - Title: {chat.title}\n"
            except:
                channels_list += f"ID: {channel} - Unable to fetch details\n"
        await update.message.reply_text(channels_list)
        return

    action = context.args[0].lower()
    if len(context.args) != 2:
        await update.message.reply_text(
            "⚠️ Please use: /target add/remove [chat_id]")
        return

    try:
        chat_id = int(context.args[1])
    except ValueError:
        await update.message.reply_text("⚠️ Invalid chat ID format")
        return

    if action == "add":
        try:
            # Verify the chat exists and bot has access
            chat = await context.bot.get_chat(chat_id)
            # Verify bot's admin status
            chat_member = await context.bot.get_chat_member(
                chat_id=chat_id, user_id=context.bot.id)
            if chat_member.status not in ['administrator', 'creator']:
                await update.message.reply_text(
                    "⚠️ Bot must be an admin in the target channel")
                return
            target_channels.add(chat_id)
            await update.message.reply_text(
                f"✅ Added channel {chat.title} to targets")
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Failed to add channel: {str(e)}")
    elif action == "remove":
        if chat_id in target_channels:
            target_channels.remove(chat_id)
            await update.message.reply_text("✅ Channel removed from targets")
        else:
            await update.message.reply_text("⚠️ Channel not found in targets")
    else:
        await update.message.reply_text(
            "⚠️ Invalid action. Use 'add' or 'remove'")


async def schedule_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ Please reply to a message you want to schedule")
        return

    reply = update.message.reply_to_message
    media_group_id = reply.media_group_id

    # Handle media group messages
    if media_group_id:
        if media_group_id not in media_group_buffer:
            media_group_buffer[media_group_id] = {
                'message': ScheduledMessage(media_group_id),
                'first_message_id': reply.message_id,
                'processed': False
            }
        buffer_data = media_group_buffer[media_group_id]
        scheduled_msg = buffer_data['message']

        # Get all messages in the media group
        media_messages = []
        if reply.media_group_id:
            async for message in context.bot.get_chat(
                    update.effective_chat.id).iter_messages(
                        limit=10  # Adjust limit if needed
                    ):
                if message.media_group_id == media_group_id:
                    media_messages.append(message)
        else:
            media_messages = [reply]

        # Process all media messages
        for msg in media_messages:
            if msg.photo:
                scheduled_msg.add_media(
                    msg.photo[-1].file_id, 'photo',
                    msg.caption if not scheduled_msg.media else None,
                    msg.caption_entities if not scheduled_msg.media else None)
            elif msg.video:
                scheduled_msg.add_media(
                    msg.video.file_id, 'video',
                    msg.caption if not scheduled_msg.media else None,
                    msg.caption_entities if not scheduled_msg.media else None)

        # Show processing message for first media in group
        if not buffer_data['processed']:
            buffer_data['processed'] = True
            position = len(scheduled_messages) + 1

            # Calculate next send time in Cairo timezone
            cairo_tz = pytz.timezone('Africa/Cairo')
            current_time = datetime.now(cairo_tz)
            next_odd_hour = current_time.hour + (1 if current_time.hour %
                                                 2 == 0 else 2)

            if next_odd_hour >= 24:
                next_odd_hour = 1
                next_send_time = current_time.replace(
                    hour=next_odd_hour, minute=0, second=0,
                    microsecond=0) + timedelta(days=1)
            else:
                next_send_time = current_time.replace(hour=next_odd_hour,
                                                      minute=0,
                                                      second=0,
                                                      microsecond=0)

            # Check for duplicates before processing
            if is_duplicate_scheduled_message(scheduled_msg, next_send_time):
                await update.message.reply_text(
                    "⚠️ This media group is already scheduled for the same time!\n"
                    "Skipping to avoid duplicates.",
                    reply_to_message_id=buffer_data['first_message_id'])
                del media_group_buffer[media_group_id]
                return

            await update.message.reply_text(
                f"✅ Media group being processed\n\n"
                f"📝 Queue position: #{position}\n"
                f"📅 Scheduled for: {next_send_time.strftime('%I:%M %p, %d/%m/%Y')} (Cairo time)\n"
                "🕒 Please wait while processing...",
                reply_to_message_id=buffer_data['first_message_id'])

        # Add to scheduled messages after delay
        context.job_queue.run_once(
            lambda _: finalize_media_group(media_group_id, context, update),
            3,
            data=None,
            name=f'finalize_media_group_{media_group_id}')
    else:
        # Handle single messages (text, photo, or video)
        scheduled_msg = ScheduledMessage()
        if reply.text:
            scheduled_msg.set_text(reply.text, reply.entities)
        elif reply.photo:
            scheduled_msg.add_media(reply.photo[-1].file_id, 'photo',
                                    reply.caption, reply.caption_entities)
        elif reply.video:
            scheduled_msg.add_media(reply.video.file_id, 'video',
                                    reply.caption, reply.caption_entities)

        # Add to scheduled messages queue instead of immediate scheduling
        position = len(scheduled_messages) + 1
        scheduled_messages.append(scheduled_msg)

        # Calculate when this message will be sent based on its position in queue
        egypt_tz = pytz.timezone('Africa/Cairo')
        current_time = datetime.now(egypt_tz)
        current_hour = current_time.hour

        if current_hour % 2 == 0:
            next_odd_hour = current_hour + 1
        else:
            next_odd_hour = current_hour + 2

        if next_odd_hour >= 24:
            next_odd_hour = 1
            next_send_time = current_time.replace(
                hour=next_odd_hour, minute=0, second=0,
                microsecond=0) + timedelta(days=1)
        else:
            next_send_time = current_time.replace(hour=next_odd_hour,
                                                  minute=0,
                                                  second=0,
                                                  microsecond=0)

        # Calculate send time based on position in queue
        send_time = next_send_time + timedelta(hours=2 * (position - 1))
        scheduled_msg.send_time = send_time

        await update.message.reply_text(
            f"✅ Message added to scheduled queue!\n\n"
            f"📝 Queue position: #{position}\n"
            f"📅 Will send at: {send_time.strftime('%I:%M %p, %d/%m/%Y')} (Cairo time)\n"
            f"🤖 Use /a to start automatic sending",
            reply_to_message_id=reply.message_id)


def is_duplicate_scheduled_message(new_msg, scheduled_time):
    """Check if a message is already scheduled for the same time"""
    # Get the hour of the new message's schedule
    scheduled_hour = scheduled_time.hour

    for msg in scheduled_messages:
        # Calculate this message's scheduled time
        idx = scheduled_messages.index(msg)
        current_time = datetime.now(cairo_tz)
        next_odd_hour = current_time.hour + (1 if current_time.hour %
                                             2 == 0 else 2)

        if next_odd_hour >= 24:
            next_odd_hour = 1
            next_send_time = current_time.replace(
                hour=next_odd_hour, minute=0, second=0) + timedelta(days=1)
        else:
            next_send_time = current_time.replace(hour=next_odd_hour,
                                                  minute=0,
                                                  second=0)

        msg_time = next_send_time + timedelta(hours=2 * idx)

        # If the times match and content is duplicate
        if msg_time.hour == scheduled_hour and new_msg.is_duplicate_of(msg):
            return True

    return False


async def load_collected_posts():
    """Load collected posts from JSON file"""
    try:
        if not os.path.exists(COLLECTED_POSTS_FILE):
            return []

        async with aiofiles.open(COLLECTED_POSTS_FILE, 'r') as f:
            content = await f.read()
            if not content.strip():
                return []

        data = json.loads(content)
        return data.get("collected_posts", [])
    except Exception as e:
        logging.error(f"Error loading collected posts: {e}")
        return []


async def save_collected_posts(collected_posts):
    """Save collected posts to JSON file"""
    try:
        data = {
            "collected_posts": collected_posts,
            "last_collection": datetime.now().isoformat(),
            "total_posts": len(collected_posts)
        }

        async with aiofiles.open(COLLECTED_POSTS_FILE, 'w') as f:
            await f.write(json.dumps(data, indent=2))

        logging.info(f"Saved {len(collected_posts)} collected posts")
    except Exception as e:
        logging.error(f"Error saving collected posts: {e}")


def get_first_line(text, caption):
    """Extract first line from text or caption"""
    content = text if text else (caption if caption else "")
    if content:
        return content.split('\n')[0][:100].strip()
    return ""


def remove_duplicates_keep_newest(posts):
    """Remove duplicates based on first line, keeping the newest (highest message_id)"""
    seen_first_lines = {}

    for post in posts:
        first_line = get_first_line(post.get('text'), post.get('caption'))
        if first_line:
            message_id = post['message_id']

            if first_line in seen_first_lines:
                # Keep the post with higher message_id (newer)
                if message_id > seen_first_lines[first_line]['message_id']:
                    seen_first_lines[first_line] = post
            else:
                seen_first_lines[first_line] = post

    # Return only the unique posts (newest ones)
    return list(seen_first_lines.values())


async def auto_collect_new_post(context: ContextTypes.DEFAULT_TYPE, channel_id,
                                message_id):
    """Automatically collect a new post from channel and add to JSON"""
    try:
        # Rate limiting
        await asyncio.sleep(0.3)

        # Try to copy message (safer than forward)
        try:
            copied = await context.bot.copy_message(
                chat_id=ADMIN_IDS[0],  # Copy to admin chat instead
                from_chat_id=channel_id,
                message_id=message_id,
                disable_notification=True)

            # Extract message content from copied message
            new_post = {
                'message_id':
                message_id,
                'channel_id':
                channel_id,
                'text':
                copied.text if copied.text else None,
                'caption':
                copied.caption if copied.caption else None,
                'has_photo':
                bool(copied.photo),
                'has_video':
                bool(copied.video),
                'has_document':
                bool(copied.document),
                'media_group_id':
                copied.media_group_id if copied.media_group_id else None,
                'date':
                copied.date.isoformat() if copied.date else None,
                'collected_at':
                datetime.now().isoformat()
            }

            # Delete the copied message immediately
            try:
                await context.bot.delete_message(chat_id=ADMIN_IDS[0],
                                                 message_id=copied.message_id)
            except:
                pass  # Ignore deletion errors

        except Exception as copy_e:
            if "400" in str(copy_e) or "Message to copy not found" in str(
                    copy_e):
                logging.info(
                    f"❌ Skipped auto-collection of message {message_id} - message not found"
                )
            else:
                logging.error(
                    f"Failed to auto-collect message {message_id}: {copy_e}")
            return  # Skip this message

        # Load existing collected posts
        collected_posts = await load_collected_posts()

        # Check if this message is already collected
        existing_ids = [post['message_id'] for post in collected_posts]
        if message_id not in existing_ids:
            # Add new post
            collected_posts.append(new_post)

            # Remove duplicates, keeping newest
            collected_posts = remove_duplicates_keep_newest(collected_posts)

            # Sort by message_id descending (newest first)
            collected_posts.sort(key=lambda x: x['message_id'], reverse=True)

            # Save updated collection
            await save_collected_posts(collected_posts)

            logging.info(
                f"✅ Auto-collected new post {message_id} from channel {channel_id}"
            )
        else:
            logging.info(f"Message {message_id} already collected, skipping")

    except Exception as e:
        logging.error(f"Error auto-collecting post {message_id}: {e}")


async def find_message_range(context: ContextTypes.DEFAULT_TYPE, channel_id):
    """Find the actual range of existing messages in the channel"""
    logging.info("🔍 Detecting message range...")

    # Probe messages spaced 1000 IDs apart to find valid range
    max_id = 0
    min_id = 999999

    # Test ranges: 1000, 2000, 3000, etc. up to 10000
    for test_id in range(1000, 10000, 1000):
        try:
            await asyncio.sleep(0.3)  # Rate limiting
            # Try to forward message to admin instead of copying to bot
            await context.bot.forward_message(chat_id=ADMIN_IDS[0],
                                              from_chat_id=channel_id,
                                              message_id=test_id,
                                              disable_notification=True)
            max_id = max(max_id, test_id)
            min_id = min(min_id, test_id)
            logging.info(f"✅ Found valid message at ID {test_id}")
        except Exception as e:
            if "400" not in str(
                    e) and "Message to forward not found" not in str(e):
                logging.error(f"❌ Error testing message {test_id}: {e}")
            continue

    if max_id == 0:
        # If no messages found in big jumps, try smaller range
        for test_id in range(100, 1000, 100):
            try:
                await asyncio.sleep(0.3)
                await context.bot.forward_message(chat_id=ADMIN_IDS[0],
                                                  from_chat_id=channel_id,
                                                  message_id=test_id,
                                                  disable_notification=True)
                max_id = max(max_id, test_id)
                min_id = min(min_id, test_id)
                logging.info(f"✅ Found valid message at ID {test_id}")
            except:
                continue

    # Fine-tune the max range by checking around the highest found ID
    if max_id > 0:
        for test_id in range(max_id, max_id + 1000, 50):
            try:
                await asyncio.sleep(0.2)
                await context.bot.forward_message(chat_id=ADMIN_IDS[0],
                                                  from_chat_id=channel_id,
                                                  message_id=test_id,
                                                  disable_notification=True)
                max_id = test_id
            except:
                break

    return min_id if min_id != 999999 else 1, max_id


async def collect_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Collect all posts from Tester channel and save to collected_posts.json"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Check if user is admin
    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return

    await update.message.reply_text(
        "📝 Starting smart collection from Tester channel...")

    tester_channel_id = -1002554306424
    collected_posts = []
    media_groups = {}  # Track media groups

    try:
        # Get chat info to verify we can access it
        chat = await context.bot.get_chat(tester_channel_id)
        await update.message.reply_text(f"📡 Collecting from {chat.title}...")

        # Smart range detection
        await update.message.reply_text("🔍 Detecting message range...")
        min_id, max_id = await find_message_range(context, tester_channel_id)

        if max_id == 0:
            await update.message.reply_text(
                "❌ No valid messages found in channel.")
            return

        await update.message.reply_text(
            f"📊 Found message range: {min_id} to {max_id}")
        logging.info(f"Collection range: {min_id} to {max_id}")

        collected_from_channel = 0
        failed_messages = 0
        consecutive_failures = 0
        max_consecutive_failures = 30  # Reduced from 50

        # Collect messages in the detected range (work backwards from max)
        for msg_id in range(max_id, min_id - 1, -1):
            try:
                # Rate limiting
                await asyncio.sleep(0.3)

                # Try to copy message
                try:
                    copied = await context.bot.copy_message(
                        chat_id=ADMIN_IDS[0],  # Copy to admin chat instead
                        from_chat_id=tester_channel_id,
                        message_id=msg_id,
                        disable_notification=True)

                    # Reset consecutive failures counter on success
                    consecutive_failures = 0

                    # Extract message content
                    message_data = {
                        'message_id':
                        msg_id,
                        'channel_id':
                        tester_channel_id,
                        'text':
                        copied.text if copied.text else None,
                        'caption':
                        copied.caption if copied.caption else None,
                        'has_photo':
                        bool(copied.photo),
                        'has_video':
                        bool(copied.video),
                        'has_document':
                        bool(copied.document),
                        'media_group_id':
                        copied.media_group_id
                        if copied.media_group_id else None,
                        'date':
                        copied.date.isoformat() if copied.date else None,
                        'collected_at':
                        datetime.now().isoformat()
                    }

                    # Delete the copied message immediately to clean up
                    try:
                        await context.bot.delete_message(
                            chat_id=ADMIN_IDS[0], message_id=copied.message_id)
                    except:
                        pass  # Ignore deletion errors

                    # Handle media groups
                    if message_data.get('media_group_id'):
                        media_group_id = message_data['media_group_id']
                        if media_group_id not in media_groups:
                            media_groups[media_group_id] = []
                        media_groups[media_group_id].append(message_data)
                    else:
                        # Single message (not part of media group)
                        collected_posts.append(message_data)

                    collected_from_channel += 1
                    logging.info(f"✅ Collected message {msg_id}")

                    # Update progress every 50 successful collections
                    if collected_from_channel % 50 == 0:
                        await update.message.reply_text(
                            f"📊 Collected {collected_from_channel} posts so far..."
                        )

                except Exception as copy_e:
                    consecutive_failures += 1
                    failed_messages += 1

                    # Log skipped messages
                    if "400" in str(copy_e) or "403" in str(
                            copy_e) or "404" in str(copy_e):
                        logging.info(
                            f"❌ Skipped message {msg_id} - {str(copy_e)[:50]}")
                    else:
                        logging.error(
                            f"❌ Failed to collect message {msg_id}: {copy_e}")

                    # Stop if we have too many consecutive failures
                    if consecutive_failures >= max_consecutive_failures:
                        logging.info(
                            f"🛑 Stopping collection after {consecutive_failures} consecutive failures at message {msg_id}"
                        )
                        break
                    continue

            except Exception as msg_e:
                consecutive_failures += 1
                failed_messages += 1

                logging.error(f"❌ Error processing message {msg_id}: {msg_e}")

                # Stop if we have too many consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    logging.info(
                        f"🛑 Stopping collection after {consecutive_failures} consecutive failures at message {msg_id}"
                    )
                    break
                continue

        # Process media groups - keep only the first message of each group
        logging.info(f"📱 Processing {len(media_groups)} media groups...")
        for media_group_id, group_messages in media_groups.items():
            if group_messages:
                # Sort by message_id and take the first (lowest ID) as representative
                group_messages.sort(key=lambda x: x['message_id'])
                representative_msg = group_messages[0]
                representative_msg['is_media_group'] = True
                representative_msg['media_group_size'] = len(group_messages)
                collected_posts.append(representative_msg)

        if not collected_posts:
            await update.message.reply_text(
                f"❌ No posts collected. Failed messages: {failed_messages}")
            return

        # Remove duplicates, keeping newest
        await update.message.reply_text("🔄 Removing duplicates...")
        unique_posts = remove_duplicates_keep_newest(collected_posts)
        duplicates_removed = len(collected_posts) - len(unique_posts)

        # Sort by message_id descending (newest first)
        unique_posts.sort(key=lambda x: x['message_id'], reverse=True)

        # Save collected posts to JSON file
        await save_collected_posts(unique_posts)

        await update.message.reply_text(
            f"🎉 Smart collection completed!\n\n"
            f"📊 Message range: {min_id} to {max_id}\n"
            f"✅ Posts collected: {collected_from_channel}\n"
            f"📱 Media groups processed: {len(media_groups)}\n"
            f"❌ Failed/skipped messages: {failed_messages}\n"
            f"🗑️ Duplicates removed: {duplicates_removed}\n"
            f"💾 Final unique posts: {len(unique_posts)}\n"
            f"📁 Saved to: {COLLECTED_POSTS_FILE}\n"
            f"🔍 Subscribers can now search through collected posts!")

    except Exception as e:
        await update.message.reply_text(f"❌ Error collecting posts: {str(e)}")
        logging.error(f"Error in collect_posts: {e}")


async def update_channel_link(update: Update,
                              context: ContextTypes.DEFAULT_TYPE):
    """Update channel link (admin only)"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return

    # If no reply, ask for the new link
    if not update.message.reply_to_message:
        global waiting_for_channel_link
        waiting_for_channel_link.add(user_id)
        await update.message.reply_text("Send the new link.")
        return

    # Save the new link
    new_link = update.message.reply_to_message.text
    if new_link and new_link.startswith("https://t.me/"):
        await save_channel_link(new_link)
        await update.message.reply_text(
            f"✅ Channel link updated to: {new_link}")
    else:
        await update.message.reply_text(
            "⚠️ Please provide a valid Telegram link starting with https://t.me/"
        )


async def update_top_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Replace top post at given index (admin only)"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        await update.message.reply_text(
            "⛔ Sorry, only the admin can use this command.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ Please reply to a message to replace a top post.")
        return

    if not context.args or len(context.args) != 1:
        await update.message.reply_text("⚠️ Please use: /utop <number>")
        return

    # Reload top posts to ensure consistency
    load_top_posts()

    try:
        post_index = int(context.args[0]) - 1  # Convert to 0-based index
        if 0 <= post_index < len(top_posts):
            # Create new top post from replied message
            reply = update.message.reply_to_message
            new_top_msg = ScheduledMessage()

            if reply.text:
                new_top_msg.set_text(reply.text, reply.entities)
            elif reply.photo:
                new_top_msg.add_media(reply.photo[-1].file_id, 'photo',
                                      reply.caption, reply.caption_entities)
            elif reply.video:
                new_top_msg.add_media(reply.video.file_id, 'video',
                                      reply.caption, reply.caption_entities)
            else:
                await update.message.reply_text(
                    "⚠️ Only text, photo, or video messages can be used as TOP posts"
                )
                return

            # Replace the top post
            top_posts[post_index] = new_top_msg
            save_top_posts()
            await update.message.reply_text(
                f"✅ Replaced TOP post #{post_index + 1}! Total top posts: {len(top_posts)}"
            )
        else:
            await update.message.reply_text(
                f"⚠️ Invalid post number. Use 1-{len(top_posts)}")
    except ValueError:
        await update.message.reply_text("⚠️ Please provide a valid number")


async def remove_top_post_by_id(update: Update,
                                context: ContextTypes.DEFAULT_TYPE):
    """Remove a top post by ID (admin only)"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        return  # Silently ignore non-admin requests

    if not context.args or len(context.args) != 1:
        await update.message.reply_text("⚠️ Please use: /top_r <number>")
        return

    # Reload top posts to ensure consistency
    load_top_posts()

    try:
        post_index = int(context.args[0]) - 1  # Convert to 0-based index
        if 0 <= post_index < len(top_posts):
            removed_post = top_posts.pop(post_index)
            save_top_posts()
            await update.message.reply_text(
                f"✅ Removed TOP post #{post_index + 1}! Total remaining: {len(top_posts)}"
            )
        else:
            await update.message.reply_text(
                f"⚠️ Invalid post number. Use 1-{len(top_posts)}")
    except ValueError:
        await update.message.reply_text("⚠️ Please provide a valid number")


# Failed subscribers management
failed_subscribers = {}


async def load_failed_subscribers():
    """Load failed subscribers data"""
    global failed_subscribers
    try:
        if os.path.exists(FAILED_SUBSCRIBERS_FILE):
            async with aiofiles.open(FAILED_SUBSCRIBERS_FILE, 'r') as f:
                content = await f.read()
                if content.strip():
                    data = json.loads(content)
                    failed_subscribers = data.get("failed_subscribers", {})
    except Exception as e:
        logging.error(f"Failed to load failed subscribers: {e}")


def save_failed_subscribers():
    """Save failed subscribers data"""
    try:
        data = {"failed_subscribers": failed_subscribers}
        with open(FAILED_SUBSCRIBERS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.error(f"Failed to save failed subscribers: {e}")


async def track_failed_subscriber(subscriber_id):
    """Track a failed subscriber send"""
    global failed_subscribers
    str_id = str(subscriber_id)
    if str_id in failed_subscribers:
        failed_subscribers[str_id] += 1
    else:
        failed_subscribers[str_id] = 1
    save_failed_subscribers()


async def d_failed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show and manage failed subscribers (admin only)"""
    user_id = update.effective_user.id
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        return  # Silently ignore non-admin requests

    if not failed_subscribers:
        await update.message.reply_text("📊 No failed subscribers recorded.")
        return

    # Sort by failure count (highest first)
    sorted_failed = sorted(failed_subscribers.items(),
                           key=lambda x: x[1],
                           reverse=True)

    keyboard = []
    message_text = "❌ Failed Subscribers (Top failures):\n\n"

    for i, (sub_id,
            fail_count) in enumerate(sorted_failed[:10]):  # Show top 10
        message_text += f"{i+1}. ID: {sub_id} - Failed: {fail_count} times\n"
        keyboard.append([
            InlineKeyboardButton(f"🗑️ Delete {sub_id}",
                                 callback_data=f"delete_failed_{sub_id}")
        ])

    keyboard.append([
        InlineKeyboardButton("🔄 Clear All Failed",
                             callback_data="clear_all_failed")
    ])

    await update.message.reply_text(
        message_text, reply_markup=InlineKeyboardMarkup(keyboard))


async def su_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Daily summary and analytics (admin only)"""
    user_id = update.effective_user.id
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        return  # Silently ignore non-admin requests

    # Load analytics data
    analytics_data = await load_daily_analytics()

    summary_text = (
        "📊 Daily Summary & Analytics\n\n"
        f"👥 Subscribers:\n"
        f"• Total: {len(subscribers)}\n"
        f"• Old: {analytics_data.get('old_subscribers', 0)}\n"
        f"• New: {analytics_data.get('new_subscribers', 0)}\n\n"
        f"📤 Recent Sends (Last 12):\n"
        f"• Successful: {len(analytics_data.get('successful_sends', []))}\n"
        f"• Failed: {len(analytics_data.get('failed_sends', []))}\n\n"
        f"❌ Failed Subscribers: {len(failed_subscribers)}\n"
        f"📅 Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    await update.message.reply_text(summary_text)


async def load_daily_analytics():
    """Load daily analytics data"""
    try:
        if os.path.exists(DAILY_ANALYTICS_FILE):
            async with aiofiles.open(DAILY_ANALYTICS_FILE, 'r') as f:
                content = await f.read()
                if content.strip():
                    data = json.loads(content)
                    return data.get("analytics", {})
        return {}
    except Exception as e:
        logging.error(f"Failed to load daily analytics: {e}")
        return {}


def save_daily_analytics(analytics_data):
    """Save daily analytics data"""
    try:
        data = {"analytics": analytics_data}
        with open(DAILY_ANALYTICS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logging.error(f"Failed to save daily analytics: {e}")


async def tops_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export top posts data (admin only)"""
    user_id = update.effective_user.id
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        return  # Silently ignore non-admin requests

    if not top_posts:
        await update.message.reply_text("📭 No top posts to export.")
        return

    try:
        # Create export data
        export_data = {
            "top_posts": [post.to_dict() for post in top_posts],
            "exported_at": datetime.now().isoformat(),
            "total_posts": len(top_posts)
        }

        # Save to temporary file
        filename = f"top_posts_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2)

        # Send file
        with open(filename, 'rb') as f:
            await context.bot.send_document(
                chat_id=user_id,
                document=f,
                filename=filename,
                caption=f"📤 Top Posts Export\n\n"
                f"📊 Total posts: {len(top_posts)}\n"
                f"📅 Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Clean up
        os.remove(filename)

    except Exception as e:
        logging.error(f"Error exporting top posts: {e}")
        await update.message.reply_text("❌ Error exporting top posts.")


async def receive_tops_command(update: Update,
                               context: ContextTypes.DEFAULT_TYPE):
    """Import top posts from file (admin only)"""
    user_id = update.effective_user.id
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        return  # Silently ignore non-admin requests

    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text(
            "⚠️ Please reply to a JSON file to import top posts.")
        return

    try:
        # Download file
        file = await context.bot.get_file(
            update.message.reply_to_message.document.file_id)
        file_path = f"temp_tops_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        await file.download_to_drive(file_path)

        # Load and validate data
        with open(file_path, 'r') as f:
            import_data = json.load(f)

        if "top_posts" not in import_data:
            await update.message.reply_text(
                "❌ Invalid file format. Expected top posts data.")
            return

        # Import top posts
        global top_posts
        top_posts = [
            ScheduledMessage.from_dict(post_data)
            for post_data in import_data["top_posts"]
        ]
        save_top_posts()

        # Clean up
        os.remove(file_path)

        await update.message.reply_text(
            f"✅ Successfully imported {len(top_posts)} top posts!")

    except Exception as e:
        logging.error(f"Error importing top posts: {e}")
        await update.message.reply_text("❌ Error importing top posts.")


async def delete_latest_command(update: Update,
                                context: ContextTypes.DEFAULT_TYPE):
    """Delete the last sent item from everywhere (admin only)"""
    user_id = update.effective_user.id
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        return  # Silently ignore non-admin requests

    # Load sent posts tracking
    sent_posts = await load_sent_posts_tracking()

    if not sent_posts:
        await update.message.reply_text("📭 No sent posts to delete.")
        return

    # Get the most recent post
    latest_post = sent_posts[-1]
    deleted_count = 0

    # Delete from channels
    for channel_id in latest_post.get("channels", []):
        for msg_id in latest_post.get("message_ids", []):
            try:
                await context.bot.delete_message(chat_id=channel_id,
                                                 message_id=msg_id)
                deleted_count += 1
            except Exception as e:
                logging.error(
                    f"Failed to delete message {msg_id} from channel {channel_id}: {e}"
                )

    # Delete from subscribers
    for sub_id in latest_post.get("subscribers", []):
        for msg_id in latest_post.get("subscriber_message_ids",
                                      {}).get(str(sub_id), []):
            try:
                await context.bot.delete_message(chat_id=sub_id,
                                                 message_id=msg_id)
                deleted_count += 1
            except Exception as e:
                logging.error(
                    f"Failed to delete message {msg_id} from subscriber {sub_id}: {e}"
                )

    # Remove from tracking
    sent_posts.pop()
    await save_sent_posts_tracking(sent_posts)

    await update.message.reply_text(
        f"✅ Deleted latest post from {deleted_count} locations.")


async def load_sent_posts_tracking():
    """Load sent posts tracking data"""
    try:
        if os.path.exists(SENT_POSTS_TRACKING_FILE):
            async with aiofiles.open(SENT_POSTS_TRACKING_FILE, 'r') as f:
                content = await f.read()
                if content.strip():
                    data = json.loads(content)
                    return data.get("sent_posts", [])
        return []
    except Exception as e:
        logging.error(f"Failed to load sent posts tracking: {e}")
        return []


async def save_sent_posts_tracking(sent_posts):
    """Save sent posts tracking data"""
    try:
        data = {"sent_posts": sent_posts}
        async with aiofiles.open(SENT_POSTS_TRACKING_FILE, 'w') as f:
            await f.write(json.dumps(data, indent=2))
    except Exception as e:
        logging.error(f"Failed to save sent posts tracking: {e}")


async def handle_txt_import(update: Update,
                            context: ContextTypes.DEFAULT_TYPE):
    """Handle .txt file import for subscribers (admin only)"""
    user_id = update.effective_user.id
    await auto_add_subscriber(user_id)

    if not is_admin(user_id):
        return

    if not update.message.document or not update.message.document.file_name.endswith(
            '.txt'):
        return

    try:
        # Download file
        file = await context.bot.get_file(update.message.document.file_id)
        file_path = f"temp_subs_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        await file.download_to_drive(file_path)

        # Read subscriber IDs
        imported_ids = set()
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.isdigit():
                    imported_ids.add(int(line))

        # Merge with existing subscribers
        old_count = len(subscribers)
        subscribers.update(imported_ids)
        new_count = len(subscribers)
        added_count = new_count - old_count

        save_subscribers()

        # Clean up
        os.remove(file_path)

        await update.message.reply_text(
            f"✅ Subscriber import completed!\n\n"
            f"📤 Imported: {len(imported_ids)} IDs\n"
            f"📊 Added new: {added_count} subscribers\n"
            f"👥 Total subscribers: {new_count}")

    except Exception as e:
        logging.error(f"Error importing subscribers: {e}")
        await update.message.reply_text(
            "❌ Error importing subscribers from file.")


async def fallback_message_handler(update: Update,
                                   context: ContextTypes.DEFAULT_TYPE):
    """Handle any unrecognized messages by showing the main menu"""
    user_id = update.effective_user.id

    # Auto-add subscriber on any interaction
    await auto_add_subscriber(user_id)

    # Show main menu for any unrecognized input
    await menu_command(update, context)


def main():
    # Create the Application with JobQueue enabled
    async def post_init(app):
        # Load subscriber data
        await load_subscribers()
        load_top_posts()
        load_scheduled_messages()
        await load_channel_link()
        await load_failed_subscribers()

    application = Application.builder().token(BOT_TOKEN).post_init(
        post_init).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("h", help_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("top", top_command))
    application.add_handler(CommandHandler("rtop", remove_top_post))
    application.add_handler(CommandHandler("subs", subs_command))
    application.add_handler(CommandHandler("s", show_scheduled))
    application.add_handler(CommandHandler("a", auto_start))
    application.add_handler(CommandHandler("x", auto_stop))

    application.add_handler(CommandHandler("p", post_now))
    application.add_handler(CommandHandler("send", send_first_message))
    application.add_handler(CommandHandler("show", show_message_preview))
    application.add_handler(CommandHandler("admin", verify_admin))
    application.add_handler(CommandHandler("t", handle_target))
    application.add_handler(CommandHandler("id", get_chat_id))
    application.add_handler(CommandHandler("sadd", schedule_add))
    application.add_handler(CommandHandler("collect", collect_posts))
    application.add_handler(CommandHandler("r", remove_scheduled))
    application.add_handler(CommandHandler("u", update_channel_link))
    application.add_handler(CommandHandler("utop", update_top_post))
    application.add_handler(CommandHandler("top_r", remove_top_post_by_id))
    application.add_handler(CommandHandler("d_failed", d_failed_command))
    application.add_handler(CommandHandler("su", su_command))
    application.add_handler(
        CommandHandler("delete_latest", delete_latest_command))
    application.add_handler(CommandHandler("tops", tops_command))
    application.add_handler(
        CommandHandler("receive_tops", receive_tops_command))

    # Add callback query handler for inline buttons
    application.add_handler(CallbackQueryHandler(handle_callback_query))

    # Document handler for .txt imports
    application.add_handler(
        MessageHandler(filters.Document.ALL, handle_txt_import))

    # Message handler for multimedia messages
    application.add_handler(
        MessageHandler(filters.PHOTO | filters.VIDEO | filters.TEXT,
                       handle_message))

    # Add fallback message handler (must be last)
    application.add_handler(
        MessageHandler(filters.ALL, fallback_message_handler))

    # Schedule the automatic /send command every odd hour
    current_time = datetime.now(cairo_tz)
    next_odd_hour = (current_time.hour // 2 * 2 + 1) % 24

    if next_odd_hour <= current_time.hour:
        next_send_time = current_time.replace(
            hour=next_odd_hour, minute=0, second=0,
            microsecond=0) + timedelta(days=1)
    else:
        next_send_time = current_time.replace(hour=next_odd_hour,
                                              minute=0,
                                              second=0,
                                              microsecond=0)

    delay = (next_send_time - current_time).total_seconds()

    application.job_queue.run_once(trigger_send_command,
                                   when=delay,
                                   name="auto_send_command")

    # Add periodic save job to ensure data persistence
    async def periodic_save(context):
        """Periodically save subscribers and other data"""
        try:
            save_subscribers()
            save_top_posts()
            save_scheduled_messages()
            logging.info("Periodic save completed")
        except Exception as e:
            logging.error(f"Periodic save failed: {e}")

    # Save every 5 minutes
    application.job_queue.run_repeating(periodic_save,
                                        interval=300,
                                        first=60,
                                        name="periodic_save")

    # Start the bot
    application.run_polling()


if __name__ == "__main__":
    main()
