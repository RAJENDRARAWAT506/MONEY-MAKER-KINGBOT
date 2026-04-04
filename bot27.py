import os
import asyncio
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ChatJoinRequestHandler,
    ContextTypes
)

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SOURCE_CHAT_ID = int(os.getenv("SOURCE_CHAT_ID"))

# Only required message IDs
MSG_ID_1 = int(os.getenv("MSG_ID_1"))          # Text
VIDEO_MSG_ID_1 = int(os.getenv("VIDEO_MSG_ID_1"))  # Video
APK_MSG_ID = int(os.getenv("APK_MSG_ID"))      # APK
VOICE_MSG_ID = int(os.getenv("VOICE_MSG_ID"))  # Audio


# Function to send messages in order
async def send_full_message(user_id, context):
    for msg_id in [
        MSG_ID_1,
        VIDEO_MSG_ID_1,
        APK_MSG_ID,
        VOICE_MSG_ID
    ]:
        try:
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=SOURCE_CHAT_ID,
                message_id=msg_id
            )
            await asyncio.sleep(2)  # delay between messages

        except Exception as e:
            print(f"Error sending {msg_id}: {e}")


# Handle join request
async def handle_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_join_request.from_user
    chat_id = update.chat_join_request.chat.id

    try:
        # Approve user
        await context.bot.approve_chat_join_request(
            chat_id=chat_id,
            user_id=user.id
        )

        # Send messages
        await send_full_message(user.id, context)

    except Exception as e:
        print(f"Join error: {e}")


# Start bot
app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(ChatJoinRequestHandler(handle_join))

print("🔥 BOT RUNNING CORRECTLY...")
app.run_polling()