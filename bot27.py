import os
import asyncio
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import ApplicationBuilder, ChatJoinRequestHandler, ContextTypes

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SOURCE_CHAT_ID = int(os.getenv("SOURCE_CHAT_ID"))

MSG_IDS = [
    int(os.getenv("MSG_ID_1")),
    int(os.getenv("VIDEO_MSG_ID_1")),
    int(os.getenv("APK_MSG_ID")),
    int(os.getenv("VOICE_MSG_ID"))
]


# Send all saved messages
async def send_full_message(user_id, context):
    for msg_id in MSG_IDS:
        try:
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=SOURCE_CHAT_ID,
                message_id=msg_id
            )
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error sending {msg_id}: {e}")


# Handle join request → auto send
async def handle_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.chat_join_request.from_user

    try:
        # Send messages directly (no /start needed)
        await context.bot.send_message(
            chat_id=user.id,
            text="🔥 Sending your content..."
        )

        await send_full_message(user.id, context)

    except Exception as e:
        print(f"Join error: {e}")


# App setup
app = ApplicationBuilder().token(BOT_TOKEN).build()

app.add_handler(ChatJoinRequestHandler(handle_join))

print("🔥 BOT RUNNING (AUTO SEND ON JOIN)...")
app.run_polling()
