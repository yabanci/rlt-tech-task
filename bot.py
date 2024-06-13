import logging
import json
import asyncio
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aggregator import aggregate_payments

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def send_welcome(message: types.Message):
    await message.reply(
        "Send a JSON with 'dt_from', 'dt_upto', and 'group_type' to get aggregated data."
    )


@dp.message()
async def handle_message(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data["dt_from"]
        dt_upto = data["dt_upto"]
        group_type = data["group_type"]

        result = aggregate_payments(dt_from, dt_upto, group_type)
        await message.reply(json.dumps(result))
    except Exception as e:
        await message.reply(f"Error: {e}")


def start_bot():
    asyncio.run(dp.run_polling(bot))
