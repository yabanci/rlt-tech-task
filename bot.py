import logging
import json
from os import getenv
from aiogram import Bot, Dispatcher, types, executor
from aggregator import aggregate_payments

API_TOKEN = getenv("API_TOKEN")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.reply("Send a JSON with 'dt_from', 'dt_upto', and 'group_type' to get aggregated data.")

@dp.message_handler()
async def handle_message(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']
        
        result = aggregate_payments(dt_from, dt_upto, group_type)
        await message.reply(json.dumps(result))
    except Exception as e:
        await message.reply(f"Error: {e}")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
