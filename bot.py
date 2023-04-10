# coding=utf8

import json
import logging
import logging.config
import os

import pika
import yaml
from aiogram import Bot, Dispatcher, executor, types
from dotenv import load_dotenv

from src.static_text import HELLO_TEXT, NON_TARGET_TEXT, WAITING_TEXT, \
    NON_TARGET_CONTENT_TYPES, CLASSES_DICT, NON_LABELS_TEXT

with open("configs/logging.cfg.yml") as config_fin:
    logging.config.dictConfig(yaml.safe_load(config_fin.read()))

load_dotenv()
TOKEN = os.getenv('TOKEN')
bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

rabbit_user = os.getenv("RABBIT_USER")
rabbit_passwd = os.getenv("RABBIT_PASSWORD")

rabbit_url = f'amqp://{rabbit_user}:{rabbit_passwd}@10.14.244.26:5672/master'


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    user_name = message.from_user.first_name
    user_id = message.from_user.id
    text = HELLO_TEXT % user_name
    logging.info(
        f'First start from user_name = {user_name}, user_id = {user_id}')
    await message.reply(text)


@dp.message_handler(content_types=NON_TARGET_CONTENT_TYPES)
async def handle_docs_photo(message):
    user_name = message.from_user.first_name
    text = NON_TARGET_TEXT % user_name
    await message.reply(text)



@dp.message_handler(content_types=['photo'])
async def handle_docs_photo(message):
    chat_id = message.chat.id

    if message.media_group_id is None:
        user_name = message.from_user.first_name
        user_id = message.from_user.id
        message_id = message.message_id
        text = WAITING_TEXT % user_name
        logging.info(f'{user_name, user_id} is knocking to our bot')
        await bot.send_message(chat_id, text)

        photo_name = './input/photo_%s_%s.jpg' % (user_id, message_id)
        await message.photo[-1].download(
            destination_file=photo_name)

        #photo_output, text = model(photo_name)
        message = json.dumps({
            'data': {photo_name}}, ensure_ascii=False)

        channel.basic_publish(
            properties=pika.BasicProperties(correlation_id=str(123),
                                            priority=120,
                                            reply_to="bot_input",
                                            delivery_mode=2),
            body=message,
            exchange='',
            routing_key="text_easyocr_input_dev")

        await bot.send_photo(chat_id, photo_output)
        output_text = []
        for i in text:
            output_text.append(CLASSES_DICT[i])
        output_text = '\n\n'.join(output_text)
        if not output_text:
            output_text = NON_LABELS_TEXT
        await bot.send_message(chat_id, output_text)
        os.remove(photo_name)

    else:
        text = NOT_TARGET_TEXT % user_name
        await message.reply(text)
\

if __name__ == '__main__':
    logging.info('Bot started!')
    executor.start_polling(dp, skip_updates=True)
