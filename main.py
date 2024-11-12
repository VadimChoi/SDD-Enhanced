# -*- coding:utf-8 -*-
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from decouple import config
from telethon import TelegramClient, events, errors
from datetime import datetime
import os

# Создание директорий для логов и медиа, если они не существуют
log_dir = 'Logs'
media_dir = 'Media'
photo_dir = os.path.join(media_dir, 'Photo')
video_dir = os.path.join(media_dir, 'Video')
voice_dir = os.path.join(media_dir, 'Voice')

for directory in [log_dir, media_dir, photo_dir, video_dir, voice_dir]:
    os.makedirs(directory, exist_ok=True)

# Настройка логирования
log_filename = os.path.join(log_dir, f"telegram_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

# Создание обработчика для записи в файл с уровнем INFO
file_handler = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=5)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.INFO)

# Создание обработчика для вывода в консоль с уровнем INFO
console_handler = logging.StreamHandler()
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
console_handler.setLevel(logging.INFO)

# Настройка общего логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Настройка логирования для Telethon
telethon_logger = logging.getLogger("telethon")
telethon_logger.setLevel(logging.INFO)
telethon_logger.addHandler(file_handler)
telethon_logger.addHandler(console_handler)

# Чтение api_id, api_hash и MY_ID из файла конфигурации
api_id = config('API_ID')
api_hash = config('API_HASH')
my_id = int(config('MY_ID'))

# Имя файла сессии
session_file = 'vadimchoi'
client = TelegramClient(session_file, api_id, api_hash)

# Получение информации об отправителе сообщения
async def get_sender_info(event):
    """Получение информации об отправителе сообщения с приоритетом: username, номер телефона, ID."""
    sender = await event.get_sender()
    if sender.username:
        return f"@{sender.username}"
    elif sender.phone:
        return f"Phone: {sender.phone}"
    else:
        return f"ID: {sender.id}"

# Проверка, является ли голосовое сообщение самоуничтожающимся
def is_self_destructing_voice(message):
    """Проверка, является ли голосовое сообщение самоуничтожающимся."""
    if message.voice:
        ttl_seconds = getattr(message.media, 'ttl_seconds', None)
        if ttl_seconds and ttl_seconds > 0:
            return True
    return False

# Проверка, является ли видео сообщение самоуничтожающимся
def is_self_destructing_video(message):
    """Проверка, является ли видео сообщение самоуничтожающимся."""
    if message.video:
        ttl_seconds = getattr(message.media, 'ttl_seconds', None)
        if ttl_seconds and ttl_seconds > 0:
            return True
    return False

# Обработчик для новых медиафайлов
@client.on(events.NewMessage(func=lambda e: e.is_private and (e.photo or e.video or e.voice) and e.media_unread))
async def downloader(event):
    try:
        sender_info = await get_sender_info(event)

        # Проверка, является ли отправитель сообщения тобой
        if event.sender_id == my_id:
            logger.info(f"Пропускаем медиа от себя (отправитель: {sender_info}).")
            return

        # Если это голосовое сообщение, проверяем наличие таймера самоуничтожения
        if event.voice:
            if is_self_destructing_voice(event):
                folder = voice_dir
            else:
                logger.info("Голосовое сообщение не является самоуничтожающимся, пропускаем его.")
                return
        elif event.photo:
            folder = photo_dir
        elif event.video:
            if is_self_destructing_video(event):
                folder = video_dir
            else:
                logger.info("Видео сообщение не является самоуничтожающимся, пропускаем его.")
                return
        else:
            logger.error("Не удалось определить тип медиафайла, пропускаем загрузку.")
            return

        # Скачивание медиафайла в соответствующую папку
        file_path = await event.download_media(file=os.path.join(folder, datetime.now().strftime('%Y-%m-%d_%H-%M-%S')))
        logger.info(f"Медиафайл сохранён: {file_path}")
        logger.info("Медиа загружено, отправка себе...")
        await client.send_file("me", file_path, caption="Скачано @VadimChoi")
        logger.info("Медиа успешно отправлено.")
    except Exception as e:
        logger.error(f"Ошибка в загрузчике: {e}")

# Обработчик команд для загрузки медиафайлов по ссылке в личных сообщениях
@client.on(events.NewMessage(func=lambda e: e.is_private))
async def download_by_link(event):
    """Обработчик команд для загрузки медиафайлов по ссылке в личных сообщениях."""
    try:
        if event.chat_id != my_id:
            return  # Игнорируем команды, отправленные не в личном чате

        parts = event.message.message.split()
        if len(parts) < 2:
            await event.respond("Пожалуйста, используйте формат команды: /download <message_link>")
            return

        message_link = parts[1]

        if 't.me/c/' in message_link or 't.me/' in message_link:
            try:
                # Разбираем ссылку
                parts = message_link.split('/')
                chat_id = int(parts[-2]) if 't.me/c/' in message_link else parts[3]
                message_id = int(parts[-1])

                chat = await client.get_entity(chat_id)
                message = await client.get_messages(chat, ids=message_id)

                if message and (message.photo or message.video or message.voice):
                    # Проверка, является ли отправитель сообщения вами
                    if message.sender_id == my_id:
                        await event.respond("Медиафайлы от вашего аккаунта игнорируются.")
                        return

                    # Определение типа медиа и выбор папки для сохранения
                    if message.photo:
                        folder = photo_dir
                    elif message.video:
                        folder = video_dir
                    elif message.voice:
                        folder = voice_dir
                    else:
                        await event.respond("Не удалось определить тип медиафайла, загрузка пропущена.")
                        logger.error("Не удалось определить тип медиафайла, пропускаем загрузку.")
                        return

                    file_path = await message.download_media(file=os.path.join(folder, datetime.now().strftime('%Y-%m-%d_%H-%M-%S')))
                    await client.send_file("me", file_path, caption="Скачано @VadimChoi")
                    await event.respond("Медиа успешно загружено и отправлено.")
                else:
                    await event.respond("Медиа не найдено в указанном сообщении или не является фото/видео/голосовым сообщением.")
            except Exception as e:
                error_message = f"Ошибка при обработке ссылки: {e}"
                await event.respond(error_message)
                logger.error(error_message)
    except Exception as e:
        logger.error(f"Ошибка при скачивании по ссылке: {e}")

async def main():
    try:
        logger.info("Запуск клиента...")
        await client.start()
        logger.info("Клиент запущен")
        await client.run_until_disconnected()
    except errors.SessionRevokedError as e:
        logger.error("Сессия была отозвана, пожалуйста, авторизуйтесь заново.")
    except errors.FloodWaitError as e:
        logger.error(f"Ошибка ожидания: {e}")
    except errors.PhoneCodeInvalidError as e: 
        logger.error("Введен неверный код подтверждения. Пожалуйста, проверьте и попробуйте снова.")
    except errors.PhoneNumberOccupiedError as e:
        logger.error("Этот номер телефона уже используется. Пожалуйста, используйте другой номер.")
    except errors.RPCError as e:
        logger.error(f"RPC ошибка: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {e}")
    finally:
        await client.disconnect()
        logger.info("Клиент отключен")

if __name__ == '__main__':
    asyncio.run(main())
