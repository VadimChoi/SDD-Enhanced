# -*- coding:utf-8 -*-
import asyncio
import io
import logging
import os
import re
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Callable, List, Optional

from decouple import config
from telethon import TelegramClient, errors, events
from telethon.errors import ChatForwardsRestrictedError
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

# ── Директории ────────────────────────────────────────────────
log_dir        = 'Logs'
media_dir      = 'Media'
photo_dir      = os.path.join(media_dir, 'Photo')
video_dir      = os.path.join(media_dir, 'Video')
voice_dir      = os.path.join(media_dir, 'Voice')
roundvideo_dir = os.path.join(media_dir, 'RoundVideo')   # кружочки

for directory in [log_dir, media_dir, photo_dir, video_dir, voice_dir, roundvideo_dir]:
    os.makedirs(directory, exist_ok=True)

# ── Логирование ───────────────────────────────────────────────
log_filename = os.path.join(log_dir, f"telegram_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

file_handler    = RotatingFileHandler(log_filename, maxBytes=5*1024*1024, backupCount=5)
console_handler = logging.StreamHandler()
_fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
for h in (file_handler, console_handler):
    h.setFormatter(_fmt)
    h.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

telethon_logger = logging.getLogger('telethon')
telethon_logger.setLevel(logging.INFO)
telethon_logger.addHandler(file_handler)
telethon_logger.addHandler(console_handler)

# ── Конфиг ────────────────────────────────────────────────────
api_id   = config('API_ID')
api_hash = config('API_HASH')
my_id    = int(config('MY_ID'))

session_file = 'vadimchoi'
client = TelegramClient(session_file, api_id, api_hash)

# ── Очередь и блокировки ──────────────────────────────────────
_media_queue   = asyncio.Queue()   # авто-загрузки — строго по одной
_download_lock = asyncio.Lock()    # один /download за раз
_forward_lock  = asyncio.Lock()    # один /forward за раз


# ════════════════════════════════════════════════════════════════
# Общие хелперы
# ════════════════════════════════════════════════════════════════

def _ts() -> str:
    """Временна́я метка для имён файлов."""
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


async def get_sender_info(event) -> str:
    sender = await event.get_sender()
    if sender.username:
        return f'@{sender.username}'
    elif sender.phone:
        return f'Phone: {sender.phone}'
    return f'ID: {sender.id}'


def is_self_destructing(message) -> bool:
    """Единая проверка TTL: голосовые, видео и видео-кружочки."""
    if not (message.voice or message.video or message.video_note):
        return False
    ttl = getattr(message.media, 'ttl_seconds', None)
    return bool(ttl and ttl > 0)


def parse_link(text: str):
    """
    Единый парсер ссылок — используется и в /download, и в /forward.
    Возвращает (entity, msg_id) или (None, None).

    Форматы:
      https://t.me/username/123        — публичный канал/группа
      https://t.me/c/1234567890/123    — приватный канал/группа
      @username 123                    — короткая форма
    """
    text = text.strip()
    m = re.match(r'https?://t\.me/c/(\d+)/(\d+)', text)
    if m:
        return int('-100' + m.group(1)), int(m.group(2))
    m = re.match(r'https?://t\.me/([A-Za-z0-9_]+)/(\d+)', text)
    if m:
        return m.group(1), int(m.group(2))
    m = re.match(r'@?([A-Za-z0-9_]+)\s+(\d+)$', text)
    if m:
        return m.group(1), int(m.group(2))
    return None, None


def _make_progress_cb(status_msg, label: str = 'Скачиваю') -> Callable:
    """
    Возвращает async-коллбэк прогресса для download_media.
    Редактирует status_msg не чаще раза в 3 секунды.
    """
    last_time = [0.0]
    last_pct  = [-1]

    async def _cb(current: int, total: int) -> None:
        if not total:
            return
        now = time.monotonic()
        pct = min(int(current / total * 100), 100)
        if pct == last_pct[0] or (now - last_time[0]) < 3:
            return
        last_pct[0]  = pct
        last_time[0] = now
        bar = '█' * (pct // 10) + '░' * (10 - pct // 10)
        try:
            await status_msg.edit(f'⏳ {label}… {bar} {pct}%')
        except Exception:
            pass  # FloodWait и прочее — молча пропускаем

    return _cb


def _get_media_filename(msg) -> str:
    """Определяет имя файла с расширением для BytesIO."""
    if isinstance(msg.media, MessageMediaPhoto):
        return 'photo.jpg'
    if isinstance(msg.media, MessageMediaDocument):
        doc = msg.media.document
        for attr in doc.attributes:
            if hasattr(attr, 'file_name') and attr.file_name:
                return attr.file_name
        mime = getattr(doc, 'mime_type', 'application/octet-stream')
        ext  = mime.split('/')[-1].replace('jpeg', 'jpg')
        return f'file.{ext}'
    return 'file'


async def _download_to_buf(msg, progress_cb=None) -> Optional[io.BytesIO]:
    """Скачивает медиа в оперативную память (используется в /forward)."""
    if not msg.media:
        return None
    buf = io.BytesIO()
    await client.download_media(msg, file=buf, progress_callback=progress_cb)
    buf.seek(0)
    buf.name = _get_media_filename(msg)
    return buf


async def _fetch_album(channel, message) -> List:
    """Собирает все сообщения альбома по grouped_id."""
    if not message.grouped_id:
        return [message]
    nearby = await client.get_messages(
        channel,
        min_id=message.id - 20,
        max_id=message.id + 20,
        limit=40,
    )
    album = [m for m in nearby if m.grouped_id == message.grouped_id]
    album.sort(key=lambda m: m.id)
    return album or [message]


async def _send_as_copy(target, message, channel, status_msg=None) -> None:
    """
    Скачивает медиа в память и отправляет копией — обходит noforwards.
    status_msg: если передан, показывает прогресс скачивания.
    """
    album   = await _fetch_album(channel, message)
    caption = next((m.text for m in album if m.text), '') or ''

    if len(album) > 1:
        buffers = []
        for i, msg in enumerate(album):
            cb  = _make_progress_cb(status_msg, f'Скачиваю {i+1}/{len(album)}') if status_msg else None
            buf = await _download_to_buf(msg, cb)
            if buf:
                buffers.append(buf)
        if buffers:
            await client.send_file(target, file=buffers, caption=caption)
        elif caption:
            await client.send_message(target, caption)

    elif message.media:
        cb  = _make_progress_cb(status_msg) if status_msg else None
        buf = await _download_to_buf(message, cb)
        if buf:
            await client.send_file(target, file=buf, caption=caption)
        elif caption:
            await client.send_message(target, caption)

    else:
        if caption:
            await client.send_message(target, caption)


# ════════════════════════════════════════════════════════════════
# Авто-загрузчик самоуничтожающихся медиа
# ════════════════════════════════════════════════════════════════

async def _process_auto_media(event) -> None:
    """Обрабатывает одно входящее медиа (запускается воркером очереди)."""
    try:
        sender_info = await get_sender_info(event)

        if event.sender_id == my_id:
            logger.info(f'Пропускаем медиа от себя ({sender_info}).')
            return

        if event.photo:
            folder = photo_dir

        elif event.voice:
            if not is_self_destructing(event.message):
                logger.info('Голосовое не самоуничтожающееся, пропускаем.')
                return
            folder = voice_dir

        elif event.video:
            if not is_self_destructing(event.message):
                logger.info('Видео не самоуничтожающееся, пропускаем.')
                return
            folder = video_dir

        elif event.video_note:
            if not is_self_destructing(event.message):
                logger.info('Видео-кружочек не самоуничтожающийся, пропускаем.')
                return
            folder = roundvideo_dir

        else:
            logger.error('Неизвестный тип медиафайла, пропускаем.')
            return

        file_path = await event.download_media(file=os.path.join(folder, _ts()))
        logger.info(f'Сохранён: {file_path} (от {sender_info})')
        await client.send_file('me', file_path, caption='Скачано @VadimChoi')
        logger.info('Отправлено в Избранное.')

    except Exception as e:
        logger.error(f'Ошибка авто-загрузки: {e}')


async def _media_worker() -> None:
    """Воркер: разбирает очередь авто-загрузок строго по одному событию."""
    logger.info('Воркер медиа-очереди запущен.')
    while True:
        event = await _media_queue.get()
        try:
            await _process_auto_media(event)
        except Exception as e:
            logger.error(f'Ошибка воркера: {e}')
        finally:
            _media_queue.task_done()


@client.on(events.NewMessage(
    func=lambda e: e.is_private
                   and (e.photo or e.video or e.voice or e.video_note)
                   and e.media_unread
))
async def downloader(event) -> None:
    """Ставит входящее медиа в очередь обработки."""
    if _media_queue.qsize() >= 20:
        logger.warning(f'Очередь переполнена ({_media_queue.qsize()}), пропускаем.')
        return
    await _media_queue.put(event)


# ════════════════════════════════════════════════════════════════
# /download <ссылка>
# ════════════════════════════════════════════════════════════════

@client.on(events.NewMessage(func=lambda e: e.is_private))
async def download_by_link(event) -> None:
    try:
        if event.chat_id != my_id:
            return

        parts = event.message.message.split()
        if len(parts) < 2 or parts[0] != '/download':
            return

        if _download_lock.locked():
            await event.respond('⏳ Уже выполняется загрузка, подожди.')
            return

        async with _download_lock:
            entity_raw, message_id = parse_link(parts[1])

            if entity_raw is None:
                await event.respond(
                    '❌ Не удалось разобрать ссылку.\n'
                    'Формат: /download https://t.me/channel/123'
                )
                return

            status = await event.respond('⏳ Получаю сообщение…')

            try:
                chat    = await client.get_entity(entity_raw)
                message = await client.get_messages(chat, ids=message_id)

                if not message:
                    await status.edit('❌ Сообщение не найдено.')
                    return

                if not (message.photo or message.video or message.voice or message.video_note):
                    await status.edit('❌ В сообщении нет медиа (фото/видео/голосовое/кружочек).')
                    return

                if message.sender_id == my_id:
                    await status.edit('❌ Медиафайлы от вашего аккаунта игнорируются.')
                    return

                if message.photo:
                    folder = photo_dir
                elif message.video:
                    folder = video_dir
                elif message.voice:
                    folder = voice_dir
                elif message.video_note:
                    folder = roundvideo_dir
                else:
                    await status.edit('❌ Неизвестный тип медиафайла.')
                    return

                file_path = await message.download_media(
                    file=os.path.join(folder, _ts()),
                    progress_callback=_make_progress_cb(status),
                )
                await client.send_file('me', file_path, caption='Скачано @VadimChoi')
                await status.edit('✅ Скачано и отправлено в Избранное.')
                logger.info(f'Скачано по ссылке: {file_path}')

            except Exception as e:
                await status.edit(f'❌ Ошибка: {e}')
                logger.error(f'Ошибка /download: {e}')

    except Exception as e:
        logger.error(f'Необработанная ошибка /download: {e}')


# ════════════════════════════════════════════════════════════════
# /forward <ссылка>
# ════════════════════════════════════════════════════════════════

@client.on(events.NewMessage(outgoing=True, pattern=r'^/forward(?:\s+(.+))?$'))
async def forward_by_link(event) -> None:
    try:
        if event.chat_id != my_id:
            return

        arg = (event.pattern_match.group(1) or '').strip()
        if not arg:
            await event.respond(
                'Использование: /forward <ссылка>\n\n'
                'Примеры:\n'
                '/forward https://t.me/channel/123\n'
                '/forward https://t.me/c/1234567890/123\n'
                '/forward @channel 123'
            )
            return

        if _forward_lock.locked():
            await event.respond('⏳ Уже выполняется пересылка, подожди.')
            return

        async with _forward_lock:
            entity_raw, msg_id = parse_link(arg)
            if entity_raw is None:
                await event.respond('❌ Не удалось разобрать ссылку. Проверь формат.')
                return

            status = await event.respond('⏳ Получаю пост…')

            try:
                channel = await client.get_entity(entity_raw)
                message = await client.get_messages(channel, ids=msg_id)

                if message is None:
                    await status.edit('❌ Сообщение не найдено.')
                    return

                try:
                    await client.forward_messages(
                        entity=my_id,
                        messages=message,
                        from_peer=channel,
                    )
                    await status.edit('✅ Переслано!')
                    logger.info(f'Переслан пост {entity_raw}/{msg_id}')

                except ChatForwardsRestrictedError:
                    await status.edit('⏳ Канал с запретом, копирую медиа…')
                    await _send_as_copy(my_id, message, channel, status_msg=status)
                    await status.edit('✅ Скопировано (запрет обойдён).')
                    logger.info(f'Скопирован пост {entity_raw}/{msg_id} (noforwards обход)')

            except Exception as e:
                await status.edit(f'❌ Ошибка: {e}')
                logger.error(f'Ошибка /forward: {e}')

    except Exception as e:
        logger.error(f'Необработанная ошибка /forward: {e}')


# ════════════════════════════════════════════════════════════════
# Запуск
# ════════════════════════════════════════════════════════════════

async def main() -> None:
    try:
        logger.info('Запуск клиента…')
        await client.start()
        asyncio.create_task(_media_worker())
        logger.info('Клиент запущен. Воркер медиа-очереди активен.')
        await client.run_until_disconnected()
    except errors.SessionRevokedError:
        logger.error('Сессия отозвана, авторизуйтесь заново.')
    except errors.FloodWaitError as e:
        logger.error(f'FloodWait: {e}')
    except errors.PhoneCodeInvalidError:
        logger.error('Неверный код подтверждения.')
    except errors.PhoneNumberOccupiedError:
        logger.error('Номер телефона уже используется.')
    except errors.RPCError as e:
        logger.error(f'RPC ошибка: {e}')
    except Exception as e:
        logger.error(f'Непредвиденная ошибка: {e}')
    finally:
        await client.disconnect()
        logger.info('Клиент отключён.')


if __name__ == '__main__':
    asyncio.run(main())
