"""
TG Parser v3 — один аккаунт (или два), скоринг + модерация через inline-кнопки.

Ключевые решения:
- Пересылка в dest-канал идёт через Telethon-аккаунт (не Bot API),
  поэтому бот НЕ обязан состоять в каналах-источниках.
- Бот используется только для: карточек модерации (sendMessage + inline-кнопки)
  и приёма callback_query от кнопок ✅/❌.
- Весь основной поток (main + хендлер) — в одном месте.
"""

import asyncio
import base64
import json
import logging
import os
import re
import time
import urllib.request
import urllib.parse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from telethon import TelegramClient, events
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.sessions import StringSession

# ── Конфиг из переменных окружения ────────────────────────────────────────────

API_ID_1   = int(os.environ.get('TG_API_ID_1', '0'))
API_HASH_1 = os.environ.get('TG_API_HASH_1', '')
SESSION_1  = os.environ.get('TG_SESSION_1', '')

API_ID_2   = int(os.environ.get('TG_API_ID_2', '0'))
API_HASH_2 = os.environ.get('TG_API_HASH_2', '')
SESSION_2  = os.environ.get('TG_SESSION_2', '')

SPREADSHEET_ID         = os.environ.get('SPREADSHEET_ID', '')
GOOGLE_CREDENTIALS_B64 = os.environ.get('GOOGLE_CREDENTIALS_BASE64', '')
SETTINGS_RELOAD_SEC    = int(os.environ.get('SETTINGS_RELOAD_INTERVAL', '300'))

# ── Логирование ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ── Глобальное состояние ───────────────────────────────────────────────────────

state = {
    'tg_token':          '',
    'score_threshold':   7,
    'min_length':        20,
    'moderator_chat_id': '',
    'dest_chat_id':      '',
    'scoring_rules':     [],   # [{'category': str, 'weight': int, 'keywords': [str]}]
    'minus_words':       [],   # [str]
    'watched_ids':       set(),
    'id_to_meta':        {},   # {id_variant: {chat_name, username, entity_id}}
    'username_to_meta':  {},
}

# Часовой пояс для записи в таблицу (GMT+3)
TZ_OFFSET_HOURS = 3

# Дедупликация входящих сообщений (chat_id, msg_id)
seen_ids: deque = deque(maxlen=2000)

# Дедупликация опубликованных постов по тексту+автору
# Храним (fingerprint) последних 500 опубликованных, чтобы не дублировать
# одно объявление из нескольких групп.
published_fingerprints: deque = deque(maxlen=500)

# pending_moderation: '{src_chat_id}:{src_msg_id}' → post dict
pending_moderation: dict = {}

_executor = ThreadPoolExecutor(max_workers=4)


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets — подключение
# ══════════════════════════════════════════════════════════════════════════════

def _get_spreadsheet():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_B64).decode('utf-8'))
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets — чтение
# ══════════════════════════════════════════════════════════════════════════════

def _read_settings(ss):
    try:
        data = ss.worksheet('Настройки').get_all_values()
        def val(row_idx):
            return str(data[row_idx][1]).strip() if len(data) > row_idx and len(data[row_idx]) > 1 else ''
        return {
            'tg_token':          val(1),
            'score_threshold':   int(val(2) or 7),
            'min_length':        int(val(3) or 20),
            'moderator_chat_id': val(4),
            'dest_chat_id':      val(5),
        }
    except Exception as e:
        log.error('Ошибка чтения настроек: ' + str(e))
        return None


def _read_scoring_rules(ss):
    try:
        data = ss.worksheet('Скоринг').get_all_values()
        rules = []
        for row in data[1:]:
            if not row or not row[0].strip():
                continue
            try:
                weight = int(str(row[1]).strip())
            except (ValueError, IndexError):
                continue
            keywords_raw = row[2] if len(row) > 2 else ''
            keywords = [k.strip().lower() for k in keywords_raw.split(',') if k.strip()]
            if keywords:
                rules.append({'category': row[0].strip(), 'weight': weight, 'keywords': keywords})
        return rules
    except Exception as e:
        log.error('Ошибка чтения скоринга: ' + str(e))
        return []


def _read_minus_words(ss):
    try:
        data = ss.worksheet('Минус-слова').get_all_values()
        return [str(row[0]).strip().lower() for row in data[1:] if row and row[0].strip()]
    except Exception as e:
        log.error('Ошибка чтения минус-слов: ' + str(e))
        return []


def _read_channels(ss):
    try:
        data = ss.worksheet('Каналы').get_all_values()
        result = []
        for row in data[1:]:
            if not row or not row[0].strip():
                continue
            status = str(row[2]).strip() if len(row) > 2 else 'активен'
            if status == 'пауза':
                continue
            username = _extract_username(row[0].strip())
            if username:
                result.append({'username': username})
        return result
    except Exception as e:
        log.error('Ошибка чтения каналов: ' + str(e))
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets — запись
# ══════════════════════════════════════════════════════════════════════════════

def _local_now() -> datetime:
    """Текущее время GMT+3 (без зависимости от pytz/zoneinfo)."""
    from datetime import timedelta
    return datetime.utcnow() + timedelta(hours=TZ_OFFSET_HOURS)


def _local_dt(dt: datetime) -> datetime:
    """Конвертирует UTC datetime в GMT+3."""
    from datetime import timedelta
    return dt + timedelta(hours=TZ_OFFSET_HOURS)


def _write_post(ss, post):
    try:
        ss.worksheet('Посты').append_row([
            _local_dt(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['author_name'],
            post['author_link'],
            post['link'],
            post['text'],
            post['score'],
            post['account'],
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error('Ошибка записи поста: ' + str(e))


def _write_rejected(ss, post, bot_message_id):
    try:
        ss.worksheet('Отклонённые').append_row([
            _local_dt(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['link'],
            post['text'],
            post['score'],
            'ожидает',
            str(bot_message_id),
            post['account'],
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error('Ошибка записи отклонённого поста: ' + str(e))


def _write_log(ss, level, message, account=''):
    try:
        safe = str(message)
        if safe and safe[0] in '=+-@':
            safe = "'" + safe
        ss.worksheet('Логи').append_row(
            [_local_now().strftime('%Y-%m-%d %H:%M:%S'), level, safe, str(account)],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error('Ошибка записи лога: ' + str(e))


def _set_channel_status(ss, username: str, status: str):
    try:
        ws = ss.worksheet('Каналы')
        data = ws.get_all_values()
        for i, row in enumerate(data[1:], start=2):
            if not row:
                continue
            u = _extract_username(row[0].strip())
            if u and u.lower() == username.lower():
                ws.update(values=[[status]], range_name=f'C{i}')
                return
    except Exception as e:
        log.error(f'Ошибка обновления статуса канала {username}: {e}')


def _update_rejected_status(ss, bot_message_id: int, new_status: str):
    """Обновляет статус в листе «Отклонённые» по bot_message_id."""
    try:
        ws = ss.worksheet('Отклонённые')
        data = ws.get_all_values()
        for i, row in enumerate(data[1:], start=2):
            if len(row) > 6 and str(row[6]) == str(bot_message_id):
                ws.update(values=[[new_status]], range_name=f'F{i}')
                return
    except Exception as e:
        log.error(f'Ошибка обновления статуса отклонённого поста: {e}')


# ══════════════════════════════════════════════════════════════════════════════
# Утилиты
# ══════════════════════════════════════════════════════════════════════════════

def _extract_username(raw: str):
    if not raw:
        return None
    m = re.match(r'(?:https?://)?t\.me/([a-zA-Z0-9_]+)', raw)
    if m:
        return m.group(1)
    if raw.startswith('@'):
        return raw[1:]
    if re.match(r'^[a-zA-Z0-9_]+$', raw):
        return raw
    if re.match(r'^-?\d+$', raw):
        return raw
    return None


def _all_id_variants(entity_id: int) -> list:
    eid = abs(entity_id)
    variants = [eid]
    s = str(eid)
    if not s.startswith('100'):
        variants.append(int('100' + s))
    else:
        short = int(s[3:])
        if short > 0:
            variants.append(short)
    return variants


def _build_link(chat, msg_id: int) -> str:
    username = getattr(chat, 'username', None)
    if username:
        return f'https://t.me/{username}/{msg_id}'
    chat_id = str(chat.id)
    if chat_id.startswith('-100'):
        chat_id = chat_id[4:]
    elif chat_id.startswith('-'):
        chat_id = chat_id[1:]
    return f'https://t.me/c/{chat_id}/{msg_id}'


def _get_author_info(msg):
    try:
        sender = msg.sender
        if not sender:
            return '', ''
        first    = getattr(sender, 'first_name', '') or ''
        last     = getattr(sender, 'last_name',  '') or ''
        username = getattr(sender, 'username',   '') or ''
        name = (first + ' ' + last).strip()
        link = f'https://t.me/{username}' if username else ''
        return name, link
    except Exception:
        return '', ''


# ══════════════════════════════════════════════════════════════════════════════
# Фильтрация и скоринг
# ══════════════════════════════════════════════════════════════════════════════

def _has_minus_word(text: str, minus_words: list) -> bool:
    lower = text.lower()
    return any(w in lower for w in minus_words if w)


def _calc_score(text: str, rules: list) -> int:
    lower = text.lower()
    total = 0
    for rule in rules:
        for kw in rule['keywords']:
            if kw.endswith('*'):
                if kw[:-1] in lower:
                    total += rule['weight']
                    break
            else:
                if kw in lower:
                    total += rule['weight']
                    break
    return total


# ══════════════════════════════════════════════════════════════════════════════
# Telegram Bot API
# ══════════════════════════════════════════════════════════════════════════════

def _tg_request(token: str, method: str, payload: dict, timeout: int = 10) -> dict:
    url  = f'https://api.telegram.org/bot{token}/{method}'
    data = json.dumps(payload).encode('utf-8')
    req  = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result = json.loads(resp.read().decode('utf-8'))
            if not result.get('ok'):
                log.error(f'TG API {method} не ок: {result.get("description")}')
            return result
    except Exception as e:
        log.error(f'TG API {method} error: {e}')
        return {}


def _send_moderation_card(post: dict, token: str, moderator_chat_id: str) -> int:
    """
    Отправляет карточку модерации в чат модератора с inline-кнопками ✅/❌.
    Возвращает message_id бота (для привязки callback).
    """
    lines = [
        f'⚠️ <b>Пост не прошёл скоринг</b> (скор: {post["score"]}/{state["score_threshold"]})',
        f'📢 <b>{post["chat_name"]}</b>',
    ]
    if post['author_name']:
        author = post['author_name']
        if post['author_link']:
            author += f' — {post["author_link"]}'
        lines.append(f'👤 {author}')
    lines += ['', post['text'][:1000], '', f'🔗 {post["link"]}']

    result = _tg_request(token, 'sendMessage', {
        'chat_id':    moderator_chat_id,
        'text':       '\n'.join(lines)[:4096],
        'parse_mode': 'HTML',
        'reply_markup': {
            'inline_keyboard': [[
                # callback_data содержит bot_message_id — он ещё неизвестен здесь,
                # поэтому кладём уникальный ключ на основе chat+msg
                {'text': '✅ Опубликовать', 'callback_data': f'approve:{post["src_chat_id"]}:{post["src_msg_id"]}'},
                {'text': '❌ Пропустить',   'callback_data': f'skip:{post["src_chat_id"]}:{post["src_msg_id"]}'},
            ]]
        },
    })
    return result.get('result', {}).get('message_id', 0)


def _answer_callback(token: str, callback_query_id: str, text: str):
    _tg_request(token, 'answerCallbackQuery', {
        'callback_query_id': callback_query_id,
        'text': text,
        'show_alert': False,
    })


def _edit_message_reply_markup(token: str, chat_id: str, message_id: int, new_text: str):
    """Убирает кнопки после решения модератора и дописывает статус реплаем."""
    _tg_request(token, 'editMessageReplyMarkup', {
        'chat_id':      chat_id,
        'message_id':   message_id,
        'reply_markup': {'inline_keyboard': []},
    })
    _tg_request(token, 'sendMessage', {
        'chat_id':             chat_id,
        'text':                new_text,
        'reply_to_message_id': message_id,
    })


def _send_alert(token: str, moderator_chat_id: str, message: str):
    if not token or not moderator_chat_id:
        return
    _tg_request(token, 'sendMessage', {
        'chat_id': moderator_chat_id,
        'text':    message[:4096],
    })


# ── Fingerprint для дедупликации опубликованных постов ─────────────────────────

def _post_fingerprint(text: str, author_name: str) -> str:
    """
    Нормализованный ключ (текст + автор) для поиска дублей.
    Убираем пунктуацию/пробелы, берём первые 120 символов текста —
    этого достаточно чтобы поймать «чуть изменённые» копии.
    """
    import unicodedata
    norm = unicodedata.normalize('NFKC', text.lower())
    norm = re.sub(r'[\s\W]+', '', norm)[:120]
    author_key = re.sub(r'\s+', '', author_name.lower())
    return f'{author_key}|{norm}'


# ── Форматированная публикация поста в канал ───────────────────────────────────

def _build_post_html(post: dict) -> str:
    """
    Формирует текст поста для отправки в канал от имени канала (без «переслано от»).

    Формат:
        <текст поста>

        <a href="ссылка_на_источник">Источник</a> · <a href="ссылка_на_автора">Имя автора</a>

    Если автор неизвестен — только «Источник».
    Если источник — приватный чат (ссылка t.me/c/...) — подпись «Источник» без имени канала,
    иначе используем название канала как текст ссылки.
    """
    text = post['text'][:4000]  # оставляем место под подпись

    # Ссылка на источник
    link        = post['link']
    chat_name   = post.get('chat_name', '')
    source_text = chat_name if chat_name else 'Источник'
    source_html = f'<a href="{link}">{source_text}</a>'

    # Ссылка на автора
    author_name = post.get('author_name', '').strip()
    author_link = post.get('author_link', '').strip()

    if author_name and author_link:
        author_html = f'<a href="{author_link}">{author_name}</a>'
        footer = f'{source_html} · {author_html}'
    elif author_name:
        footer = f'{source_html} · {author_name}'
    else:
        footer = source_html

    return f'{text}\n\n{footer}'


async def _publish_post(client: TelegramClient, post: dict, dest_chat: str):
    """
    Отправляет пост в канал через Telethon sendMessage — от имени канала,
    без плашки «переслано от». Возвращает True при успехе.
    """
    html = _build_post_html(post)
    try:
        await client.send_message(
            entity=int(dest_chat),
            message=html,
            parse_mode='html',
            link_preview=False,
        )
        return True
    except Exception as e:
        log.error(f'Ошибка публикации поста: {e}')
        return False


def _get_updates(token: str, offset: int, timeout: int = 30) -> list:
    # Сокет-таймаут больше polling-таймаута, чтобы не обрывать соединение раньше ответа
    result = _tg_request(token, 'getUpdates', {
        'offset':  offset,
        'timeout': timeout,
        'allowed_updates': ['callback_query'],
    }, timeout=timeout + 10)
    return result.get('result', [])


# ══════════════════════════════════════════════════════════════════════════════
# Управление каналами
# ══════════════════════════════════════════════════════════════════════════════

async def _resolve_entity(clients: dict, username: str, ss) -> dict | None:
    errors = {}
    for acc_name, client in clients.items():
        try:
            entity    = await client.get_entity(username)
            eid       = abs(entity.id)
            chat_name = getattr(entity, 'title', None) or username
            log.info(f'Резолв [{acc_name}]: {username} → {eid} ({chat_name})')
            return {'entity_id': eid, 'chat_name': chat_name, 'username': username}
        except FloodWaitError as e:
            log.warning(f'[{acc_name}] FloodWait при резолве {username}: жду {e.seconds}s')
            await asyncio.sleep(e.seconds + 2)
            try:
                entity    = await client.get_entity(username)
                eid       = abs(entity.id)
                chat_name = getattr(entity, 'title', None) or username
                return {'entity_id': eid, 'chat_name': chat_name, 'username': username}
            except Exception as e2:
                errors[acc_name] = str(e2)
        except (ChannelPrivateError, UsernameNotOccupiedError, UsernameInvalidError) as e:
            errors[acc_name] = str(e)
            log.warning(f'[{acc_name}] Недоступен {username}: {e}')
        except Exception as e:
            errors[acc_name] = str(e)
            log.error(f'[{acc_name}] Ошибка резолва {username}: {e}')
        await asyncio.sleep(0.5)

    msg = (f'🚫 Канал недоступен: @{username}\n'
           + '\n'.join(f'{k}: {v}' for k, v in errors.items()))
    log.error(msg)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _send_alert,
                               state['tg_token'], state['moderator_chat_id'], msg)
    await loop.run_in_executor(_executor, _set_channel_status, ss, username, 'недоступен')
    await loop.run_in_executor(_executor, _write_log, ss, 'ERROR', msg)
    return None


async def _update_watched_chats(clients: dict, channels: list, ss):
    new_ids     = set()
    new_id_meta = {}

    for ch in channels:
        username = ch['username']
        cached   = state['username_to_meta'].get(username)
        if cached and 'entity_id' in cached:
            eid = cached['entity_id']
            for vid in _all_id_variants(eid):
                new_ids.add(vid)
                new_id_meta[vid] = cached
            continue

        meta = await _resolve_entity(clients, username, ss)
        if meta:
            eid = meta['entity_id']
            for vid in _all_id_variants(eid):
                new_ids.add(vid)
                new_id_meta[vid] = meta
            state['username_to_meta'][username] = meta
        await asyncio.sleep(0.8)

    added   = new_ids - state['watched_ids']
    removed = state['watched_ids'] - new_ids
    state['watched_ids'] = new_ids
    state['id_to_meta']  = new_id_meta

    log.info(f'Каналов в watched_ids: {len(new_ids)} (ключей в id_to_meta: {len(new_id_meta)})')
    if added:   log.info(f'Добавлено ID-ключей: {len(added)}')
    if removed: log.info(f'Убрано ID-ключей: {len(removed)}')


# ══════════════════════════════════════════════════════════════════════════════
# Фоновая задача: перезагрузка настроек
# ══════════════════════════════════════════════════════════════════════════════

async def _settings_reload_loop(clients: dict, ss):
    while True:
        await asyncio.sleep(SETTINGS_RELOAD_SEC)
        try:
            log.info('Перезагрузка настроек...')
            loop         = asyncio.get_event_loop()
            new_settings = await loop.run_in_executor(_executor, _read_settings,      ss)
            new_rules    = await loop.run_in_executor(_executor, _read_scoring_rules, ss)
            new_minus    = await loop.run_in_executor(_executor, _read_minus_words,   ss)
            new_channels = await loop.run_in_executor(_executor, _read_channels,      ss)

            if new_settings:
                state.update({
                    'tg_token':          new_settings['tg_token'],
                    'score_threshold':   new_settings['score_threshold'],
                    'min_length':        new_settings['min_length'],
                    'moderator_chat_id': new_settings['moderator_chat_id'],
                    'dest_chat_id':      new_settings['dest_chat_id'],
                })
            if new_rules    is not None: state['scoring_rules'] = new_rules
            if new_minus    is not None: state['minus_words']   = new_minus
            if new_channels is not None:
                await _update_watched_chats(clients, new_channels, ss)

            log.info(
                f'Настройки применены | каналов: {len(state["watched_ids"])} | '
                f'правил: {len(state["scoring_rules"])} | '
                f'минус-слов: {len(state["minus_words"])} | '
                f'порог: {state["score_threshold"]}'
            )
        except Exception as e:
            log.error('Ошибка перезагрузки настроек: ' + str(e))


# ══════════════════════════════════════════════════════════════════════════════
# Фоновая задача: long-polling callback_query от бота
# ══════════════════════════════════════════════════════════════════════════════

async def _bot_polling_loop(clients: dict, ss):
    """
    Получает callback_query через getUpdates (long-polling).
    Обрабатывает нажатия ✅ / ❌ от модератора.

    pending_moderation хранит:
      ключ  = f'{src_chat_id}:{src_msg_id}'   (из callback_data)
      value = post dict (с полями src_chat_id, src_msg_id, bot_message_id, ...)
    """
    offset = 0
    loop   = asyncio.get_event_loop()

    # Выбираем первый доступный клиент для пересылки
    def _first_client() -> TelegramClient | None:
        return next(iter(clients.values()), None)

    while True:
        token     = state['tg_token']
        moderator = state['moderator_chat_id']
        dest_chat = state['dest_chat_id']

        if not token:
            await asyncio.sleep(5)
            continue

        try:
            updates = await loop.run_in_executor(
                _executor, _get_updates, token, offset, 30
            )
        except Exception as e:
            log.error(f'[bot_polling] getUpdates error: {e}')
            await asyncio.sleep(5)
            continue

        for upd in updates:
            offset = upd['update_id'] + 1
            cq = upd.get('callback_query')
            if not cq:
                continue

            cq_id   = cq['id']
            data    = cq.get('data', '')
            from_id = cq.get('from', {}).get('id', '')
            msg_id  = cq.get('message', {}).get('message_id', 0)

            # Парсим callback_data: 'approve:SRC_CHAT_ID:SRC_MSG_ID'
            parts = data.split(':', 2)
            if len(parts) != 3 or parts[0] not in ('approve', 'skip'):
                await loop.run_in_executor(
                    _executor, _answer_callback, token, cq_id, '⚠️ Неизвестная команда'
                )
                continue

            action, src_chat_id_str, src_msg_id_str = parts
            pend_key = f'{src_chat_id_str}:{src_msg_id_str}'
            post     = pending_moderation.get(pend_key)

            if not post:
                await loop.run_in_executor(
                    _executor, _answer_callback, token, cq_id,
                    '⚠️ Пост уже обработан или не найден в памяти'
                )
                # Убираем кнопки у устаревшей карточки
                await loop.run_in_executor(
                    _executor, _edit_message_reply_markup,
                    token, moderator, msg_id, '⚠️ Пост не найден в очереди'
                )
                continue

            if action == 'approve':
                # ── Публикуем через Telethon (от имени канала) ────────────
                client = _first_client()
                if client and dest_chat:
                    try:
                        # Проверяем дубли и при одобрении через модерацию
                        fp = _post_fingerprint(post['text'], post['author_name'])
                        if fp in published_fingerprints:
                            log.info(
                                f'[модерация ⛔ дубль] {post["chat_name"]} — уже опубликовано'
                            )
                            await loop.run_in_executor(
                                _executor, _answer_callback, token, cq_id,
                                '⛔ Дубль — такой пост уже опубликован'
                            )
                            await loop.run_in_executor(
                                _executor, _edit_message_reply_markup,
                                token, moderator, msg_id,
                                '⛔ Дубль — публикация отменена'
                            )
                            pending_moderation.pop(pend_key, None)
                            continue

                        ok = await _publish_post(client, post, dest_chat)
                        if ok:
                            published_fingerprints.append(fp)
                            log.info(
                                f'[модерация ✅ одобрено] '
                                f'{post["chat_name"]} → {post["link"]}'
                            )
                            await loop.run_in_executor(_executor, _write_post, ss, post)
                            await loop.run_in_executor(
                                _executor, _update_rejected_status,
                                ss, post.get('bot_message_id', 0), 'одобрено'
                            )
                            await loop.run_in_executor(
                                _executor, _answer_callback, token, cq_id, '✅ Опубликовано!'
                            )
                            await loop.run_in_executor(
                                _executor, _edit_message_reply_markup,
                                token, moderator, msg_id,
                                f'✅ Опубликовано модератором {from_id}'
                            )
                        else:
                            await loop.run_in_executor(
                                _executor, _answer_callback, token, cq_id,
                                '❌ Ошибка публикации — смотрите логи'
                            )
                    except Exception as e:
                        log.error(f'[модерация] Ошибка публикации одобренного: {e}')
                        await loop.run_in_executor(
                            _executor, _answer_callback, token, cq_id,
                            f'❌ Ошибка: {e}'
                        )
                else:
                    await loop.run_in_executor(
                        _executor, _answer_callback, token, cq_id,
                        '⚠️ Нет клиента или dest_chat_id'
                    )

            elif action == 'skip':
                log.info(f'[модерация ❌ пропущено] {post["chat_name"]} → {post["link"]}')
                await loop.run_in_executor(
                    _executor, _update_rejected_status,
                    ss, post.get('bot_message_id', 0), 'пропущено'
                )
                await loop.run_in_executor(
                    _executor, _answer_callback, token, cq_id, '❌ Пост пропущен'
                )
                await loop.run_in_executor(
                    _executor, _edit_message_reply_markup,
                    token, moderator, msg_id,
                    f'❌ Пропущено модератором {from_id}'
                )

            # Удаляем из очереди после обработки
            pending_moderation.pop(pend_key, None)

        await asyncio.sleep(0)  # отдаём управление event-loop'у


# ══════════════════════════════════════════════════════════════════════════════
# MAIN — регистрация хендлера + запуск
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info('═══ TG Parser v3 стартует ═══')

    loop = asyncio.get_event_loop()

    # ── Google Sheets ──────────────────────────────────────────────────────
    try:
        ss = await loop.run_in_executor(_executor, _get_spreadsheet)
        log.info('Google Sheets: подключён')
    except Exception as e:
        log.error('Google Sheets: ошибка подключения: ' + str(e))
        return

    settings = await loop.run_in_executor(_executor, _read_settings,      ss)
    rules    = await loop.run_in_executor(_executor, _read_scoring_rules, ss)
    minus    = await loop.run_in_executor(_executor, _read_minus_words,   ss)
    channels = await loop.run_in_executor(_executor, _read_channels,      ss)

    if not settings:
        log.error('Не удалось прочитать настройки — проверьте лист «Настройки»')
        return

    state.update({
        'tg_token':          settings['tg_token'],
        'score_threshold':   settings['score_threshold'],
        'min_length':        settings['min_length'],
        'moderator_chat_id': settings['moderator_chat_id'],
        'dest_chat_id':      settings['dest_chat_id'],
        'scoring_rules':     rules,
        'minus_words':       minus,
    })
    log.info(
        f'Настройки загружены | порог: {state["score_threshold"]} | '
        f'мин.длина: {state["min_length"]} | '
        f'правил скоринга: {len(rules)} | минус-слов: {len(minus)}'
    )

    # ── Telegram клиенты ───────────────────────────────────────────────────
    clients: dict[str, TelegramClient] = {}

    if SESSION_1 and API_ID_1 and API_HASH_1:
        c1 = TelegramClient(StringSession(SESSION_1), API_ID_1, API_HASH_1)
        await c1.start()
        clients['acc1'] = c1
        log.info('Аккаунт 1: подключён')
    else:
        log.warning('Аккаунт 1: пропущен (нет TG_SESSION_1 / TG_API_ID_1 / TG_API_HASH_1)')

    if SESSION_2 and API_ID_2 and API_HASH_2:
        c2 = TelegramClient(StringSession(SESSION_2), API_ID_2, API_HASH_2)
        await c2.start()
        clients['acc2'] = c2
        log.info('Аккаунт 2: подключён')
    else:
        log.info('Аккаунт 2: не задан (работаем с одним аккаунтом)')

    if not clients:
        log.error('Ни один аккаунт не подключён — выход')
        return

    # ── Резолв каналов ─────────────────────────────────────────────────────
    if not channels:
        log.warning('Лист «Каналы» пуст — добавьте каналы')
    else:
        log.info(f'Резолвим {len(channels)} каналов...')

    await _update_watched_chats(clients, channels or [], ss)
    log.info(
        f'Слежу за {len(state["watched_ids"])} ID-ключами '
        f'({len(state["username_to_meta"])} каналов)'
    )

    # ══════════════════════════════════════════════════════════════════════
    # Хендлер новых сообщений — регистрируем для каждого клиента здесь,
    # в main(), чтобы всё было в одном месте.
    # Пересылка идёт через Telethon (тот же client), бот — только для карточек.
    # ══════════════════════════════════════════════════════════════════════

    for acc_label, client in clients.items():

        @client.on(events.NewMessage)
        async def _on_new_message(event, _acc=acc_label, _client=client):
            try:
                raw_id = event.chat_id   # -1001234567890
                abs_id = abs(raw_id)     #  1001234567890

                # Ищем мета по всем вариантам ID
                meta = state['id_to_meta'].get(abs_id)
                if meta is None:
                    s = str(abs_id)
                    alt = int(s[3:]) if s.startswith('100') and len(s) > 12 else int('100' + s)
                    meta = state['id_to_meta'].get(alt)

                if meta is None:
                    log.debug(f'[{_acc}] Пропущен chat_id={raw_id} — не в списке')
                    return

                msg = event.message
                if msg.action is not None:
                    return  # служебное (пин, вход и т.д.)

                # ── Дедупликация ───────────────────────────────────────────
                dedup_key = (abs_id, msg.id)
                if dedup_key in seen_ids:
                    return
                seen_ids.append(dedup_key)

                # ── Текст ──────────────────────────────────────────────────
                text = msg.text or msg.message or ''
                if hasattr(msg, 'caption') and msg.caption:
                    text = msg.caption
                text = ' '.join(text.split())

                # ── Фильтр: минус-слова ────────────────────────────────────
                if text and _has_minus_word(text, state['minus_words']):
                    log.debug(f'[{_acc}][минус-слово] {meta["chat_name"]} — выброс')
                    return

                # ── Фильтр: минимальная длина ─────────────────────────────
                if len(text) < state['min_length']:
                    log.debug(
                        f'[{_acc}][короткий] {meta["chat_name"]} '
                        f'({len(text)} < {state["min_length"]} симв) — выброс'
                    )
                    return

                # ── Скоринг ────────────────────────────────────────────────
                score     = _calc_score(text, state['scoring_rules'])

                # Скор 0 — не представляет интереса, тихо пропускаем
                if score == 0:
                    log.debug(f'[{_acc}][скор=0] {meta["chat_name"]} — выброс')
                    return

                chat      = await event.get_chat()
                link      = _build_link(chat, msg.id)
                author_name, author_link = _get_author_info(msg)
                chat_name = meta.get('chat_name', str(abs_id))

                post = {
                    'date':        msg.date.replace(tzinfo=None),
                    'chat_name':   chat_name,
                    'author_name': author_name,
                    'author_link': author_link,
                    'link':        link,
                    'text':        text,
                    'score':       score,
                    'account':     _acc,
                    'src_chat_id': raw_id,
                    'src_msg_id':  msg.id,
                }

                tg_token  = state['tg_token']
                threshold = state['score_threshold']
                dest_chat = state['dest_chat_id']
                moderator = state['moderator_chat_id']

                if score >= threshold:
                    # ── Проверка дублей по тексту + автору ────────────────
                    fp = _post_fingerprint(text, author_name)
                    if fp in published_fingerprints:
                        log.info(
                            f'[{_acc}][дубль ⛔] {chat_name} — текст+автор уже публиковались'
                        )
                        return
                    published_fingerprints.append(fp)

                    # ── Авто-публикация от имени канала ───────────────────
                    await loop.run_in_executor(_executor, _write_post, ss, post)
                    log.info(f'[авто ✅ скор:{score} {_acc}] {chat_name} → {link}')
                    if dest_chat:
                        ok = await _publish_post(_client, post, dest_chat)
                        if not ok:
                            log.error(f'[{_acc}] Публикация не удалась: {link}')
                else:
                    # ── На модерацию: карточка с inline-кнопками ───────────
                    log.info(
                        f'[модерация ⏳ скор:{score}/{threshold} {_acc}] '
                        f'{chat_name} → {link}'
                    )
                    bot_msg_id = 0
                    if tg_token and moderator:
                        bot_msg_id = await loop.run_in_executor(
                            _executor, _send_moderation_card, post, tg_token, moderator,
                        )

                    post['bot_message_id'] = bot_msg_id

                    # Кладём в очередь для _bot_polling_loop
                    pend_key = f'{raw_id}:{msg.id}'
                    pending_moderation[pend_key] = post

                    await loop.run_in_executor(
                        _executor, _write_rejected, ss, post, bot_msg_id
                    )

            except Exception as e:
                log.error(f'[{_acc}] Ошибка обработки сообщения: {e}', exc_info=True)

        log.info(f'[{acc_label}] Хендлер зарегистрирован')

    # ── Запись в лог таблицы ───────────────────────────────────────────────
    await loop.run_in_executor(_executor, _write_log, ss, 'INFO',
        f'Запущен | аккаунтов: {len(clients)} | '
        f'каналов: {len(state["username_to_meta"])} | '
        f'правил: {len(state["scoring_rules"])} | '
        f'порог: {state["score_threshold"]} | '
        f'минус-слов: {len(state["minus_words"])}'
    )

    # ── Фоновые задачи ─────────────────────────────────────────────────────
    asyncio.create_task(_settings_reload_loop(clients, ss))
    asyncio.create_task(_bot_polling_loop(clients, ss))
    log.info(
        f'Слушаю события. '
        f'Настройки обновляются каждые {SETTINGS_RELOAD_SEC}с. '
        f'Bot polling запущен.'
    )

    # ── Держим всех клиентов живыми ────────────────────────────────────────
    await asyncio.gather(*[c.run_until_disconnected() for c in clients.values()])


if __name__ == '__main__':
    asyncio.run(main())
