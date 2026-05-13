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
import io
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
    'tg_token':             '',
    'score_threshold':      7,   # минимальный скор для авто-публикации
    'moderation_threshold': 4,   # минимальный скор для отправки на модерацию
    'min_length':           20,
    'moderator_chat_id':    '',
    'dest_chat_id':         '',
    'scoring_rules':        [],
    'minus_words':          [],
    'watched_ids':          set(),
    'id_to_meta':           {},
    'username_to_meta':     {},
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
            'tg_token':             val(1),
            'score_threshold':      int(val(2) or 7),
            'min_length':           int(val(3) or 20),
            'moderator_chat_id':    val(4),
            'dest_chat_id':         val(5),
            'moderation_threshold': int(val(6) or 4),
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
    from datetime import timezone, timedelta
    tz_utc = timezone.utc
    tz_local = timezone(timedelta(hours=TZ_OFFSET_HOURS))
    return datetime.now(tz_utc).astimezone(tz_local).replace(tzinfo=None)


def _local_dt(dt: datetime) -> datetime:
    """Конвертирует UTC datetime в GMT+3. dt ожидается naive (без tzinfo)."""
    from datetime import timezone, timedelta
    tz_utc = timezone.utc
    tz_local = timezone(timedelta(hours=TZ_OFFSET_HOURS))
    return dt.replace(tzinfo=tz_utc).astimezone(tz_local).replace(tzinfo=None)


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


def _read_entity_cache(ss) -> dict:
    """Читает лист «Кеш» → {username: {entity_id, chat_name, username}}"""
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
            ws.append_row(['username', 'entity_id', 'chat_name'])
            return {}
        result = {}
        for row in ws.get_all_values()[1:]:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                try:
                    result[row[0].strip()] = {
                        'entity_id': int(float(row[1].strip())),
                        'chat_name': row[2].strip() if len(row) > 2 else '',
                        'username':  row[0].strip(),
                    }
                except ValueError:
                    pass
        log.info(f'Кеш загружен: {len(result)} каналов')
        return result
    except Exception as e:
        log.error(f'Ошибка чтения кеша: {e}')
        return {}


def _write_entity_cache(ss):
    """Записывает state['username_to_meta'] в лист «Кеш»."""
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
        rows = [['username', 'entity_id', 'chat_name']]
        for uname, meta in state['username_to_meta'].items():
            rows.append([
                uname,
                str(meta.get('entity_id', '')),
                meta.get('chat_name', ''),
            ])
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'Кеш записан: {len(rows) - 1} каналов')
    except Exception as e:
        log.error(f'Ошибка записи кеша: {e}')


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

def _text_to_html(text: str, entities) -> str:
    if not text:
        return ''
    # Экранируем HTML-спецсимволы в plain-тексте
    escaped = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    if not entities:
        return escaped

    from telethon.tl.types import (
        MessageEntityBold, MessageEntityItalic, MessageEntityUnderline,
        MessageEntityStrike, MessageEntityCode, MessageEntityPre,
        MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention,
    )

    # Telegram offsets — в UTF-16 code units
    chars = list(text)  # список символов Unicode
    # Строим массив меток: на каждую позицию символа — какие теги открыть/закрыть
    opens  = {}
    closes = {}

    for ent in sorted(entities, key=lambda e: (e.offset, -e.length)):
        o = ent.offset
        c = ent.offset + ent.length

        if isinstance(ent, MessageEntityBold):
            tag_o, tag_c = '<b>', '</b>'
        elif isinstance(ent, MessageEntityItalic):
            tag_o, tag_c = '<i>', '</i>'
        elif isinstance(ent, MessageEntityUnderline):
            tag_o, tag_c = '<u>', '</u>'
        elif isinstance(ent, MessageEntityStrike):
            tag_o, tag_c = '<s>', '</s>'
        elif isinstance(ent, MessageEntityCode):
            tag_o, tag_c = '<code>', '</code>'
        elif isinstance(ent, MessageEntityPre):
            tag_o, tag_c = '<pre>', '</pre>'
        elif isinstance(ent, MessageEntityTextUrl):
            url = ent.url.replace('"', '&quot;')
            tag_o, tag_c = f'<a href="{url}">', '</a>'
        elif isinstance(ent, MessageEntityUrl):
            raw_url = ''.join(chars[o:c]).replace('"', '&quot;')
            tag_o, tag_c = f'<a href="{raw_url}">', '</a>'
        elif isinstance(ent, MessageEntityMention):
            mention = ''.join(chars[o:c]).lstrip('@')
            tag_o, tag_c = f'<a href="https://t.me/{mention}">', '</a>'
        else:
            continue

        opens.setdefault(o, []).append(tag_o)
        closes.setdefault(c, []).insert(0, tag_c)

    # Собираем результат
    result = []
    escaped_chars = list(escaped)  # уже экранированные символы (1:1 с chars по индексу)
    for i in range(len(chars)):
        for tag in closes.get(i, []):
            result.append(tag)
        for tag in opens.get(i, []):
            result.append(tag)
        result.append(escaped_chars[i])
    # Закрывающие теги после последнего символа
    for tag in closes.get(len(chars), []):
        result.append(tag)

    return ''.join(result)
  
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
    author_str = '—'
    if post['author_name']:
        if post['author_link']:
            author_str = f'<a href="{post["author_link"]}">{post["author_name"]}</a>'
        else:
            author_str = post['author_name']

    # Ключ для pending_moderation
    pend_key = f'{post["src_chat_id"]}:{post["src_msg_id"]}'

    lines = [
        f'📢 <b>Источник:</b>',
        f'{post["chat_name"]}',
        f'👤 <b>Автор:</b>',
        f'{author_str}',
        '',
        post['html_text'][:3500],
        '',
        f'🔗 <a href="{post["link"]}">Открыть сообщение</a>',
    ]

    result = _tg_request(token, 'sendMessage', {
        'chat_id': moderator_chat_id,
        'text': '\n'.join(lines)[:4096],
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
        'reply_markup': {
            'inline_keyboard': [[
                {'text': '✅ Отправить', 'callback_data': f'approve:{pend_key}'},
                {'text': '❌ Пропустить', 'callback_data': f'skip:{pend_key}'},
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


# ── Форматирование подписи поста ──────────────────────────────────────────────

def _build_caption(post: dict) -> str:
    """
    Собирает подпись поста:
      <оригинальный текст>

      <a href="ссылка">Название канала</a> · <a href="ссылка_автора">Имя автора</a>

    Текст НЕ нормализуется — переносы строк и форматирование сохраняются.
    Обрезается до 1024 символов (лимит caption у Bot API / Telethon).
    """
    link        = post['link']
    chat_name   = post.get('chat_name', '') or 'Источник'
    source_html = f'<a href="{link}">{chat_name}</a>'

    author_name = post.get('author_name', '').strip()
    author_link = post.get('author_link', '').strip()

    if author_name and author_link:
        footer = f'{source_html} · <a href="{author_link}">{author_name}</a>'
    elif author_name:
        footer = f'{source_html} · {author_name}'
    else:
        footer = source_html

    text = post['text']
    # Оставляем место под футер (~80 символов)
    max_text = 1024 - len(footer) - 4  # 4 = '\n\n' + запас
    if len(text) > max_text:
        text = text[:max_text].rstrip() + '…'

    return f'{text}\n\n{footer}'


# ── Bot API helpers для отправки медиа ────────────────────────────────────────

def _bot_request_raw(url: str, data: bytes, content_type: str,
                     timeout: int = 30, label: str = '') -> dict:
    """Выполняет multipart или JSON запрос к Bot API с retry на 429."""
    req = urllib.request.Request(
        url, data=data, headers={'Content-Type': content_type}
    )
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                result = json.loads(resp.read().decode('utf-8'))
                if not result.get('ok'):
                    log.error(f'[{label}] Bot API не ок: {result.get("description")}')
                return result
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get('Retry-After', 15))
                log.warning(f'[{label}] Bot API 429 — жду {retry_after}s')
                time.sleep(retry_after + 1)
            else:
                log.error(f'[{label}] Bot API HTTP {e.code}: {e}')
                return {}
        except Exception as e:
            log.error(f'[{label}] Bot API error: {e}')
            return {}
    return {}


def _build_multipart(fields: dict, files: dict) -> tuple[bytes, str]:
    """
    Собирает multipart/form-data вручную (без внешних зависимостей).
    fields: {name: str_value}
    files:  {name: (filename, bytes_data, mime)}
    Возвращает (body_bytes, content_type_header).
    """
    boundary = 'B' + str(int(time.time() * 1000))
    parts = []
    for name, value in fields.items():
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'
            f'{value}'.encode('utf-8')
        )
    for name, (filename, data, mime) in files.items():
        header = (
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
            f'Content-Type: {mime}\r\n\r\n'
        ).encode('utf-8')
        parts.append(header + data)
    body = b'\r\n'.join(parts) + f'\r\n--{boundary}--'.encode('utf-8')
    return body, f'multipart/form-data; boundary={boundary}'


def _send_photo_bot(token: str, chat_id: str, caption: str, photo: bytes):
    """Отправляет одно фото через Bot API sendPhoto."""
    url = f'https://api.telegram.org/bot{token}/sendPhoto'
    body, ct = _build_multipart(
        fields={'chat_id': chat_id, 'caption': caption[:1024], 'parse_mode': 'HTML'},
        files={'photo': ('photo.jpg', photo, 'image/jpeg')},
    )
    _bot_request_raw(url, body, ct, timeout=30, label='sendPhoto')
    time.sleep(0.3)


def _send_album_bot(token: str, chat_id: str, caption: str, photos: list[bytes]):
    """
    Отправляет альбом через Bot API sendMediaGroup.
    Первая фотография получает caption. Максимум 10 фото.
    """
    photos = photos[:10]
    url = f'https://api.telegram.org/bot{token}/sendMediaGroup'
    media_json = []
    for i in range(len(photos)):
        item: dict = {'type': 'photo', 'media': f'attach://p{i}'}
        if i == 0:
            item['caption'] = caption[:1024]
            item['parse_mode'] = 'HTML'
        media_json.append(item)

    fields = {
        'chat_id': chat_id,
        'media':   json.dumps(media_json),
    }
    files = {f'p{i}': (f'p{i}.jpg', pb, 'image/jpeg') for i, pb in enumerate(photos)}
    body, ct = _build_multipart(fields, files)
    _bot_request_raw(url, body, ct, timeout=60, label='sendMediaGroup')
    time.sleep(0.3)


def _send_text_bot(token: str, chat_id: str, text: str):
    """Отправляет текстовое сообщение через Bot API (fallback)."""
    data = json.dumps({
        'chat_id':    chat_id,
        'text':       text[:4096],
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
    }).encode('utf-8')
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            if not result.get('ok'):
                log.error(f'sendMessage не ок: {result.get("description")}')
    except Exception as e:
        log.error(f'sendMessage error: {e}')
    time.sleep(0.3)


# ── Скачивание медиа через Telethon ───────────────────────────────────────────

async def _download_photos(client: TelegramClient, messages: list) -> list[bytes]:
    """
    Скачивает все фото/документы-изображения из списка сообщений.
    Пропускает файлы > 10 МБ и не-изображения.
    """
    photos = []
    for m in messages:
        media = m.photo or (m.document if _is_image_doc(m) else None)
        if not media:
            continue
        try:
            buf = io.BytesIO()
            await asyncio.wait_for(client.download_media(m, file=buf), timeout=30)
            photos.append(buf.getvalue())
        except asyncio.TimeoutError:
            log.warning(f'download_media timeout msg_id={m.id}')
        except Exception as e:
            log.warning(f'download_media error msg_id={m.id}: {e}')
    return photos


def _is_image_doc(msg) -> bool:
    """True если document это изображение (jpeg/png/webp/gif)."""
    doc = getattr(msg, 'document', None)
    if not doc:
        return False
    mime = getattr(doc, 'mime_type', '') or ''
    return mime.startswith('image/')


# ── Публикация поста в канал (текст + медиа) ──────────────────────────────────

async def _publish_post(client: TelegramClient, post: dict,
                        dest_chat: str, photos: list[bytes] | None = None):
    """
    Публикует пост в канал от имени канала (без «переслано от»).

    Если есть фото — использует Bot API (sendPhoto / sendMediaGroup),
    так как Telethon.send_file требует upload а не bytes.
    Если фото нет — отправляет через Telethon send_message (HTML).

    Возвращает True при успехе.
    """
    caption  = _build_caption(post)
    token    = state.get('tg_token', '')

    try:
        if photos and token:
            # Bot API: канал видит бота как автора — нормально для публикации
            if len(photos) == 1:
                await asyncio.get_event_loop().run_in_executor(
                    _executor, _send_photo_bot, token, dest_chat, caption, photos[0]
                )
            else:
                await asyncio.get_event_loop().run_in_executor(
                    _executor, _send_album_bot, token, dest_chat, caption, photos
                )
        elif photos and not token:
            # Нет токена — шлём через Telethon, фото как bytes через upload
            await client.send_file(
                entity=int(dest_chat),
                file=photos if len(photos) > 1 else photos[0],
                caption=caption,
                parse_mode='html',
            )
        else:
            # Только текст — Telethon
            await client.send_message(
                entity=int(dest_chat),
                message=caption,
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

    # Пишем кеш один раз в конце — избегаем 429 от Sheets API
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _write_entity_cache, ss)


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
                    'tg_token':             new_settings['tg_token'],
                    'score_threshold':      new_settings['score_threshold'],
                    'moderation_threshold': new_settings['moderation_threshold'],
                    'min_length':           new_settings['min_length'],
                    'moderator_chat_id':    new_settings['moderator_chat_id'],
                    'dest_chat_id':         new_settings['dest_chat_id'],
                })
            if new_rules    is not None: state['scoring_rules'] = new_rules
            if new_minus    is not None: state['minus_words']   = new_minus
            if new_channels is not None:
                await _update_watched_chats(clients, new_channels, ss)

            log.info(
                f'Настройки применены | каналов: {len(state["watched_ids"])} | '
                f'правил: {len(state["scoring_rules"])} | '
                f'минус-слов: {len(state["minus_words"])} | '
                f'публикация: {state["score_threshold"]} | '
                f'модерация: {state["moderation_threshold"]}'
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
            err_str = str(e)
            if '409' in err_str:
                log.warning('[bot_polling] 409 Conflict — другой инстанс ещё жив, жду 15с...')
                await asyncio.sleep(15)
            else:
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
                        fp = _post_fingerprint(post['text'], post['author_name'])
                        if fp in published_fingerprints:
                            log.info(f'[модерация ⛔ дубль] {post["chat_name"]}')
                            await loop.run_in_executor(
                                _executor, _answer_callback, token, cq_id,
                                '⛔ Дубль — такой пост уже опубликован'
                            )
                            await loop.run_in_executor(
                                _executor, _edit_message_reply_markup,
                                token, moderator, msg_id, '⛔ Дубль — публикация отменена'
                            )
                            pending_moderation.pop(pend_key, None)
                            continue

                        # Скачиваем фото (они были сохранены при поступлении)
                        grouped_msgs = post.get('grouped_msgs', [])
                        photos = await _download_photos(client, grouped_msgs) if grouped_msgs else []

                        ok = await _publish_post(client, post, dest_chat, photos or None)
                        if ok:
                            published_fingerprints.append(fp)
                            log.info(f'[модерация ✅ одобрено фото:{len(photos)}] {post["chat_name"]} → {post["link"]}')
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
                            _executor, _answer_callback, token, cq_id, f'❌ Ошибка: {e}'
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
        'tg_token':             settings['tg_token'],
        'score_threshold':      settings['score_threshold'],
        'moderation_threshold': settings['moderation_threshold'],
        'min_length':           settings['min_length'],
        'moderator_chat_id':    settings['moderator_chat_id'],
        'dest_chat_id':         settings['dest_chat_id'],
        'scoring_rules':        rules,
        'minus_words':          minus,
    })
    log.info(
        f'Настройки загружены | публикация: {state["score_threshold"]} | '
        f'модерация: {state["moderation_threshold"]} | '
        f'мин.длина: {state["min_length"]} | '
        f'правил скоринга: {len(rules)} | минус-слов: {len(minus)}'
    )

    # ── Сбрасываем webhook — иначе getUpdates вернёт 409 ──────────────────
    if state['tg_token']:
        await loop.run_in_executor(
            _executor, _tg_request,
            state['tg_token'], 'deleteWebhook', {'drop_pending_updates': False}
        )
        log.info('Webhook сброшен')

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
    # Сначала загружаем кеш entity_id из таблицы — чтобы не делать get_entity
    # для уже известных каналов при каждом рестарте.
    cached_meta = await loop.run_in_executor(_executor, _read_entity_cache, ss)
    state['username_to_meta'].update(cached_meta)

    if not channels:
        log.warning('Лист «Каналы» пуст — добавьте каналы')
    else:
        known = sum(1 for ch in channels if _extract_username(ch['username']) in cached_meta)
        log.info(f'Каналов: {len(channels)} | из кеша: {known} | новых (нужен резолв): {len(channels) - known}')

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

    # ── Буфер для сборки альбомов ──────────────────────────────────────────
    # grouped_id → {'msgs': [...], 'timer': asyncio.TimerHandle, 'client': ..., 'acc': str}
    album_buffer: dict = {}

    async def _flush_album(grouped_id: int, _acc: str, _client: TelegramClient):
        """Вызывается через 1.5с после первого сообщения группы — обрабатывает альбом."""
        entry = album_buffer.pop(grouped_id, None)
        if not entry:
            return
        msgs = sorted(entry['msgs'], key=lambda m: m.id)
        first = msgs[0]

        raw_id = first.peer_id.channel_id if hasattr(first.peer_id, 'channel_id') else first.chat_id
        # Нормализуем: событие могло прийти с разными форматами
        try:
            raw_id = first.chat_id
        except Exception:
            pass
        abs_id = abs(raw_id)

        meta = state['id_to_meta'].get(abs_id)
        if meta is None:
            s = str(abs_id)
            alt = int(s[3:]) if s.startswith('100') and len(s) > 12 else int('100' + s)
            meta = state['id_to_meta'].get(alt)
        if meta is None:
            return

         # Берём текст из первого сообщения с непустым текстом
        text = ''
        text_entities = None
        for m in msgs:
            t = m.text or m.message or ''
            if hasattr(m, 'caption') and m.caption:
                t = m.caption
            if t.strip():
                text = t
                text_entities = m.entities
                break
        html_text = _text_to_html(text, text_entities)

        if _has_minus_word(text, state['minus_words']):
            return
        if len(text) < state['min_length']:
            return

        score = _calc_score(text, state['scoring_rules'])
        if score < state['moderation_threshold']:
            return

        try:
            chat = await _client.get_entity(raw_id)
        except Exception:
            chat = None

        link      = _build_link(chat, first.id) if chat else f'https://t.me/c/{abs_id}/{first.id}'
        chat_name = meta.get('chat_name', str(abs_id))
        author_name, author_link = _get_author_info(first)

        post = {
            'date':        first.date.replace(tzinfo=None),
            'chat_name':   chat_name,
            'author_name': author_name,
            'author_link': author_link,
            'link':        link,
            'text':        text,
            'html_text':   html_text,
            'score':       score,
            'account':     _acc,
            'src_chat_id': raw_id,
            'src_msg_id':  first.id,
        }

        tg_token  = state['tg_token']
        threshold = state['score_threshold']
        dest_chat = state['dest_chat_id']
        moderator = state['moderator_chat_id']

        if score >= threshold:
            fp = _post_fingerprint(text, author_name)
            if fp in published_fingerprints:
                log.info(f'[{_acc}][дубль альбом ⛔] {chat_name}')
                return
            published_fingerprints.append(fp)

            photos = await _download_photos(_client, msgs)
            await loop.run_in_executor(_executor, _write_post, ss, post)
            log.info(f'[авто альбом ✅ скор:{score} фото:{len(photos)} {_acc}] {chat_name} → {link}')
            if dest_chat:
                ok = await _publish_post(_client, post, dest_chat, photos)
                if not ok:
                    log.error(f'[{_acc}] Публикация альбома не удалась: {link}')
        else:
            log.info(f'[модерация альбом ⏳ скор:{score}/{threshold} {_acc}] {chat_name} → {link}')
            post['photos_count'] = len(msgs)  # фото скачаем только при одобрении
            bot_msg_id = 0
            if tg_token and moderator:
                bot_msg_id = await loop.run_in_executor(
                    _executor, _send_moderation_card, post, tg_token, moderator,
                )
            post['bot_message_id'] = bot_msg_id
            post['grouped_msgs']   = msgs  # сохраняем для скачивания при одобрении
            pend_key = f'{raw_id}:{first.id}'
            pending_moderation[pend_key] = post
            await loop.run_in_executor(_executor, _write_rejected, ss, post, bot_msg_id)

    for acc_label, client in clients.items():

        @client.on(events.NewMessage)
        async def _on_new_message(event, _acc=acc_label, _client=client):
            try:
                raw_id = event.chat_id
                abs_id = abs(raw_id)

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
                    return

                # ── Дедупликация входящих ──────────────────────────────────
                dedup_key = (abs_id, msg.id)
                if dedup_key in seen_ids:
                    return
                seen_ids.append(dedup_key)

                # ── Альбом: собираем grouped_id в буфер ───────────────────
                grouped_id = getattr(msg, 'grouped_id', None)
                if grouped_id:
                    if grouped_id not in album_buffer:
                        # Запускаем таймер — через 1.5с обрабатываем всю группу
                        handle = loop.call_later(
                            1.5, lambda gid=grouped_id: asyncio.ensure_future(
                                _flush_album(gid, _acc, _client)
                            )
                        )
                        album_buffer[grouped_id] = {
                            'msgs': [], 'timer': handle,
                            'acc': _acc, 'client': _client,
                        }
                    album_buffer[grouped_id]['msgs'].append(msg)
                    return  # одиночную обработку не делаем — ждём таймер

                # ── Одиночное сообщение ────────────────────────────────────
                # Текст — без flatten, сохраняем оригинальное форматирование
                text = msg.text or msg.message or ''
                if hasattr(msg, 'caption') and msg.caption:
                    text = msg.caption
                text = re.sub(r'[^\S\n]+', ' ', text).strip()
                html_text = _text_to_html(text, msg.entities)

                if _has_minus_word(text, state['minus_words']):
                    log.debug(f'[{_acc}][минус-слово] {meta["chat_name"]} — выброс')
                    return

                if len(text) < state['min_length']:
                    log.debug(f'[{_acc}][короткий] {meta["chat_name"]} ({len(text)}) — выброс')
                    return

                score = _calc_score(text, state['scoring_rules'])
                mod_threshold = state['moderation_threshold']
                if score < mod_threshold:
                    log.debug(f'[{_acc}][скор:{score}<{mod_threshold}] {meta["chat_name"]} — выброс')
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
                    'html_text':   html_text,
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
                    fp = _post_fingerprint(text, author_name)
                    if fp in published_fingerprints:
                        log.info(f'[{_acc}][дубль ⛔] {chat_name}')
                        return
                    published_fingerprints.append(fp)

                    # Скачиваем фото если есть
                    photos = await _download_photos(_client, [msg])

                    await loop.run_in_executor(_executor, _write_post, ss, post)
                    log.info(f'[авто ✅ скор:{score} фото:{len(photos)} {_acc}] {chat_name} → {link}')
                    if dest_chat:
                        ok = await _publish_post(_client, post, dest_chat, photos)
                        if not ok:
                            log.error(f'[{_acc}] Публикация не удалась: {link}')
                else:
                    log.info(f'[модерация ⏳ скор:{score}/{threshold} {_acc}] {chat_name} → {link}')
                    bot_msg_id = 0
                    if tg_token and moderator:
                        bot_msg_id = await loop.run_in_executor(
                            _executor, _send_moderation_card, post, tg_token, moderator,
                        )
                    post['bot_message_id'] = bot_msg_id
                    post['grouped_msgs']   = [msg]  # для скачивания при одобрении
                    pend_key = f'{raw_id}:{msg.id}'
                    pending_moderation[pend_key] = post
                    await loop.run_in_executor(_executor, _write_rejected, ss, post, bot_msg_id)

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
