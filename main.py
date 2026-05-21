"""
TG Parser v4 — один аккаунт (или два), скоринг + AI-модерация через Gemini.
Маршрутизация: approve_private → dest_chat_id, approve_agent → dest_chat_id_agent.
Новое в v4:
  - 4 класса AI: approve_private, approve_agent, ask_author, skip
  - ask_author: Telethon пишет автору в личку, ждёт ответ 15 минут,
    повторно прогоняет через AI → публикует в нужный канал
  - Дедупликация по листам «Посты» И «Отклонено ИИ» (не делаем повторных запросов к AI)
  - Запись ожидающих ответа в лист «Ожидание»
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
import unicodedata
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
    UserIsBlockedError,
    ChatWriteForbiddenError,
    InputUserDeactivatedError,
)
from telethon.sessions import StringSession
from telethon.tl.types import MessageService, MessageEmpty

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
EXECUTOR_WORKERS       = int(os.environ.get('EXECUTOR_WORKERS', '8'))
GEMINI_API_KEY         = os.environ.get('GEMINI_API_KEY', '')

# Таймаут ожидания ответа от автора (секунды)
ASK_AUTHOR_TIMEOUT_SEC = int(os.environ.get('ASK_AUTHOR_TIMEOUT_SEC', '900'))  # 15 минут
ASK_AUTHOR_ACCOUNT = os.environ.get('ASK_AUTHOR_ACCOUNT', 'acc2')

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
    'score_threshold':      7,
    'moderation_threshold': 4,
    'min_length':           20,
    'moderator_chat_id':    '',
    'dest_chat_id':         '',
    'dest_chat_id_agent':   '',
    'scoring_rules':        [],
    'minus_words':          [],
    'watched_ids':          set(),
    'id_to_meta':           {},
    'username_to_meta':     {},
    'excluded_accounts':    set(),
    'realtors':             set(),
}

TZ_OFFSET_HOURS = 3

seen_ids: deque = deque(maxlen=2000)
published_fingerprints: deque = deque(maxlen=10000)

# Fingerprints постов уже отклонённых AI — не делаем повторный запрос к Gemini
ai_rejected_fingerprints: deque = deque(maxlen=10000)

pending_moderation: dict = {}

# Словарь ожидания ответа от автора поста
# Ключ: user_id (int)
# Значение: {
#   'post': dict,
#   'client': TelegramClient,
#   'ss': spreadsheet,
#   'acc': str,
#   'sender_bio': str,
#   'added_at': float,
#   'timeout_handle': asyncio.TimerHandle,
# }
pending_ask_author: dict = {}

_executor = ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS)
_sheets_lock: asyncio.Lock | None = None
_state_lock:  asyncio.Lock | None = None

metrics = {
    'processed': 0,
    'published': 0,
    'moderated': 0,
    'asked':     0,
    'errors':    0,
}


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets
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


def _write_with_retry(fn, *args, max_attempts: int = 3):
    for attempt in range(max_attempts):
        try:
            fn(*args)
            return
        except gspread.exceptions.APIError as e:
            code = getattr(e.response, 'status_code', 0)
            if code in (429, 500):
                wait = (2 ** attempt) * 5
                log.warning(f'Sheets {code} — retry через {wait}s (попытка {attempt + 1})')
                time.sleep(wait)
            else:
                log.error(f'Sheets APIError: {e}', exc_info=True)
                return
        except Exception as e:
            log.error(f'Sheets write failed: {e}', exc_info=True)
            return
    log.error(f'Sheets write не удалась после {max_attempts} попыток')


async def _safe_sheets(fn, *args):
    loop = asyncio.get_event_loop()
    async with _sheets_lock:
        await loop.run_in_executor(_executor, fn, *args)


async def _safe_sheets_retry(fn, *args):
    loop = asyncio.get_event_loop()
    async with _sheets_lock:
        await loop.run_in_executor(_executor, _write_with_retry, fn, *args)


async def _safe_sheets_result(fn, *args):
    loop = asyncio.get_event_loop()
    async with _sheets_lock:
        return await loop.run_in_executor(_executor, fn, *args)


# ── Чтение ─────────────────────────────────────────────────────────────────────

def _read_settings(ss):
    try:
        data = ss.worksheet('Настройки').get_all_values()
        def val(row_idx):
            return str(data[row_idx][1]).strip() if len(data) > row_idx and len(data[row_idx]) > 1 else ''
        return {
            'tg_token':             val(1),
            'score_threshold':      int(val(2) or 7),
            'moderation_threshold': int(val(3) or 3),
            'min_length':           int(val(4) or 20),
            'moderator_chat_id':    val(5),
            'dest_chat_id':         val(6),
            'dest_chat_id_agent':   val(7),
            'excluded_accounts':    val(8),
        }
    except Exception as e:
        log.error('Ошибка чтения настроек: ' + str(e), exc_info=True)
        return None

def _parse_excluded_accounts(raw: str) -> set:
    result = set()
    for part in raw.split(','):
        part = part.strip()
        if part.isdigit():
            result.add(int(part))
    return result

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
        log.error('Ошибка чтения скоринга: ' + str(e), exc_info=True)
        return []


def _read_minus_words(ss):
    try:
        data = ss.worksheet('Минус-слова').get_all_values()
        return [str(row[0]).strip().lower() for row in data[1:] if row and row[0].strip()]
    except Exception as e:
        log.error('Ошибка чтения минус-слов: ' + str(e), exc_info=True)
        return []

# ── Стало ─────────────────────────────────────────────────────────────────────

def _read_realtors_raw(ss) -> tuple[set, list]:
    """
    Читает лист «Риэлторы».
    Возвращает:
      resolved   — set[int]  уже известные числовые user_id
      to_resolve — list[str] username-ы/ссылки, требующие резолва через Telethon
    """
    try:
        try:
            ws = ss.worksheet('Риэлторы')
        except Exception:
            ws = ss.add_worksheet('Риэлторы', 1000, 3)
            ws.append_row(['user_id', 'имя', 'комментарий'])
            return set(), []

        resolved   = set()
        to_resolve = []

        for row in ws.get_all_values()[1:]:
            if not row or not row[0].strip():
                continue
            raw = row[0].strip()

            # Числовой ID (включая отрицательные) — сразу в set
            if raw.lstrip('-').isdigit():
                resolved.add(int(raw))
                continue

            # Ссылка вида https://t.me/username или @username или просто username
            username = _extract_username(raw)
            if username:
                to_resolve.append(username)
            else:
                log.warning(f'[риэлторы] Не удалось распознать запись: {repr(raw)}')

        log.info(
            f'Риэлторы: {len(resolved)} числовых ID, '
            f'{len(to_resolve)} username для резолва'
        )
        return resolved, to_resolve

    except Exception as e:
        log.error(f'Ошибка чтения риэлторов: {e}', exc_info=True)
        return set(), []


async def _resolve_realtors(
    ss,
    clients: dict,
    resolved: set,
    to_resolve: list,
) -> set:
    """
    Резолвит список username-ов через Telethon → user_id (int).
    Найденные ID записывает обратно в лист (колонка A), чтобы при
    следующей загрузке они уже читались как числа без повторного резолва.
    Возвращает итоговый set[int].
    """
    if not to_resolve:
        return resolved

    client = next(iter(clients.values()), None)
    if not client:
        log.warning('[риэлторы] Нет Telethon-клиента для резолва username-ов')
        return resolved

    try:
        ws   = ss.worksheet('Риэлторы')
        rows = ws.get_all_values()           # включая заголовок
    except Exception as e:
        log.error(f'[риэлторы] Не удалось открыть лист для обновления: {e}')
        return resolved

    # Индекс строк по username (lowercase) → номер строки в Sheets (1-based)
    row_index: dict[str, int] = {}
    for i, row in enumerate(rows[1:], start=2):
        if not row or not row[0].strip():
            continue
        raw = row[0].strip()
        if not raw.lstrip('-').isdigit():
            uname = _extract_username(raw)
            if uname:
                row_index[uname.lower()] = i

    newly_resolved = set()

    for username in to_resolve:
        try:
            entity  = await client.get_entity(username)
            user_id = entity.id          # для юзеров всегда положительный int
            resolved.add(user_id)
            newly_resolved.add(user_id)
            log.info(f'[риэлторы] @{username} → user_id={user_id}')

            # Перезаписываем ячейку числовым ID, чтобы больше не резолвить
            sheet_row = row_index.get(username.lower())
            if sheet_row:
                try:
                    ws.update(
                        values=[[str(user_id)]],
                        range_name=f'A{sheet_row}',
                    )
                except Exception as e:
                    log.warning(f'[риэлторы] Не удалось обновить A{sheet_row}: {e}')

        except FloodWaitError as e:
            log.warning(f'[риэлторы] FloodWait {e.seconds}s при резолве @{username}')
            await asyncio.sleep(e.seconds + 2)
            try:
                entity  = await client.get_entity(username)
                user_id = entity.id
                resolved.add(user_id)
                newly_resolved.add(user_id)
            except Exception as e2:
                log.warning(f'[риэлторы] Повторная ошибка @{username}: {e2}')

        except (UsernameNotOccupiedError, UsernameInvalidError) as e:
            log.warning(f'[риэлторы] Username не найден @{username}: {e}')

        except Exception as e:
            log.warning(f'[риэлторы] Ошибка резолва @{username}: {e}')

        await asyncio.sleep(0.5)   # пауза между запросами к TG

    if newly_resolved:
        log.info(f'[риэлторы] Резолвлено username → ID: {newly_resolved}')

    return resolved

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
        log.error('Ошибка чтения каналов: ' + str(e), exc_info=True)
        return []


# ── Запись ─────────────────────────────────────────────────────────────────────

def _local_now() -> datetime:
    from datetime import timezone, timedelta
    return datetime.now(timezone.utc).astimezone(
        __import__('datetime').timezone(__import__('datetime').timedelta(hours=TZ_OFFSET_HOURS))
    ).replace(tzinfo=None)


def _local_dt(dt: datetime) -> datetime:
    from datetime import timezone, timedelta
    tz_local = timezone(timedelta(hours=TZ_OFFSET_HOURS))
    return dt.replace(tzinfo=timezone.utc).astimezone(tz_local).replace(tzinfo=None)

def _flatten_text(text: str) -> str:
    """Убирает переносы строк и лишние пробелы — одна строка для хранения в Sheets."""
    return re.sub(r'\s+', ' ', text or '').strip()

def _write_post(ss, post):
    try:
        ss.worksheet('Посты').append_row([
            _local_dt(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['author_name'],
            post['author_link'],
            post['link'],
            _flatten_text(post['text']),   # ← было post['text']
            post['score'],
            post['account'],
            post.get('ai_decision', ''),
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error('Ошибка записи поста: ' + str(e), exc_info=True)
        raise


def _write_rejected(ss, post, bot_message_id):
    try:
        ss.worksheet('Отклонённые').append_row([
            _local_dt(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['link'],
            _flatten_text(post['text']),   # ← было post['text']
            post['score'],
            'ожидает',
            str(bot_message_id),
            post['account'],
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error('Ошибка записи отклонённого поста: ' + str(e), exc_info=True)
        raise


def _write_ai_rejected(ss, post: dict, ai_decision: str):
    try:
        ss.worksheet('Отклонено ИИ').append_row([
            _local_dt(post['date']).strftime('%Y-%m-%d %H:%M:%S'),
            post['chat_name'],
            post['link'],
            _flatten_text(post['text']),   # ← было post['text']
            post['score'],
            ai_decision,
            post['account'],
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error(f'Ошибка записи в Отклонено ИИ: {e}', exc_info=True)


def _write_ask_author(ss, post: dict, user_id: int):
    try:
        try:
            ws = ss.worksheet('Ожидание')
        except Exception:
            ws = ss.add_worksheet('Ожидание', 1000, 9)
            ws.append_row([
                'timestamp', 'user_id', 'author_name', 'author_link',
                'chat_name', 'post_link', 'post_text', 'статус', 'ai_decision_final',
            ])
        ws.append_row([
            _local_now().strftime('%Y-%m-%d %H:%M:%S'),
            str(user_id),
            post['author_name'],
            post['author_link'],
            post['chat_name'],
            post['link'],
            _flatten_text(post['text']),   # ← уже было [:500], теперь ещё и flatten
            'ожидает',
            '',
        ], value_input_option='USER_ENTERED')
    except Exception as e:
        log.error(f'Ошибка записи в Ожидание: {e}', exc_info=True)

def _add_realtor_to_sheet(ss, post: dict, user_id: int):
    """Добавляет риэлтора в лист «Риэлторы» если его там ещё нет."""
    try:
        try:
            ws = ss.worksheet('Риэлторы')
        except Exception:
            ws = ss.add_worksheet('Риэлторы', 1000, 4)
            ws.append_row(['user_id', 'имя', 'комментарий', 'username'])

        # Проверяем — вдруг уже есть
        existing = [r[0].strip() for r in ws.get_all_values()[1:] if r]
        if str(user_id) in existing:
            return

        # Извлекаем @username из ссылки https://t.me/username
        author_link = post.get('author_link', '')
        username = ''
        if author_link:
            m = re.search(r't\.me/([a-zA-Z0-9_]+)', author_link)
            if m:
                username = '@' + m.group(1)

        ws.append_row([
            str(user_id),
            post.get('author_name', ''),
            f'авто: ответил на ask_author {_local_now().strftime("%Y-%m-%d %H:%M")}',
            username,
        ], value_input_option='USER_ENTERED')
        log.info(f'[риэлторы] user_id={user_id} ({username}) записан в лист')
    except Exception as e:
        log.error(f'Ошибка записи риэлтора: {e}', exc_info=True)

def _update_ask_author_status(ss, user_id: int, status: str, ai_decision: str = ''):
    """Обновляет статус строки в листе «Ожидание» по user_id (первая строка со статусом 'ожидает')."""
    try:
        ws   = ss.worksheet('Ожидание')
        data = ws.get_all_values()
        for i, row in enumerate(data[1:], start=2):
            if len(row) > 1 and row[1] == str(user_id) and (len(row) < 8 or row[7] == 'ожидает'):
                ws.update(values=[[status, ai_decision]], range_name=f'H{i}:I{i}')
                return
    except Exception as e:
        log.error(f'Ошибка обновления статуса Ожидание: {e}', exc_info=True)


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
        log.error('Ошибка записи лога: ' + str(e), exc_info=True)


def _read_entity_cache(ss) -> dict:
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
        log.error(f'Ошибка чтения кеша: {e}', exc_info=True)
        return {}


def _write_entity_cache(ss):
    try:
        try:
            ws = ss.worksheet('Кеш')
        except Exception:
            ws = ss.add_worksheet('Кеш', 1000, 3)
        rows = [['username', 'entity_id', 'chat_name']]
        for uname, meta in state['username_to_meta'].items():
            rows.append([uname, str(meta.get('entity_id', '')), meta.get('chat_name', '')])
        ws.clear()
        ws.update(rows, value_input_option='USER_ENTERED')
        log.info(f'Кеш записан: {len(rows) - 1} каналов')
    except Exception as e:
        log.error(f'Ошибка записи кеша: {e}', exc_info=True)


def _set_channel_status(ss, username: str, status: str):
    try:
        ws   = ss.worksheet('Каналы')
        data = ws.get_all_values()
        for i, row in enumerate(data[1:], start=2):
            if not row:
                continue
            u = _extract_username(row[0].strip())
            if u and u.lower() == username.lower():
                ws.update(values=[[status]], range_name=f'C{i}')
                return
    except Exception as e:
        log.error(f'Ошибка обновления статуса канала {username}: {e}', exc_info=True)


def _update_rejected_status(ss, bot_message_id: int, new_status: str):
    try:
        ws   = ss.worksheet('Отклонённые')
        data = ws.get_all_values()
        for i, row in enumerate(data[1:], start=2):
            if len(row) > 6 and str(row[6]) == str(bot_message_id):
                ws.update(values=[[new_status]], range_name=f'F{i}')
                return
    except Exception as e:
        log.error(f'Ошибка обновления статуса отклонённого поста: {e}', exc_info=True)


# ── Fingerprints ───────────────────────────────────────────────────────────────

def _post_fingerprint(text: str, author_name: str) -> str:
    norm = unicodedata.normalize('NFKC', text.lower())
    norm = re.sub(r'[\s\W]+', '', norm)[:120]
    author_key = re.sub(r'\s+', '', author_name.lower())
    return f'{author_key}|{norm}'


def _load_published_fingerprints(ss) -> set:
    try:
        from datetime import date, timedelta
        today     = date.today()
        yesterday = today - timedelta(days=1)  # если надо добавить позавчера - изменить 1 на 2
        cutoff    = yesterday.strftime('%Y-%m-%d')   # 'YYYY-MM-DD' — достаточно префикса

        data   = ss.worksheet('Посты').get_all_values()
        result = set()
        skipped = 0
        for row in data[1:]:
            ts          = row[0].strip() if row else ''
            text        = row[5].strip() if len(row) > 5 else ''
            author_name = row[2].strip() if len(row) > 2 else ''
            # Дата хранится как 'YYYY-MM-DD HH:MM:SS' — берём первые 10 символов
            if ts[:10] < cutoff:
                skipped += 1
                continue
            if text:
                result.add(_post_fingerprint(text, author_name))
        log.info(
            f'Загружено {len(result)} fingerprint-ов из листа «Посты» '
            f'(пропущено старых: {skipped})'
        )
        return result
    except Exception as e:
        log.error(f'Ошибка загрузки fingerprints (Посты): {e}', exc_info=True)
        return set()


def _load_ai_rejected_fingerprints(ss) -> set:
    """
    Загружает fingerprints из листа «Отклонено ИИ».
    Используем пустой author_name — в этом листе автор не хранится.
    Это позволяет отсекать дубли одного поста из разных групп,
    не делая повторных запросов к Gemini.
    """
    try:
        data = ss.worksheet('Отклонено ИИ').get_all_values()
        result = set()
        for row in data[1:]:
            text = row[3].strip() if len(row) > 3 else ''  # колонка D — текст поста
            if text:
                result.add(_post_fingerprint(text, ''))
        log.info(f'Загружено {len(result)} fingerprint-ов из листа «Отклонено ИИ»')
        return result
    except Exception as e:
        log.error(f'Ошибка загрузки fingerprints (Отклонено ИИ): {e}', exc_info=True)
        return set()


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
    variants: set[int] = {eid}
    s = str(eid)
    try:
        if s.startswith('100') and len(s) > 12:
            short = int(s[3:])
            if short > 0:
                variants.add(short)
        else:
            variants.add(int('100' + s))
    except ValueError:
        pass
    return list(variants)


def _meta_by_abs_id(id_to_meta: dict, abs_id: int):
    meta = id_to_meta.get(abs_id)
    if meta is not None:
        return meta
    s = str(abs_id)
    try:
        if s.startswith('100') and len(s) > 12:
            alt = int(s[3:])
        else:
            alt = int('100' + s)
        return id_to_meta.get(alt)
    except ValueError:
        return None


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
    """Возвращает (name, link, user_id)."""
    try:
        sender = msg.sender
        if not sender:
            return '', '', 0
        first    = getattr(sender, 'first_name', '') or ''
        last     = getattr(sender, 'last_name',  '') or ''
        username = getattr(sender, 'username',   '') or ''
        name     = (first + ' ' + last).strip()
        link     = f'https://t.me/{username}' if username else ''
        user_id  = getattr(sender, 'id', 0) or 0
        return name, link, user_id
    except Exception:
        return '', '', 0


async def _get_sender_bio(client: TelegramClient, msg) -> str:
    """Возвращает bio отправителя или пустую строку."""
    try:
        sender = await msg.get_sender()
        if sender is None:
            return ''
        from telethon.tl.functions.users import GetFullUserRequest
        full = await client(GetFullUserRequest(sender))
        return (full.full_user.about or '').strip()
    except Exception:
        return ''


def _utf16_to_unicode_idx(text: str, utf16_offset: int) -> int:
    idx = 0
    u16 = 0
    for ch in text:
        if u16 >= utf16_offset:
            break
        u16 += 2 if ord(ch) > 0xFFFF else 1
        idx += 1
    return idx


def _text_to_html(text: str, entities) -> str:
    if not text:
        return ''
    escaped = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    if not entities:
        return escaped

    from telethon.tl.types import (
        MessageEntityBold, MessageEntityItalic, MessageEntityUnderline,
        MessageEntityStrike, MessageEntityCode, MessageEntityPre,
        MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention,
    )

    chars         = list(text)
    escaped_chars = list(escaped)
    opens  = {}
    closes = {}

    for ent in sorted(entities, key=lambda e: (e.offset, -e.length)):
        o = _utf16_to_unicode_idx(text, ent.offset)
        c = _utf16_to_unicode_idx(text, ent.offset + ent.length)

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

    result = []
    for i in range(len(chars)):
        for tag in closes.get(i, []):
            result.append(tag)
        for tag in opens.get(i, []):
            result.append(tag)
        result.append(escaped_chars[i])
    for tag in closes.get(len(chars), []):
        result.append(tag)

    return ''.join(result)


# ══════════════════════════════════════════════════════════════════════════════
# Фильтрация и скоринг
# ══════════════════════════════════════════════════════════════════════════════

def _find_minus_word(text: str, minus_words: list) -> str | None:
    lower = text.lower()
    for w in minus_words:
        if not w:
            continue
        if len(w) <= 4:
            if re.search(r'(?<![а-яёa-z])' + re.escape(w) + r'(?![а-яёa-z])', lower):
                return w
        else:
            if w in lower:
                return w
    return None


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
# AI-модерация через Gemini
# ══════════════════════════════════════════════════════════════════════════════

def _build_prompt(text: str, score: int, sender_bio: str, is_reply: bool = False) -> str:
    bio_block = (
        f'Описание аккаунта автора (bio):\n{sender_bio[:500]}'
        if sender_bio else
        'Описание аккаунта автора (bio): не указано'
    )

    if is_reply:
        return f"""Ты модератор доски объявлений по аренде недвижимости в Батуми.
Ты написал автору поста вопрос "Вы риелтор или ищете для себя?" и получил ответ.
Определи: это частный человек или риелтор/агент.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ЧАСТНЫЙ ЧЕЛОВЕК → approve_private

Любые формулировки означающие что ищет для себя лично:
- «для себя», «ищу себе», «себе», «лично для себя», «нам», «мне»
- «я сам», «мы сами», «для нашей семьи», «для меня»
- «не риелтор», «просто ищу», «обычный человек», «частное лицо»
- «снимаю для себя», «переезжаю», «живу здесь»
- Любой ответ где человек описывает личную ситуацию

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
РИЕЛТОР/АГЕНТ → approve_agent

Только если есть явное указание на профессиональную деятельность:
- «риелтор», «агент», «для клиента», «клиенту», «под клиента»
- «работаю в недвижимости», «агентство», «broker»
- «ищу для своего клиента», «подбираю для клиента»
- «помощник агента», «ассистент риелтора», «работаю с агентом»
- «ищу для клиента», «для клиента», «клиенту»

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ВАЖНЫЕ ПРАВИЛА:

1. «для себя», «ищу себе», «себе» — это ВСЕГДА approve_private, никаких исключений.
2. Если ответ двусмысленный или непонятный — approve_private (безопасный выбор).
3. Короткий ответ «себе» или «для себя» — approve_private, не анализируй дальше.
4. approve_agent ТОЛЬКО если человек прямо назвал себя риелтором или упомянул клиента.

{bio_block}

Ответ автора:
{text[:1000]}

Отвечай СТРОГО одним словом: approve_private или approve_agent."""

    return f"""Ты модератор доски объявлений по аренде и продаже недвижимости в Батуми (Грузия).

Твоя задача: определить тип поста и маршрут публикации.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ТОЧНО ЧАСТНЫЙ → approve_private

Человек явно ищет жильё для себя — есть хотя бы один личный маркер:

• Описывает себя или свою ситуацию: «мы с женой», «я с семьёй»,
  «без вредных привычек», «не курю», «работаем удалённо»,
  «для себя», «для нас», «переезжаю», «приезжаю», «прилетаю» 

• Пишет от первого лица про личные обстоятельства: «работаю в батуми», «учусь здесь»,
  «нас двое», «я один», «живу один»

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ТОЧНО АГЕНТ → approve_agent

Риелтор или агент — есть хотя бы один профессиональный маркер:

• Явный поиск для клиента: «для клиента», «клиенту», «под клиента», «сниму клиенту»,
  «ищу для клиента», «помощник агента», «ассистент риелтора», «работаю с агентом»
  
• Профессиональная идентификация: «риелтор», «агент», «broker», «estate», «realty»,
  «управляющая компания», «агентство»

• Профессиональные формулировки: «готов к сотрудничеству», «открыт к сотрудничеству»,
  «сотрудничаю с собственниками», «гарантирую быструю сдачу»,
  «работаю по договору с описью», «договор + опись», «сотрудничаю»

• Ищет несколько объектов с разными бюджетами: «1+1 за 600$, 2+1 за 900$»

• В bio упоминается риелторская деятельность: «риелтор», «агент по недвижимости»,
  «помогу с арендой», «подбор недвижимости», «broker», «estate», «realty»

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
НУЖНО УТОЧНИТЬ → ask_author

Пост выглядит как запрос жилья, но нет ни личных, ни агентских маркеров.
Нейтральный пост — невозможно определить кто автор:
  Примеры:
  «Ищу 1+1 в аренду, бюджет 600$» → ask_author
  «Сниму студию в центре на год» → ask_author
  «#сниму Ищу дом в Гонио с 14 по 26 числа» → ask_author
  «нужна студия на сутки» → ask_author
  «нужна квартира» → ask_author

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ОТКЛОНИТЬ → skip

• Объявление о СДАЧЕ или ПРОДАЖЕ: «сдаю», «продаю», «сдаётся», «есть варианты»
• Реклама агентства без конкретного запроса на поиск жилья
• Не связано с недвижимостью: вакансии, услуги, личные вопросы
• Ищет соседа или подселение
• Туристические вопросы без запроса жилья
• Флуд, жалобы, поздравления

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ВАЖНЫЕ ПРАВИЛА:

1. Есть личный маркер → approve_private, даже если пост короткий.
2. Есть агентский маркер → approve_agent (приоритет над личными деталями).
3. Нет ни тех ни других маркеров, но явный запрос жилья → ask_author.
4. «Сдаю», «сдаётся», «продаю» от владельца → skip.
5. Ищут в Батуми и окрестностях (Кабулети, Гонио, Квариати, Сарпи, Махинджаури,
   Букнари, Чакви, Цихисдзири) — одобряй.
6. Запросы на нескольких языках (русский + грузинский + английский) — одобряй.
7. Bio с риелторской деятельностью → approve_agent.

Скоринг системы: {score} (выше = больше ключевых слов поиска жилья)

{bio_block}

Отвечай СТРОГО одним словом: approve_private, approve_agent, ask_author или skip.

Текст сообщения:
{text[:2000]}"""


async def _ai_moderate(text: str, score: int, sender_bio: str = '', is_reply: bool = False) -> str:
    if not GEMINI_API_KEY:
        log.warning('[gemini] GEMINI_API_KEY не задан — fallback: approve_private')
        return 'approve_private'

    prompt = _build_prompt(text, score, sender_bio, is_reply=is_reply)
    loop   = asyncio.get_event_loop()

    def _call_gemini() -> str:
        url = (
            'https://generativelanguage.googleapis.com/v1beta/models/'
            f'gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}'
        )
        payload = json.dumps({
            'contents': [{'parts': [{'text': prompt}]}],
            'generationConfig': {
                'maxOutputTokens': 10,
                'temperature': 0,
                'thinkingConfig': {'thinkingBudget': 0},
            },
        }).encode('utf-8')
        req = urllib.request.Request(
            url, data=payload,
            headers={'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data      = json.loads(resp.read().decode('utf-8'))
                candidate = data.get('candidates', [{}])[0]
                parts     = candidate.get('content', {}).get('parts', [])
                answer    = ''
                for part in parts:
                    if part.get('type') == 'thought':
                        continue
                    text_val = part.get('text', '')
                    if text_val:
                        answer = text_val.strip().lower()
                        break
                log.debug(f'[gemini] raw answer: {repr(answer)}')
                if 'approve_agent'   in answer: return 'approve_agent'
                if 'approve_private' in answer: return 'approve_private'
                if answer == 'approve':          return 'approve_private'
                if 'ask_author'      in answer: return 'ask_author'
                if 'skip'            in answer: return 'skip'
                return 'approve_private'
        except urllib.error.HTTPError as e:
            body = ''
            try:
                body = e.read().decode('utf-8')
            except Exception:
                pass
            if e.code == 503:
                raise RuntimeError('GEMINI_503')
            log.warning(f'[gemini] HTTP {e.code}: {body[:200]} — fallback: moderation')
            return 'MODERATION_NEEDED'
        except Exception as e:
            log.warning(f'[gemini] ошибка: {e} — fallback: moderation')
            return 'MODERATION_NEEDED'

    for attempt in range(3):
        try:
            result = await loop.run_in_executor(_executor, _call_gemini)
            return result
        except RuntimeError as e:
            if 'GEMINI_503' in str(e):
                wait = (attempt + 1) * 10  # 10s, 20s, 30s
                log.warning(
                    f'[gemini] 503 — asyncio.sleep {wait}s '
                    f'(попытка {attempt + 1}/3)'
                )
                await asyncio.sleep(wait)
            else:
                raise

    log.warning('[gemini] все попытки исчерпаны → модерация')
    return 'MODERATION_NEEDED'

def _pick_dest_chat(ai_decision: str) -> str:
    if ai_decision == 'approve_agent' and state.get('dest_chat_id_agent'):
        return state['dest_chat_id_agent']
    return state['dest_chat_id']


# ══════════════════════════════════════════════════════════════════════════════
# ask_author — отправка вопроса автору и обработка ответа
# ══════════════════════════════════════════════════════════════════════════════

async def _do_publish(post: dict, client: TelegramClient, ss, acc: str, ai_decision: str):
    """Финальная публикация поста в нужный канал."""
    # ── Проверка списка риэлторов (переопределяет решение AI, кроме skip) ─
    user_id = post.get('user_id', 0)
    async with _state_lock:
        realtors = set(state.get('realtors', set()))
    if user_id and user_id in realtors and ai_decision not in ('approve_agent', 'skip'):
        log.info(f'[{acc}][риэлтор из списка →_do_publish] user_id={user_id} → approve_agent')
        ai_decision = 'approve_agent'

    post['ai_decision'] = ai_decision
    target = _pick_dest_chat(ai_decision)
    fp     = _post_fingerprint(post['text'], post['author_name'])

    refs   = post.get('grouped_refs', [])
    msgs   = await _fetch_messages_by_refs(client, refs) if refs else []
    photos = await _download_photos(client, msgs) if msgs else []

    published_fingerprints.append(fp)
    await _safe_sheets_retry(_write_post, ss, post)
    metrics['published'] += 1
    log.info(
        f'[✅ опубликовано AI:{ai_decision} acc:{acc}] '
        f'{post["chat_name"]} → {post["link"]} (канал: {target or "не задан"})'
    )
    if target:
        ok = await _publish_post(client, post, target, photos or None)
        if not ok:
            metrics['errors'] += 1


async def _ask_author_timeout(user_id: int):
    """Вызывается по таймауту — автор не ответил → публикуем как approve_private."""
    entry = pending_ask_author.pop(user_id, None)
    if not entry:
        return
    post   = entry['post']
    client = entry['client']
    ss     = entry['ss']
    acc    = entry['acc']

    log.info(f'[ask_author ⏱️ таймаут] user_id={user_id} → approve_private | {post["link"]}')
    await _safe_sheets(_update_ask_author_status, ss, user_id, 'таймаут', 'approve_private')
    await _do_publish(post, client, ss, acc, 'approve_private')

    async def _delayed_delete():
        await asyncio.sleep(30)
        try:
            await client.delete_dialog(user_id)
            log.info(f'[ask_author] диалог с user_id={user_id} удалён (таймаут)')
        except Exception as e:
            log.warning(f'[ask_author] не удалось удалить диалог user_id={user_id}: {e}')

    asyncio.ensure_future(_delayed_delete())


async def _send_ask_author_message(client: TelegramClient, user_id: int, post_link: str) -> bool:
    """
    Отправляет вопрос автору в личку через Telethon userbot.
    Возвращает True если успешно, False если личка закрыта.
    """
    text = (
        f'Добрый день, увидел ваш пост {post_link}. '
        f'Вы риелтор или ищете для себя?'
    )
    try:
        await client.send_message(user_id, text)
        return True
    except (UserIsBlockedError, ChatWriteForbiddenError,
            InputUserDeactivatedError, ValueError) as e:
        log.info(f'[ask_author] Личка закрыта user_id={user_id}: {e}')
        return False
    except FloodWaitError as e:
        log.warning(f'[ask_author] FloodWait {e.seconds}s при отправке user_id={user_id}')
        await asyncio.sleep(e.seconds + 2)
        try:
            await client.send_message(user_id, text)
            return True
        except Exception as e2:
            log.warning(f'[ask_author] Повторная ошибка user_id={user_id}: {e2}')
            return False
    except Exception as e:
        log.warning(f'[ask_author] Ошибка отправки user_id={user_id}: {e}')
        return False


async def _handle_author_reply(reply_text: str, user_id: int):
    """
    Обрабатывает ответ автора на наш вопрос.
    Вызывается из хендлера входящих личных сообщений.
    """
    entry = pending_ask_author.get(user_id)
    if not entry:
        return

    # Отменяем таймаут
    handle = entry.get('timeout_handle')
    if handle:
        handle.cancel()

    pending_ask_author.pop(user_id, None)

    post       = entry['post']
    client     = entry['client']
    ss         = entry['ss']
    acc        = entry['acc']
    sender_bio = entry.get('sender_bio', '')

    log.info(f'[ask_author 💬 ответ] user_id={user_id}: {repr(reply_text[:100])}')

    # Прогоняем ответ через AI
    ai_decision = await _ai_moderate(reply_text, 0, sender_bio, is_reply=True)

    # Если AI не определил однозначно — в частные (безопасный fallback)
    if ai_decision not in ('approve_private', 'approve_agent'):
        ai_decision = 'approve_private'

    log.info(f'[ask_author AI:{ai_decision}] user_id={user_id} → {post["link"]}')
    if ai_decision == 'approve_agent':
        await _safe_sheets_retry(_add_realtor_to_sheet, ss, post, user_id)
        async with _state_lock:
            state['realtors'].add(user_id)
        log.info(f'[ask_author] user_id={user_id} добавлен в риэлторы')
      
    await _safe_sheets(_update_ask_author_status, ss, user_id, 'ответил', ai_decision)
    await _do_publish(post, client, ss, acc, ai_decision)

    async def _delayed_delete():
        await asyncio.sleep(30)
        try:
            await client.delete_dialog(user_id)
            log.info(f'[ask_author] диалог с user_id={user_id} удалён (после ответа)')
        except Exception as e:
            log.warning(f'[ask_author] не удалось удалить диалог user_id={user_id}: {e}')

    asyncio.ensure_future(_delayed_delete())

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
                log.error(f'TG API {method} не ок: {result.get("description")} '
                          f'(error_code={result.get("error_code")})')
            return result
    except urllib.error.HTTPError as e:
        if e.code in (409, 401, 403):
            try:
                body = json.loads(e.read().decode('utf-8'))
            except Exception:
                body = {}
            return {'ok': False, 'error_code': e.code,
                    'description': body.get('description', str(e))}
        log.error(f'TG API {method} HTTP {e.code}: {e}', exc_info=True)
        return {}
    except Exception as e:
        log.error(f'TG API {method} error: {e}', exc_info=True)
        return {}


def _get_updates(token: str, offset: int, timeout: int = 30) -> list:
    result = _tg_request(token, 'getUpdates', {
        'offset':  offset,
        'timeout': timeout,
        'allowed_updates': ['callback_query'],
    }, timeout=timeout + 10)
    if not result.get('ok'):
        error_code  = result.get('error_code', 0)
        description = result.get('description', '')
        if error_code == 409 or 'Conflict' in description:
            raise RuntimeError(f'409:{description}')
        if error_code in (401, 403):
            raise RuntimeError(f'{error_code}:{description}')
        return []
    return result.get('result', [])


def _send_moderation_card(post: dict, token: str, moderator_chat_id: str) -> int:
    author_str = '—'
    if post['author_name']:
        if post['author_link']:
            author_str = f'<a href="{post["author_link"]}">{post["author_name"]}</a>'
        else:
            author_str = post['author_name']

    pend_key = f'{post["src_chat_id"]}:{post["src_msg_id"]}'
    lines = [
        f'📢 <b>Источник:</b> {post["chat_name"]}',
        f'👤 <b>Автор:</b> {author_str}',
        f'🏆 <b>Скор:</b> {post["score"]}',
        '',
        post['html_text'][:3500],
        '',
        f'🔗 <a href="{post["link"]}">Открыть сообщение</a>',
    ]
    result = _tg_request(token, 'sendMessage', {
        'chat_id':    moderator_chat_id,
        'text':       '\n'.join(lines)[:4096],
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
        'reply_markup': {
            'inline_keyboard': [[
                {'text': '👤 Частный',    'callback_data': f'approve_private:{pend_key}'},
                {'text': '🏢 Агент',      'callback_data': f'approve_agent:{pend_key}'},
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


def _build_caption(post: dict) -> str:
    chat_name   = post.get('chat_name', '') or 'Источник'
    link        = post['link']
    author_name = post.get('author_name', '').strip()
    author_link = post.get('author_link', '').strip()
    source_line = f'Источник: <a href="{link}">{chat_name}</a>'
    if author_name and author_link:
        author_line = f'Автор: <a href="{author_link}">{author_name}</a>'
    elif author_name:
        author_line = f'Автор: {author_name}'
    else:
        author_line = ''
    text = post['text']
    header = source_line + '\n' + author_line if author_line else source_line
    max_text = 4096 - len(header) - 3
    if len(text) > max_text:
        text = text[:max_text].rstrip() + '…'
    if author_line:
        return f'{author_line}\n{source_line}\n\n{text}'
    return f'{source_line}\n\n{text}'


# ── Bot API helpers для отправки медиа ────────────────────────────────────────

def _bot_request_raw(url: str, data: bytes, content_type: str,
                     timeout: int = 30, label: str = '') -> dict:
    req = urllib.request.Request(url, data=data, headers={'Content-Type': content_type})
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
                log.error(f'[{label}] Bot API HTTP {e.code}: {e}', exc_info=True)
                return {}
        except Exception as e:
            log.error(f'[{label}] Bot API error: {e}', exc_info=True)
            return {}
    return {}


def _build_multipart(fields: dict, files: dict) -> tuple[bytes, str]:
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
    url = f'https://api.telegram.org/bot{token}/sendPhoto'
    body, ct = _build_multipart(
        fields={'chat_id': chat_id, 'caption': caption[:1024], 'parse_mode': 'HTML'},
        files={'photo': ('photo.jpg', photo, 'image/jpeg')},
    )
    _bot_request_raw(url, body, ct, timeout=30, label='sendPhoto')
    time.sleep(0.3)


def _send_album_bot(token: str, chat_id: str, caption: str, photos: list[bytes]):
    photos = photos[:10]
    url = f'https://api.telegram.org/bot{token}/sendMediaGroup'
    media_json = []
    for i in range(len(photos)):
        item: dict = {'type': 'photo', 'media': f'attach://p{i}'}
        if i == 0:
            item['caption'] = caption[:1024]
            item['parse_mode'] = 'HTML'
        media_json.append(item)
    fields = {'chat_id': chat_id, 'media': json.dumps(media_json)}
    files  = {f'p{i}': (f'p{i}.jpg', pb, 'image/jpeg') for i, pb in enumerate(photos)}
    body, ct = _build_multipart(fields, files)
    _bot_request_raw(url, body, ct, timeout=60, label='sendMediaGroup')
    time.sleep(0.3)


async def _download_photos(client: TelegramClient, messages: list) -> list[bytes]:
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
            log.warning(f'download_media error msg_id={m.id}: {e}', exc_info=True)
    return photos


async def _fetch_messages_by_refs(client: TelegramClient, refs: list[tuple]) -> list:
    msgs = []
    for chat_id, msg_id in refs:
        try:
            msg = await client.get_messages(chat_id, ids=msg_id)
            if msg:
                msgs.append(msg)
        except Exception as e:
            log.warning(f'Не удалось загрузить сообщение {chat_id}/{msg_id}: {e}', exc_info=True)
    return msgs


def _is_image_doc(msg) -> bool:
    doc = getattr(msg, 'document', None)
    if not doc:
        return False
    mime = getattr(doc, 'mime_type', '') or ''
    return mime.startswith('image/')


async def _publish_post(client: TelegramClient, post: dict,
                        dest_chat: str, photos: list[bytes] | None = None):
    caption = _build_caption(post)
    token   = state.get('tg_token', '')
    loop    = asyncio.get_event_loop()
    try:
        if photos and token:
            if len(photos) == 1:
                await loop.run_in_executor(_executor, _send_photo_bot,
                                           token, dest_chat, caption, photos[0])
            else:
                await loop.run_in_executor(_executor, _send_album_bot,
                                           token, dest_chat, caption, photos)
        elif photos and not token:
            await client.send_file(
                entity=int(dest_chat),
                file=photos if len(photos) > 1 else photos[0],
                caption=caption,
                parse_mode='html',
            )
        else:
            await client.send_message(
                entity=int(dest_chat),
                message=caption,
                parse_mode='html',
                link_preview=True,
            )
        return True
    except Exception as e:
        log.error(f'Ошибка публикации поста: {e}', exc_info=True)
        return False


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
            log.error(f'[{acc_name}] Ошибка резолва {username}: {e}', exc_info=True)
        await asyncio.sleep(0.5)

    msg = (f'🚫 Канал недоступен: @{username}\n'
           + '\n'.join(f'{k}: {v}' for k, v in errors.items()))
    log.error(msg)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, _send_alert,
                               state['tg_token'], state['moderator_chat_id'], msg)
    await _safe_sheets(_set_channel_status, ss, username, 'недоступен')
    await _safe_sheets(_write_log, ss, 'ERROR', msg)
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
    async with _state_lock:
        state['watched_ids'] = new_ids
        state['id_to_meta']  = new_id_meta

    log.info(f'Каналов в watched_ids: {len(new_ids)} (ключей в id_to_meta: {len(new_id_meta)})')
    if added:   log.info(f'Добавлено ID-ключей: {len(added)}')
    if removed: log.info(f'Убрано ID-ключей: {len(removed)}')
    sample = sorted(new_id_meta.keys())[:6]
    log.info(f'Пример ключей id_to_meta: {sample}')
    await _safe_sheets(_write_entity_cache, ss)


# ══════════════════════════════════════════════════════════════════════════════
# Главная функция обработки поста
# ══════════════════════════════════════════════════════════════════════════════

async def _process_and_publish(
    post: dict,
    client: TelegramClient,
    ask_client: TelegramClient,
    msgs_for_photos: list,
    ss,
    acc: str,
):
    text        = post['text']
    author_name = post['author_name']
    chat_name   = post['chat_name']
    score       = post['score']
    link        = post['link']
    user_id     = post.get('user_id', 0)

    # ── 1. Дедупликация: уже опубликован ──────────────────────────────────
    fp = _post_fingerprint(text, author_name)
    if fp in published_fingerprints:
        log.info(f'[{acc}][дубль опубликован ⛔] {chat_name}')
        return

    # ── 2. Дедупликация: уже отклонён AI ──────────────────────────────────
    fp_anon = _post_fingerprint(text, '')
    if fp_anon in ai_rejected_fingerprints:
        log.info(f'[{acc}][дубль AI-rejected ⛔] {chat_name}')
        return

    # ── 3. Bio автора ──────────────────────────────────────────────────────
    sender_bio = ''
    try:
        source_msg = msgs_for_photos[0] if msgs_for_photos else None
        if source_msg:
            sender_bio = await _get_sender_bio(client, source_msg)
            if sender_bio:
                log.info(f'[{acc}][bio] {sender_bio[:80]}')
    except Exception as e:
        log.warning(f'[{acc}] Не удалось получить bio: {e}')

    # ── 4. AI-модерация ────────────────────────────────────────────────────
    ai_decision = await _ai_moderate(text, score, sender_bio)

    # ── Проверка списка риэлторов (skip не переопределяем) ─────────────────
    async with _state_lock:
        realtors = set(state.get('realtors', set()))
    if user_id and user_id in realtors and ai_decision != 'skip':
        if ai_decision != 'approve_agent':
            log.info(f'[{acc}][риэлтор из списка] user_id={user_id} '
                     f'{ai_decision} → approve_agent')
        ai_decision = 'approve_agent'

    log.info(f'[{acc}][AI:{ai_decision} скор:{score}] {chat_name} → {link}')

    post['ai_decision'] = ai_decision

    # ── 5. Маршрутизация ───────────────────────────────────────────────────

    if ai_decision == 'skip':
        ai_rejected_fingerprints.append(fp_anon)
        await _safe_sheets_retry(_write_ai_rejected, ss, post, ai_decision)
        return

    if ai_decision in ('approve_private', 'approve_agent'):
        await _do_publish(post, client, ss, acc, ai_decision)
        return

    if ai_decision == 'ask_author':
        if not user_id:
            log.info(f'[{acc}][ask_author] user_id неизвестен → approve_private')
            await _do_publish(post, client, ss, acc, 'approve_private')
            return

        if user_id in pending_ask_author:
            log.info(f'[{acc}][ask_author] user_id={user_id} уже в ожидании → пропуск')
            return

        sent = await _send_ask_author_message(ask_client, user_id, link)

        if not sent:
            log.info(f'[{acc}][ask_author] личка закрыта user_id={user_id} → approve_private')
            await _do_publish(post, client, ss, acc, 'approve_private')
            return

        timeout_handle = asyncio.get_event_loop().call_later(
            ASK_AUTHOR_TIMEOUT_SEC,
            lambda uid=user_id: asyncio.ensure_future(_ask_author_timeout(uid)),
        )
        pending_ask_author[user_id] = {
            'post':           post,
            'client':         ask_client,
            'ss':             ss,
            'acc':            acc,
            'sender_bio':     sender_bio,
            'added_at':       time.time(),
            'timeout_handle': timeout_handle,
        }
        metrics['asked'] += 1
        log.info(
            f'[{acc}][ask_author ✉️] user_id={user_id} | '
            f'ожидаем {ASK_AUTHOR_TIMEOUT_SEC}s | {link}'
        )
        await _safe_sheets_retry(_write_ask_author, ss, post, user_id)
        return

    if ai_decision == 'MODERATION_NEEDED':
        async with _state_lock:
            token     = state['tg_token']
            moderator = state['moderator_chat_id']
        if token and moderator:
            pend_key = f'{post["src_chat_id"]}:{post["src_msg_id"]}'
            post['added_at'] = time.time()
            pending_moderation[pend_key] = post
            loop = asyncio.get_event_loop()
            bot_message_id = await loop.run_in_executor(
                _executor, _send_moderation_card, post, token, moderator
            )
            post['bot_message_id'] = bot_message_id
            await _safe_sheets_retry(_write_rejected, ss, post, bot_message_id)
            metrics['moderated'] += 1
            log.info(
                f'[{acc}][gemini недоступен → модерация] '
                f'{post["chat_name"]} → {post["link"]}'
            )
        else:
            log.warning(
                f'[{acc}][gemini недоступен, модератор не задан → skip] '
                f'{post["link"]}'
            )
        return

    # Неизвестный ответ AI — approve_private
    log.warning(f'[{acc}][AI неизвестный ответ: {ai_decision}] → approve_private')
    await _do_publish(post, client, ss, acc, 'approve_private')


# ══════════════════════════════════════════════════════════════════════════════
# Фоновые задачи
# ══════════════════════════════════════════════════════════════════════════════

async def _settings_reload_loop(clients: dict, ss):
    while True:
        await asyncio.sleep(SETTINGS_RELOAD_SEC)
        try:
            log.info('Перезагрузка настроек...')
            new_settings = await _safe_sheets_result(_read_settings,      ss)
            new_rules    = await _safe_sheets_result(_read_scoring_rules, ss)
            new_minus    = await _safe_sheets_result(_read_minus_words,   ss)
            resolved_r, to_resolve_r = await _safe_sheets_result(_read_realtors_raw, ss)
            new_realtors = await _resolve_realtors(ss, clients, resolved_r, to_resolve_r)
            new_channels = await _safe_sheets_result(_read_channels,      ss)

            if new_settings:
                async with _state_lock:
                    state.update({
                        'tg_token':             new_settings['tg_token'],
                        'score_threshold':      new_settings['score_threshold'],
                        'moderation_threshold': new_settings['moderation_threshold'],
                        'min_length':           new_settings['min_length'],
                        'moderator_chat_id':    new_settings['moderator_chat_id'],
                        'dest_chat_id':         new_settings['dest_chat_id'],
                        'dest_chat_id_agent':   new_settings.get('dest_chat_id_agent', ''),
                        'excluded_accounts':    _parse_excluded_accounts(new_settings.get('excluded_accounts', '')),
                    })
            if new_rules  is not None:
                async with _state_lock: state['scoring_rules'] = new_rules
            if new_minus  is not None:
                async with _state_lock: state['minus_words']   = new_minus
            if new_realtors  is not None:
                async with _state_lock: state['realtors']      = new_realtors
            if new_channels is not None:
                await _update_watched_chats(clients, new_channels, ss)

            log.info(
                f'Настройки применены | каналов: {len(state["watched_ids"])} | '
                f'правил: {len(state["scoring_rules"])} | '
                f'минус-слов: {len(state["minus_words"])} | '
                f'порог публикации: {state["score_threshold"]} | '
                f'порог модерации: {state["moderation_threshold"]} | '
                f'канал частных: {state["dest_chat_id"]} | '
                f'канал агентов: {state["dest_chat_id_agent"]}'
            )
        except Exception as e:
            log.error('Ошибка перезагрузки настроек: ' + str(e), exc_info=True)


async def _cleanup_pending_loop():
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = time.time() - 86400
            stale  = [k for k, v in pending_moderation.items()
                      if v.get('added_at', 0) < cutoff]
            for k in stale:
                pending_moderation.pop(k, None)
            if stale:
                log.info(f'Cleanup: удалено {len(stale)} устаревших pending-постов')
        except Exception as e:
            log.error(f'Ошибка cleanup pending: {e}', exc_info=True)


async def _heartbeat_loop():
    while True:
        await asyncio.sleep(300)
        log.info(
            f'[heartbeat] processed:{metrics["processed"]} '
            f'published:{metrics["published"]} '
            f'asked:{metrics["asked"]} '
            f'moderated:{metrics["moderated"]} '
            f'errors:{metrics["errors"]} '
            f'pending_mod:{len(pending_moderation)} '
            f'pending_ask:{len(pending_ask_author)}'
        )


async def _bot_polling_loop(clients: dict, ss):
    offset = 0
    loop   = asyncio.get_event_loop()

    def _first_client() -> TelegramClient | None:
        return next(iter(clients.values()), None)

    token = state['tg_token']
    if token:
        for attempt in range(10):
            result = await loop.run_in_executor(
                _executor, _tg_request, token, 'getUpdates',
                {'offset': 0, 'timeout': 1, 'allowed_updates': ['callback_query']}, 15,
            )
            if result.get('ok'):
                log.info('[bot_polling] Слот getUpdates свободен — стартуем')
                break
            if result.get('error_code') == 409 or 'Conflict' in result.get('description', ''):
                wait = min(15 * (attempt + 1), 60)
                log.warning(f'[bot_polling] 409 при старте (попытка {attempt + 1}) — жду {wait}с')
                await asyncio.sleep(wait)
            else:
                break

    while True:
        async with _state_lock:
            token     = state['tg_token']
            moderator = state['moderator_chat_id']

        if not token:
            await asyncio.sleep(5)
            continue

        try:
            updates = await loop.run_in_executor(_executor, _get_updates, token, offset, 30)
        except RuntimeError as e:
            err_str = str(e)
            if err_str.startswith('409:'):
                log.warning('[bot_polling] 409 Conflict в цикле — жду 15с + deleteWebhook')
                await asyncio.sleep(15)
                await loop.run_in_executor(
                    _executor, _tg_request,
                    token, 'deleteWebhook', {'drop_pending_updates': False}
                )
            elif err_str.startswith('401:') or err_str.startswith('403:'):
                log.critical(f'[bot_polling] Неверный токен бота ({err_str}) — останавливаемся')
                return
            else:
                log.error(f'[bot_polling] getUpdates RuntimeError: {e}', exc_info=True)
                await asyncio.sleep(5)
            continue
        except Exception as e:
            log.error(f'[bot_polling] getUpdates error: {e}', exc_info=True)
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

            parts = data.split(':', 2)
            if len(parts) != 3 or parts[0] not in ('approve_private', 'approve_agent', 'skip'):
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
                await loop.run_in_executor(
                    _executor, _edit_message_reply_markup,
                    token, moderator, msg_id, '⚠️ Пост не найден в очереди'
                )
                continue

            if action in ('approve_private', 'approve_agent'):
                client = _first_client()
                async with _state_lock:
                    dest_private = state['dest_chat_id']
                    dest_agent   = state.get('dest_chat_id_agent', '')
                target = dest_agent if (action == 'approve_agent' and dest_agent) else dest_private

                if client and target:
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

                        refs         = post.get('grouped_refs', [])
                        grouped_msgs = await _fetch_messages_by_refs(client, refs) if refs else []
                        photos       = await _download_photos(client, grouped_msgs) if grouped_msgs else []

                        post['ai_decision'] = action
                        ok = await _publish_post(client, post, target, photos or None)

                        if ok:
                            published_fingerprints.append(fp)
                            metrics['published'] += 1
                            label = '👤 частный' if action == 'approve_private' else '🏢 агент'
                            log.info(
                                f'[модерация ✅ {label} фото:{len(photos)}] '
                                f'{post["chat_name"]} → {post["link"]}'
                            )
                            await _safe_sheets_retry(_write_post, ss, post)
                            await _safe_sheets(_update_rejected_status, ss,
                                               post.get('bot_message_id', 0), f'одобрено ({label})')
                            await loop.run_in_executor(
                                _executor, _answer_callback, token, cq_id,
                                f'✅ Опубликовано ({label})!'
                            )
                            await loop.run_in_executor(
                                _executor, _edit_message_reply_markup,
                                token, moderator, msg_id,
                                f'✅ {label.capitalize()} — опубликовано модератором {from_id}'
                            )
                        else:
                            metrics['errors'] += 1
                            await loop.run_in_executor(
                                _executor, _answer_callback, token, cq_id,
                                '❌ Ошибка публикации — смотрите логи'
                            )
                    except Exception as e:
                        metrics['errors'] += 1
                        log.error(f'[модерация] Ошибка: {e}', exc_info=True)
                        await loop.run_in_executor(
                            _executor, _answer_callback, token, cq_id, f'❌ Ошибка: {e}'
                        )
                else:
                    await loop.run_in_executor(
                        _executor, _answer_callback, token, cq_id,
                        '⚠️ Нет клиента или целевого канала'
                    )

            elif action == 'skip':
                log.info(f'[модерация ❌ пропущено] {post["chat_name"]} → {post["link"]}')
                await _safe_sheets(_update_rejected_status, ss,
                                   post.get('bot_message_id', 0), 'пропущено')
                await loop.run_in_executor(
                    _executor, _answer_callback, token, cq_id, '❌ Пост пропущен'
                )
                await loop.run_in_executor(
                    _executor, _edit_message_reply_markup,
                    token, moderator, msg_id,
                    f'❌ Пропущено модератором {from_id}'
                )

            pending_moderation.pop(pend_key, None)

        await asyncio.sleep(0)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    global _sheets_lock, _state_lock
    log.info('═══ TG Parser v4 стартует ═══')
    await asyncio.sleep(45)

    _sheets_lock = asyncio.Lock()
    _state_lock  = asyncio.Lock()

    loop = asyncio.get_event_loop()

    # ── Google Sheets ──────────────────────────────────────────────────────
    try:
        ss = await loop.run_in_executor(_executor, _get_spreadsheet)
        log.info('Google Sheets: подключён')
    except Exception as e:
        log.error('Google Sheets: ошибка подключения: ' + str(e), exc_info=True)
        return

    settings = await _safe_sheets_result(_read_settings,      ss)
    rules    = await _safe_sheets_result(_read_scoring_rules, ss)
    minus    = await _safe_sheets_result(_read_minus_words,   ss)
    resolved_r, to_resolve_r = await _safe_sheets_result(_read_realtors_raw, ss)
    channels = await _safe_sheets_result(_read_channels,      ss)

    # ── Загрузка fingerprints из обоих листов ──────────────────────────────
    initial_fps  = await _safe_sheets_result(_load_published_fingerprints,    ss)
    rejected_fps = await _safe_sheets_result(_load_ai_rejected_fingerprints,  ss)
    for fp in initial_fps:
        published_fingerprints.append(fp)
    for fp in rejected_fps:
        ai_rejected_fingerprints.append(fp)
    log.info(
        f'Дедупликация: {len(initial_fps)} из «Посты», '
        f'{len(rejected_fps)} из «Отклонено ИИ»'
    )

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
        'dest_chat_id_agent':   settings.get('dest_chat_id_agent', ''),
        'scoring_rules':        rules,
        'minus_words':          minus,
        'excluded_accounts':    _parse_excluded_accounts(settings.get('excluded_accounts', '')),
    })
    log.info(
        f'Настройки загружены | '
        f'порог публикации: {state["score_threshold"]} | '
        f'порог модерации: {state["moderation_threshold"]} | '
        f'мин.длина: {state["min_length"]} | '
        f'правил: {len(rules)} | минус-слов: {len(minus)} | '
        f'канал частных: {state["dest_chat_id"]} | '
        f'канал агентов: {state["dest_chat_id_agent"] or "не задан"} | '
        f'таймаут ask_author: {ASK_AUTHOR_TIMEOUT_SEC}s'
        f'риэлторов: {len(state["realtors"])} | '
    )

    if GEMINI_API_KEY:
        log.info('Gemini AI: ключ задан, AI-модерация активна (4 класса)')
    else:
        log.warning('Gemini AI: GEMINI_API_KEY не задан — fallback: approve_private')

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
    # ── Выбор аккаунта для ask_author ─────────────────────────────────────
    ask_client = clients.get(ASK_AUTHOR_ACCOUNT) or next(iter(clients.values()))
    ask_acc_label = ASK_AUTHOR_ACCOUNT if ASK_AUTHOR_ACCOUNT in clients else next(iter(clients.keys()))
    log.info(f'ask_author аккаунт: {ask_acc_label}')
  
    # ── Резолв риэлторов (требует готовых clients) ─────────────────────────
    realtors = await _resolve_realtors(ss, clients, resolved_r, to_resolve_r)
    state['realtors'] = realtors

    # ── Резолв каналов ─────────────────────────────────────────────────────
    cached_meta = await _safe_sheets_result(_read_entity_cache, ss)
    state['username_to_meta'].update(cached_meta)

    if not channels:
        log.warning('Лист «Каналы» пуст — добавьте каналы')
    else:
        known = sum(1 for ch in channels if _extract_username(ch['username']) in cached_meta)
        log.info(f'Каналов: {len(channels)} | из кеша: {known} | новых: {len(channels) - known}')

    await _update_watched_chats(clients, channels or [], ss)
    log.info(
        f'Слежу за {len(state["watched_ids"])} ID-ключами '
        f'({len(state["username_to_meta"])} каналов)'
    )

    # ══════════════════════════════════════════════════════════════════════
    # Буфер альбомов и хендлеры
    # ══════════════════════════════════════════════════════════════════════

    album_buffer: dict = {}

    async def _flush_album(grouped_id: int, _acc: str, _client: TelegramClient, _ask_client: TelegramClient):
        entry = album_buffer.pop(grouped_id, None)
        if not entry:
            return
        try:
            msgs  = sorted(entry['msgs'], key=lambda m: m.id)
            first = msgs[0]

            try:
                raw_id = first.chat_id
            except Exception:
                raw_id = -(getattr(first.peer_id, 'channel_id', 0))

            abs_id = abs(raw_id)

            async with _state_lock:
                id_to_meta    = dict(state['id_to_meta'])
                minus_words   = list(state['minus_words'])
                scoring_rules = list(state['scoring_rules'])
                min_length    = state['min_length']
                mod_threshold = state['moderation_threshold']

            meta = _meta_by_abs_id(id_to_meta, abs_id)
            if meta is None:
                log.info(f'[{_acc}][альбом] abs_id={abs_id} не в списке каналов — пропуск')
                return

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
            chat_name = meta.get('chat_name', str(abs_id))

            minus_hit = _find_minus_word(text, minus_words)
            if minus_hit:
                log.info(f'[{_acc}][альбом минус "{minus_hit}"] {chat_name}')
                return
            if len(text) < min_length:
                log.info(f'[{_acc}][альбом короткий {len(text)}<{min_length}] {chat_name}')
                return

            score = _calc_score(text, scoring_rules)
            if score < mod_threshold:
                log.info(f'[{_acc}][альбом скор:{score}<{mod_threshold}] {chat_name}')
                return

            try:
                chat = await _client.get_entity(raw_id)
            except Exception:
                chat = None

            link = _build_link(chat, first.id) if chat else f'https://t.me/c/{abs_id}/{first.id}'
            author_name, author_link, user_id = _get_author_info(first)
            async with _state_lock:
                excluded = set(state.get('excluded_accounts', set()))
            if user_id and user_id in excluded:
                log.info(f'[{_acc}][альбом исключён] user_id={user_id} {chat_name}')
                return

            post = {
                'date':         first.date.replace(tzinfo=None),
                'chat_name':    chat_name,
                'author_name':  author_name,
                'author_link':  author_link,
                'user_id':      user_id,
                'link':         link,
                'text':         text,
                'html_text':    html_text,
                'score':        score,
                'account':      _acc,
                'src_chat_id':  raw_id,
                'src_msg_id':   first.id,
                'grouped_refs': [(m.chat_id, m.id) for m in msgs],
                'added_at':     time.time(),
            }

            metrics['processed'] += 1
            await _process_and_publish(post, _client, _ask_client, msgs, ss, _acc)
          
        except Exception as e:
            metrics['errors'] += 1
            log.error(f'_flush_album error grouped_id={grouped_id}: {e}', exc_info=True)

    for acc_label, client in clients.items():

        @client.on(events.NewMessage)
        async def _on_new_message(event, _acc=acc_label, _client=client):
            try:
                msg = event.message

                if isinstance(msg, (MessageService, MessageEmpty)):
                    return
                if getattr(msg, 'action', None) is not None:
                    return

                raw_id = event.chat_id
                abs_id = abs(raw_id)

                async with _state_lock:
                    id_to_meta    = dict(state['id_to_meta'])
                    minus_words   = list(state['minus_words'])
                    scoring_rules = list(state['scoring_rules'])
                    min_length    = state['min_length']
                    mod_threshold = state['moderation_threshold']

                meta = _meta_by_abs_id(id_to_meta, abs_id)
                if meta is None:
                    return

                dedup_key = (raw_id, msg.id)
                if dedup_key in seen_ids:
                    return
                seen_ids.append(dedup_key)

                chat_name = meta.get('chat_name', str(abs_id))

                # ── Альбом ─────────────────────────────────────────────────
                grouped_id = getattr(msg, 'grouped_id', None)
                if grouped_id:
                    if grouped_id not in album_buffer:
                        handle = asyncio.get_event_loop().call_later(
                            1.5, lambda gid=grouped_id: asyncio.ensure_future(
                                _flush_album(gid, _acc, _client, ask_client)
                            )
                        )
                        album_buffer[grouped_id] = {
                            'msgs': [], 'timer': handle,
                            'acc': _acc, 'client': _client,
                        }
                    album_buffer[grouped_id]['msgs'].append(msg)
                    return

                # ── Одиночное сообщение ────────────────────────────────────
                text = msg.text or getattr(msg, 'message', '') or ''
                if hasattr(msg, 'caption') and msg.caption:
                    text = msg.caption
                text = re.sub(r'[^\S\n]+', ' ', text).strip()
                html_text = _text_to_html(text, msg.entities)

                minus_hit = _find_minus_word(text, minus_words)
                if minus_hit:
                    log.info(f'[{_acc}][минус "{minus_hit}"] {chat_name} | {repr(text[:80])}')
                    return

                if len(text) < min_length:
                    log.info(f'[{_acc}][короткий {len(text)}<{min_length}] {chat_name}')
                    return

                score = _calc_score(text, scoring_rules)
                if score < mod_threshold:
                    log.info(f'[{_acc}][скор:{score}<{mod_threshold}] {chat_name} | {repr(text[:60])}')
                    return

                chat = await event.get_chat()
                link = _build_link(chat, msg.id)
                author_name, author_link, user_id = _get_author_info(msg)
                async with _state_lock:
                    excluded = set(state.get('excluded_accounts', set()))
                if user_id and user_id in excluded:
                    log.info(f'[{_acc}][исключён] user_id={user_id} {chat_name}')
                    return

                log.info(f'[{_acc}][принят скор:{score}] {chat_name} → {link}')

                post = {
                    'date':         msg.date.replace(tzinfo=None),
                    'chat_name':    chat_name,
                    'author_name':  author_name,
                    'author_link':  author_link,
                    'user_id':      user_id,
                    'link':         link,
                    'text':         text,
                    'html_text':    html_text,
                    'score':        score,
                    'account':      _acc,
                    'src_chat_id':  raw_id,
                    'src_msg_id':   msg.id,
                    'grouped_refs': [(msg.chat_id, msg.id)],
                    'added_at':     time.time(),
                }

                metrics['processed'] += 1
                await _process_and_publish(post, _client, ask_client, [msg], ss, _acc)

            except Exception as e:
                metrics['errors'] += 1
                log.error(f'[{_acc}] Ошибка обработки сообщения: {e}', exc_info=True)

        log.info(f'[{acc_label}] Хендлер зарегистрирован')
      
    # ── Хендлер личных сообщений только на ask_client ─────────────────────
    @ask_client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
    async def _on_private_reply(event):
        try:
            msg = event.message
            if msg.out:
                return
            sender_id = event.sender_id
            if sender_id and sender_id in pending_ask_author:
                await _handle_author_reply(msg.text or '', sender_id)
        except Exception as e:
            log.error(f'[ask_author reply] Ошибка: {e}', exc_info=True)
    
    log.info(f'[{ask_acc_label}] Хендлер личных сообщений зарегистрирован')

    await _safe_sheets(_write_log, ss, 'INFO',
        f'Запущен v4 | аккаунтов: {len(clients)} | '
        f'каналов: {len(state["username_to_meta"])} | '
        f'правил: {len(state["scoring_rules"])} | '
        f'порог: {state["score_threshold"]} | '
        f'минус-слов: {len(state["minus_words"])} | '
        f'канал частных: {state["dest_chat_id"]} | '
        f'канал агентов: {state["dest_chat_id_agent"] or "не задан"}'
    )

    asyncio.create_task(_settings_reload_loop(clients, ss))
    asyncio.create_task(_bot_polling_loop(clients, ss))
    asyncio.create_task(_cleanup_pending_loop())
    asyncio.create_task(_heartbeat_loop())

    log.info(
        f'Слушаю события. '
        f'Настройки обновляются каждые {SETTINGS_RELOAD_SEC}с. '
        f'Bot polling запущен. '
        f'ask_author таймаут: {ASK_AUTHOR_TIMEOUT_SEC}s.'
    )

    try:
        await asyncio.gather(*[c.run_until_disconnected() for c in clients.values()])
    finally:
        for name, c in clients.items():
            try:
                await c.disconnect()
                log.info(f'{name}: отключён')
            except Exception:
                pass


if __name__ == '__main__':
    asyncio.run(main())
