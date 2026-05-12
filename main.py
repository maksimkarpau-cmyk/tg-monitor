"""
TG Parser v3 — два аккаунта, скоринг + модерация через inline-кнопки.

Архитектура:
  - Два TelegramClient (acc1, acc2) слушают одни и те же каналы
  - Первый резолв канала пробует acc1, если недоступен — acc2
  - Если канал недоступен обоим — алерт в бот + статус «недоступен» в таблице
  - Новые сообщения обрабатывает тот клиент, который получил событие
  - Дубли между аккаунтами гасятся через seen_ids (deque)

Фильтрация:
  1. Минус-слова  → выброс немедленно
  2. Мин. длина   → выброс
  3. Скоринг      → авто-отправка или карточка на модерацию

Модерацию (polling getUpdates) обрабатывает GAS-триггер.

Кэш резолва:
  - При старте читается из листа «Кэш» → entity_id не запрашиваются повторно
  - Каждый новый резолв дописывается в лист «Кэш»
  - Лист «Кэш»: колонки A=username, B=entity_id, C=chat_name
"""

import asyncio
import base64
import json
import logging
import os
import re
import time
import urllib.request
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

API_ID_1   = int(os.environ.get('TG_API_ID_1',  '0'))
API_HASH_1 = os.environ.get('TG_API_HASH_1', '')
SESSION_1  = os.environ.get('TG_SESSION_1',  '')

API_ID_2   = int(os.environ.get('TG_API_ID_2',  '0'))
API_HASH_2 = os.environ.get('TG_API_HASH_2', '')
SESSION_2  = os.environ.get('TG_SESSION_2',  '')

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
    'id_to_meta':        {},   # {abs_id: {chat_name, username}}
    'username_to_meta':  {},   # кэш резолва (in-memory)
}

# Дедупликация: храним последние 2000 (chat_id, msg_id) чтобы не писать дубли
# когда оба аккаунта получают одно и то же событие
seen_ids: deque = deque(maxlen=2000)

_executor = ThreadPoolExecutor(max_workers=4)

# Зарегистрированные хендлеры NewMessage (для перерегистрации при обновлении списка каналов)
_registered_handlers: dict = {}


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets — подключение
# ══════════════════════════════════════════════════════════════════════════════

def _get_spreadsheet():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds_json = json.loads(base64.b64decode(GOOGLE_CREDENTIALS_B64).decode('utf-8'))
    creds      = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc         = gspread.authorize(creds)
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
        data  = ss.worksheet('Скоринг').get_all_values()
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
    """
    Колонка D (Аккаунт) игнорируется — оба аккаунта слушают все каналы.
    """
    try:
        data   = ss.worksheet('Каналы').get_all_values()
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


def _read_cache(ss) -> dict:
    """
    Читает кэш резолва из листа «Кэш».
    Формат: A=username, B=entity_id, C=chat_name
    Возвращает {username: {entity_id, chat_name, username}}
    """
    try:
        data   = ss.worksheet('Кэш').get_all_values()
        result = {}
        for row in data[1:]:
            if not row or not row[0].strip():
                continue
            try:
                username  = row[0].strip()
                entity_id = int(row[1].strip())
                chat_name = row[2].strip() if len(row) > 2 else username
                result[username] = {
                    'entity_id': entity_id,
                    'chat_name': chat_name,
                    'username':  username,
                }
            except (ValueError, IndexError):
                continue
        log.info(f'Кэш загружен из таблицы: {len(result)} каналов')
        return result
    except Exception as e:
        log.error(f'Ошибка чтения кэша: {e}')
        return {}


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets — запись
# ══════════════════════════════════════════════════════════════════════════════

def _write_post(ss, post):
    try:
        ss.worksheet('Посты').append_row([
            post['date'].strftime('%Y-%m-%d %H:%M:%S'),
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
            post['date'].strftime('%Y-%m-%d %H:%M:%S'),
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
            [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, safe, str(account)],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error('Ошибка записи лога: ' + str(e))


def _write_cache_row(ss, username: str, entity_id: int, chat_name: str):
    """Дописывает одну строку в лист «Кэш»."""
    try:
        ss.worksheet('Кэш').append_row(
            [username, str(entity_id), chat_name],
            value_input_option='USER_ENTERED',
        )
    except Exception as e:
        log.error(f'Ошибка записи кэша для {username}: {e}')


def _rebuild_cache(ss, entries: list):
    """
    Полностью перезаписывает лист «Кэш» за один запрос.
    entries = list of {'username', 'entity_id', 'chat_name'}
    Вызывается после массового резолва при старте.
    """
    try:
        ws   = ss.worksheet('Кэш')
        rows = [['username', 'entity_id', 'chat_name']]
        rows += [[e['username'], str(e['entity_id']), e['chat_name']] for e in entries]
        ws.clear()
        ws.update(range_name='A1', values=rows, value_input_option='USER_ENTERED')
        log.info(f'Кэш перезаписан: {len(entries)} записей')
    except Exception as e:
        log.error(f'Ошибка перезаписи кэша: {e}')


def _set_channel_status(ss, username: str, status: str):
    """Обновляет колонку C (Статус) для канала в листе «Каналы»."""
    try:
        ws   = ss.worksheet('Каналы')
        data = ws.get_all_values()
        for i, row in enumerate(data[1:], start=2):
            if not row:
                continue
            u = _extract_username(row[0].strip())
            if u and u.lower() == username.lower():
                ws.update_cell(i, 3, status)
                return
    except Exception as e:
        log.error(f'Ошибка обновления статуса канала {username}: {e}')


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


def _build_link(chat, msg_id: int) -> str:
    username = getattr(chat, 'username', None)
    if username:
        return f'https://t.me/{username}/{msg_id}'
    chat_id = str(chat.id)
    if chat_id.startswith('-100'):
        chat_id = chat_id[4:]
    return f'https://t.me/c/{chat_id}/{msg_id}'


def _get_author_info(msg):
    try:
        sender = msg.sender
        if not sender:
            return '', ''
        first    = getattr(sender, 'first_name', '') or ''
        last     = getattr(sender, 'last_name',  '') or ''
        username = getattr(sender, 'username',   '') or ''
        name     = (first + ' ' + last).strip()
        link     = f'https://t.me/{username}' if username else ''
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
# Telegram Bot API (синхронные вызовы для executor)
# ══════════════════════════════════════════════════════════════════════════════

def _tg_request(token: str, method: str, payload: dict) -> dict:
    url  = f'https://api.telegram.org/bot{token}/{method}'
    data = json.dumps(payload).encode('utf-8')
    req  = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        log.error(f'TG API {method} error: {e}')
        return {}


def _send_moderation_card(post: dict, token: str, moderator_chat_id: str) -> int:
    lines = [
        f'⚠️ Пост не прошёл скоринг (скор: {post["score"]}/{state["score_threshold"]})',
        f'📢 {post["chat_name"]}',
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
                {'text': '✅ Отправить', 'callback_data': f'approve:{post["link"]}'},
                {'text': '❌ Пропустить', 'callback_data': f'skip:{post["link"]}'},
            ]]
        },
    })
    return result.get('result', {}).get('message_id', 0)


def _send_alert(token: str, moderator_chat_id: str, message: str):
    """Отправляет простое текстовое уведомление модератору."""
    if not token or not moderator_chat_id:
        return
    _tg_request(token, 'sendMessage', {
        'chat_id': moderator_chat_id,
        'text':    message[:4096],
    })


def _forward_message_sync(token: str, from_chat_id, message_id_tg: int, dest_chat_id: str):
    _tg_request(token, 'forwardMessage', {
        'chat_id':      dest_chat_id,
        'from_chat_id': from_chat_id,
        'message_id':   message_id_tg,
    })
    time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════════════
# Обработчик новых сообщений
# ══════════════════════════════════════════════════════════════════════════════

def _register_handler(client: TelegramClient, acc_label: str, ss, chat_ids: list):
    """
    Регистрирует (или перерегистрирует) NewMessage-хендлер с явным списком
    chat_ids. Без chats= Telethon не гарантирует доставку событий для
    супергрупп/каналов при большом их количестве.
    """
    # Удаляем старый хендлер если есть
    if acc_label in _registered_handlers:
        client.remove_event_handler(_registered_handlers[acc_label])
        log.info(f'[{acc_label}] Старый хендлер удалён')

    @client.on(events.NewMessage(chats=chat_ids))
    async def _on_new_message(event):
        try:
            raw_id = event.chat_id
            abs_id = abs(raw_id)
            meta   = state['id_to_meta'].get(abs_id) or state['id_to_meta'].get(raw_id)
            if not meta:
                return

            msg = event.message
            if msg.action is not None:
                return

            # ── Дедупликация между двумя аккаунтами ───────────────────────
            dedup_key = (abs_id, msg.id)
            if dedup_key in seen_ids:
                return
            seen_ids.append(dedup_key)

            text = msg.text or msg.message or ''
            if hasattr(msg, 'caption') and msg.caption:
                text = msg.caption
            text = ' '.join(text.split())

            # ── Фильтр 1: минус-слова ──────────────────────────────────────
            if text and _has_minus_word(text, state['minus_words']):
                log.debug(f'[минус-слово] {meta["chat_name"]} — выброс')
                return

            # ── Фильтр 2: минимальная длина ───────────────────────────────
            if len(text) < state['min_length']:
                log.debug(f'[короткий] {meta["chat_name"]} ({len(text)} симв) — выброс')
                return

            # ── Фильтр 3: скоринг ─────────────────────────────────────────
            score = _calc_score(text, state['scoring_rules'])

            chat        = await event.get_chat()
            link        = _build_link(chat, msg.id)
            author_name, author_link = _get_author_info(msg)
            chat_name   = meta.get('chat_name', str(abs_id))

            post = {
                'date':        msg.date.replace(tzinfo=None),
                'chat_name':   chat_name,
                'author_name': author_name,
                'author_link': author_link,
                'link':        link,
                'text':        text,
                'score':       score,
                'account':     acc_label,
                'src_chat_id': raw_id,
                'src_msg_id':  msg.id,
            }

            loop      = asyncio.get_running_loop()
            tg_token  = state['tg_token']
            threshold = state['score_threshold']
            dest_chat = state['dest_chat_id']

            if score >= threshold:
                await loop.run_in_executor(_executor, _write_post, ss, post)
                log.info(f'[авто ✅ скор:{score} {acc_label}] {chat_name} → {link}')
                if tg_token and dest_chat:
                    await loop.run_in_executor(
                        _executor, _forward_message_sync,
                        tg_token, raw_id, msg.id, dest_chat,
                    )
            else:
                moderator  = state['moderator_chat_id']
                log.info(f'[модерация ⏳ скор:{score}/{threshold} {acc_label}] {chat_name} → {link}')
                bot_msg_id = 0
                if tg_token and moderator:
                    bot_msg_id = await loop.run_in_executor(
                        _executor, _send_moderation_card, post, tg_token, moderator,
                    )
                await loop.run_in_executor(_executor, _write_rejected, ss, post, bot_msg_id)

        except Exception as e:
            log.error(f'[{acc_label}] Ошибка обработки сообщения: {e}')

    _registered_handlers[acc_label] = _on_new_message
    log.info(f'[{acc_label}] Хендлер зарегистрирован для {len(chat_ids)} каналов')


# ══════════════════════════════════════════════════════════════════════════════
# Управление подпиской на каналы (dual-account с fallback)
# ══════════════════════════════════════════════════════════════════════════════

async def _resolve_entity(clients: dict, username: str, ss) -> dict | None:
    """
    Пробует резолвить канал через acc1, при ошибке — через acc2.
    Если оба провалились — шлёт алерт и ставит статус «недоступен».
    При успехе — дописывает строку в лист «Кэш».
    Возвращает meta-dict или None.
    """
    errors = {}
    for acc_name, client in clients.items():
        try:
            entity    = await client.get_entity(username)
            eid       = abs(entity.id)
            chat_name = getattr(entity, 'title', None) or username
            log.info(f'Резолв [{acc_name}]: {username} → {eid} ({chat_name})')

            # Сохраняем в кэш таблицы
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(_executor, _write_cache_row, ss, username, eid, chat_name)

            return {
                'entity_id': eid,
                'chat_name': chat_name,
                'username':  username,
            }
        except FloodWaitError as e:
            log.warning(f'[{acc_name}] FloodWait при резолве {username}: жду {e.seconds}s')
            await asyncio.sleep(e.seconds + 2)
            try:
                entity    = await client.get_entity(username)
                eid       = abs(entity.id)
                chat_name = getattr(entity, 'title', None) or username

                loop = asyncio.get_running_loop()
                await loop.run_in_executor(_executor, _write_cache_row, ss, username, eid, chat_name)

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

    # Оба провалились
    msg = (
        f'🚫 Канал недоступен обоим аккаунтам: @{username}\n'
        f'acc1: {errors.get("acc1", "—")}\n'
        f'acc2: {errors.get("acc2", "—")}'
    )
    log.error(msg)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_executor, _send_alert, state['tg_token'], state['moderator_chat_id'], msg)
    await loop.run_in_executor(_executor, _set_channel_status, ss, username, 'недоступен')
    await loop.run_in_executor(_executor, _write_log, ss, 'ERROR', msg)
    return None


async def _update_watched_chats(clients: dict, channels: list, ss):
    """
    Резолвит каналы (используя in-memory кэш), обновляет state,
    затем перерегистрирует хендлеры с актуальным списком chat_ids.
    """
    new_ids     = set()
    new_id_meta = {}

    for ch in channels:
        username = ch['username']

        cached = state['username_to_meta'].get(username)
        if cached and 'entity_id' in cached:
            eid = cached['entity_id']
            new_ids.add(eid)
            new_id_meta[eid] = cached
            continue

        meta = await _resolve_entity(clients, username, ss)
        if meta:
            eid = meta['entity_id']
            new_ids.add(eid)
            new_id_meta[eid]                   = meta
            state['username_to_meta'][username] = meta
            await asyncio.sleep(0.8)

    added   = new_ids - state['watched_ids']
    removed = state['watched_ids'] - new_ids

    state['watched_ids'] = new_ids
    state['id_to_meta']  = new_id_meta

    if added:   log.info(f'Добавлено каналов: {len(added)}')
    if removed: log.info(f'Убрано каналов: {len(removed)}')

    # Перерегистрируем хендлеры с актуальным списком
    if state['watched_ids']:
        chat_ids = list(state['watched_ids'])
        for acc_name, client in clients.items():
            _register_handler(client, acc_name, ss, chat_ids)
    else:
        log.warning('Список каналов пуст — хендлеры не зарегистрированы')


# ══════════════════════════════════════════════════════════════════════════════
# Фоновая задача: перезагрузка настроек
# ══════════════════════════════════════════════════════════════════════════════

async def _settings_reload_loop(clients: dict, ss):
    while True:
        await asyncio.sleep(SETTINGS_RELOAD_SEC)
        try:
            log.info('Перезагрузка настроек...')
            loop = asyncio.get_running_loop()

            new_settings = await loop.run_in_executor(_executor, _read_settings,     ss)
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

            if new_rules  is not None: state['scoring_rules'] = new_rules
            if new_minus  is not None: state['minus_words']   = new_minus
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
# Точка входа
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info('═══ TG Parser v3 (dual-account) стартует ═══')

    loop = asyncio.get_running_loop()

    # ── Google Sheets ──────────────────────────────────────────────────────
    try:
        ss = await loop.run_in_executor(_executor, _get_spreadsheet)
        log.info('Google Sheets: подключён')
    except Exception as e:
        log.error('Google Sheets: ошибка подключения: ' + str(e))
        return

    settings = await loop.run_in_executor(_executor, _read_settings,     ss)
    rules    = await loop.run_in_executor(_executor, _read_scoring_rules, ss)
    minus    = await loop.run_in_executor(_executor, _read_minus_words,   ss)
    channels = await loop.run_in_executor(_executor, _read_channels,      ss)

    if not settings:
        log.error('Не удалось прочитать настройки')
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

    # ── Загрузка кэша резолва из таблицы ──────────────────────────────────
    cache = await loop.run_in_executor(_executor, _read_cache, ss)
    state['username_to_meta'].update(cache)
    log.info(f'Кэш загружен в память: {len(cache)} каналов')

    # ── Telegram клиенты ───────────────────────────────────────────────────
    clients = {}

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
        log.warning('Аккаунт 2: пропущен (нет TG_SESSION_2 / TG_API_ID_2 / TG_API_HASH_2)')

    if not clients:
        log.error('Ни один аккаунт не подключён — выход')
        return

    # ── Резолв каналов + регистрация хендлеров ────────────────────────────
    # _update_watched_chats сам вызовет _register_handler в конце
    if not channels:
        log.warning('Лист «Каналы» пуст — добавьте каналы')
    await _update_watched_chats(clients, channels or [], ss)
    log.info(f'Слежу за {len(state["watched_ids"])} каналами/группами')

    # Если после первого запуска в кэше появились новые записи — перезаписываем
    # лист целиком (убираем дубли, которые могли накопиться построчной записью)
    if state['username_to_meta']:
        entries = list(state['username_to_meta'].values())
        await loop.run_in_executor(_executor, _rebuild_cache, ss, entries)

    await loop.run_in_executor(_executor, _write_log, ss, 'INFO',
        f'Запущен | аккаунтов: {len(clients)} | каналов: {len(state["watched_ids"])} | '
        f'правил: {len(state["scoring_rules"])} | порог: {state["score_threshold"]} | '
        f'минус-слов: {len(state["minus_words"])}'
    )

    # ── Фоновая перезагрузка настроек ─────────────────────────────────────
    asyncio.create_task(_settings_reload_loop(clients, ss))

    log.info(f'Слушаю события. Настройки обновляются каждые {SETTINGS_RELOAD_SEC}с')

    # Ждём все клиенты одновременно
    await asyncio.gather(*[c.run_until_disconnected() for c in clients.values()])


if __name__ == '__main__':
    asyncio.run(main())
