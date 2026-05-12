# TG Parser v3

Парсер Telegram-каналов с двумя аккаунтами, скорингом и модерацией через бота.

## Архитектура

```
Railway (main.py)
  ├── acc1 (TelegramClient) ─┐
  └── acc2 (TelegramClient) ─┴── слушают все каналы одновременно
                                  │
                                  ├── дедупликация (deque 2000 msg)
                                  ├── минус-слова → выброс
                                  ├── мин. длина  → выброс
                                  └── скоринг
                                        ├── ≥ порога → Посты + forward в канал
                                        └── < порога → Отклонённые + карточка боту

GAS-триггер (каждую минуту)
  └── pollBotUpdates() → ✅/❌ кнопки → обновляет «Отклонённые»
```

**Fallback при недоступности канала:**  
При резолве нового канала сначала пробует acc1, при ошибке — acc2.  
Если оба провалились → алерт в бот + статус «недоступен» в таблице.

**Дедупликация:**  
Оба аккаунта получают одно и то же событие — первый пишет пост, второй видит его в `seen_ids` и игнорирует.

---

## Быстрый старт

### 1. Клонировать репо и настроить окружение

```bash
git clone https://github.com/YOUR/tg-parser.git
cd tg-parser
cp .env.example .env
```

Заполнить `.env` (см. раздел ниже). Файл `.env` **не коммитится**.

### 2. Получить session string для каждого аккаунта

Используй свой скрипт для генерации StringSession, добавь строки в `.env`:

```
TG_SESSION_1=1BVtsOH8BuzАААААА...
TG_SESSION_2=1BVtsOH8BuzБББББ...
```

### 3. Подготовить Google Service Account

1. Google Cloud Console → создать проект → включить **Sheets API** и **Drive API**
2. IAM → Service Accounts → создать → скачать JSON
3. В Google Таблице → Поделиться → добавить email сервис-аккаунта с правами редактора
4. Закодировать JSON в base64:
   ```bash
   # Linux/Mac
   base64 -w 0 service-account.json
   # Вывод вставить в GOOGLE_CREDENTIALS_BASE64
   ```

### 4. Настроить Google Таблицу

Открыть пустую таблицу → Расширения → Apps Script → вставить `setup.gs` → запустить `setupAll`.

Заполнить лист **Настройки**:
- `B2` — токен бота (@BotFather)
- `B3` — порог скора (например `7`)
- `B4` — мин. длина текста (`20`)
- `B5` — твой `moderator_chat_id` (@userinfobot)
- `B6` — `chat_id` канала-назначения

### 5. Деплой на Railway

```bash
# Войти в Railway CLI
railway login

# Создать проект (первый раз)
railway init

# Задать переменные из .env
railway variables set --from .env

# Деплой
railway up
```

Или через GitHub: Railway Dashboard → New Project → Deploy from GitHub repo.  
После первого деплоя Railway автоматически передеплоит при каждом `git push`.

---

## Переменные окружения (.env)

| Переменная | Описание |
|---|---|
| `TG_API_ID_1` | api_id первого аккаунта (my.telegram.org) |
| `TG_API_HASH_1` | api_hash первого аккаунта |
| `TG_SESSION_1` | StringSession первого аккаунта |
| `TG_API_ID_2` | api_id второго аккаунта |
| `TG_API_HASH_2` | api_hash второго аккаунта |
| `TG_SESSION_2` | StringSession второго аккаунта |
| `SPREADSHEET_ID` | ID Google Таблицы |
| `GOOGLE_CREDENTIALS_BASE64` | Service account JSON в base64 |
| `SETTINGS_RELOAD_INTERVAL` | Интервал перечитывания таблицы (сек, по умолчанию 300) |

> Можно запустить и с одним аккаунтом — просто не заполнять `TG_*_2`.

---

## Структура репо

```
tg-parser/
├── main.py           # основной парсер
├── requirements.txt  # зависимости Python
├── railway.toml      # конфиг деплоя
├── .env.example      # шаблон переменных (коммитится)
├── .env              # реальные секреты (НЕ коммитится!)
├── .gitignore
└── README.md
```

---

## GAS-триггер модерации

В Apps Script таблицы создать триггер: **каждую минуту → `pollBotUpdates`**.

Функция:
1. Вызывает `getUpdates` Bot API
2. Находит нажатия `✅ Отправить` / `❌ Пропустить`
3. При `✅` → `forwardMessage` в канал-назначение + статус «отправлен»
4. При `❌` → статус «пропущен»
5. Вызывает `answerCallbackQuery` чтобы убрать часики у кнопки

---

## Логи

Railway → твой сервис → вкладка **Logs** — все `log.info` / `log.error` в реальном времени.  
Также пишутся в лист **Логи** Google Таблицы (уровни INFO / WARN / ERROR с подсветкой).
