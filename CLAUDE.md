# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Are.na References Bot system — три Telegram бота, которые ежедневно отправляют подборки визуальных референсов из Are.na. Один скрипт (`arena_refs_bot.py`) используется во всех трёх ботах.

**Репозитории:**
- `Ruslanroses/arena-refs-bot` — RefArena (@RefArenabot), интерфейсы
- `Ruslanroses/perfect-refs-bot` — Arena_P, арт/эстетика (только Руслан)
- `Ruslanroses/tools-refs-bot` — Arena.Tools (@Arena_Tools_bot), дизайн-инструменты
- `Ruslanroses/arena-registration-bot` — Registration Bot (Railway, 24/7)

## Запуск

```bash
# Установка зависимостей
pip install httpx

# Запуск бота вручную (нужны переменные из .env)
python arena_refs_bot.py

# Триггер GitHub Actions вручную
gh workflow run daily.yml --repo Ruslanroses/arena-refs-bot
```

## Архитектура

### arena_refs_bot.py (ежедневная рассылка)

Запускается через GitHub Actions в 09:00 UTC (12:00 Тбилиси).

**Ключевые функции:**
- `_load_gist_subscribers(bot_name, default_slugs)` — загружает подписчиков из GitHub Gist по ключу `BOT_NAME`. Используется только если заданы `BOT_NAME` и `DEFAULT_SLUGS` env vars.
- `_parse_subscribers()` — парсит `SUBSCRIBERS` env (`chat_id:slug1+slug2,...`) и мерджит с Gist-подписчиками. Вызывается при импорте модуля — логгер должен быть инициализирован до этих функций.
- `run_for_subscriber(sub, all_seen_ids)` — основной цикл для одного подписчика: загружает Are.na каналы, ищет новые блоки, отправляет в Telegram.
- `discover_new_blocks(...)` — граф-обход Are.na: блоки из исходных каналов → смежные каналы → новые блоки. Каждый slug обходится отдельно (`_traverse_blocks`), чтобы гарантировать представленность каждого источника. Balanced selection 50/50 между источниками.
- `smart_filter(blocks, candidates, target_count)` — приоритет картинкам, дедупликация по домену, фильтр по score.

**Персональный кэш:** `seen_{chat_id}.json` — хранит ID уже отправленных блоков для каждого подписчика. Кэшируется между запусками через `actions/cache@v4` (wildcard `seen_*.json`).

### registration_bot.py (Railway, 24/7)

Long-polling все три бота в одном процессе. Состояние ожидающих ввода кода хранится в `pending` dict (`bot_name:chat_id → username`).

**Флоу:**
1. `/start` → проверяет Gist → если новый: запрашивает пригласительный код
2. Текст (не команда) → если `bot_name:chat_id` в `pending`: проверяет код
3. Код верный (`INVITE_CODES = {"prostor friends"}`) → `add_subscriber()` → Gist + Sheets

## Переменные окружения

### arena_refs_bot.py
| Переменная | Где задана | Описание |
|---|---|---|
| `ARENA_TOKEN` | GitHub Secret | Are.na API токен |
| `TELEGRAM_BOT_TOKEN` | GitHub Secret | Токен бота |
| `SUBSCRIBERS` | GitHub Secret | `chat_id:slug1+slug2,chat_id:slug` |
| `BOT_NAME` | workflow env | `RefArena` или `Arena_Tools` (для Gist) |
| `DEFAULT_SLUGS` | GitHub Secret | Are.na каналы для новых подписчиков |
| `GIST_ID` | GitHub Secret | ID Gist с подписчиками |
| `GH_PAT` | GitHub Secret | GitHub PAT для чтения Gist |

### registration_bot.py (Railway env vars)
`REFBOT_TOKEN`, `PERFECTBOT_TOKEN`, `TOOLSBOT_TOKEN`, `GITHUB_TOKEN`, `GIST_ID`, `SHEETS_ID`, `GOOGLE_CREDS` (JSON строка)

## Are.na API

- Base URL: `https://api.are.na/v2`
- Rate limit: 429 → retry с паузой 15–120 сек (8 попыток)
- Ключевые эндпоинты: `/channels/{slug}`, `/channels/{slug}/contents`, `/blocks/{id}/channels`
- `PER_PAGE = 100` (максимум)

## Google Sheets

- Лист называется **"Лист1"** (русский Google Sheets, НЕ "Sheet1")
- Авторизация через service account (`google-auth`, `google-api-python-client`)
- Credentials передаются как JSON-строка в `GOOGLE_CREDS` env var
- Файл credentials локально: `~/Downloads/Arena Bots.json`

## Gist — база подписчиков

```json
{
  "RefArena": [{"chat_id": "278506234", "username": "...", "added": "2026-04-10"}],
  "Arena_P":  [...],
  "Arena_Tools": [...]
}
```

Gist ID: `21d72f7bee86eeaa89df90878db06c03`

## Важные ограничения

- **Arena_P не читает Gist** — приватный бот, только для Руслана. `BOT_NAME` не задан в его workflow.
- **Логгер должен инициализироваться до `_parse_subscribers()`** — функция вызывается на уровне модуля при импорте.
- **Are.na rate limit** — после нескольких тестовых запусков подряд возможен 429, нужно ждать ~30 мин.
