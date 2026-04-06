"""
Are.na Daily References Bot
----------------------------
Каждый день находит 20-40 новых визуальных референсов на основе
твоей Are.na доски и отправляет их в Telegram + сохраняет на Are.na.

Логика поиска (граф связей Are.na):
  1. Берёт все блоки из твоего исходного канала.
  2. Для каждого блока смотрит, в каких ещё каналах он встречается.
  3. Собирает блоки из этих «родственных» каналов.
  4. Ранжирует по частоте совпадений (чем в большем числе связанных
     каналов встречается блок, тем он релевантнее).
  5. Отфильтровывает уже виденное и отправляет топ N.
"""

import os
import json
import time
import random
import logging
import httpx
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# ── конфиг ────────────────────────────────────────────────────────────────────

ARENA_TOKEN          = os.environ["ARENA_TOKEN"]
TELEGRAM_BOT_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID     = os.environ["TELEGRAM_CHAT_ID"]

# исходный канал с твоими референсами (slug из URL)
SOURCE_CHANNEL_SLUG  = os.environ.get("SOURCE_CHANNEL_SLUG", "interface-m0ymi5bf4dw")

# канал, куда будем сохранять новые находки (создай пустой канал на are.na и вставь slug)
OUTPUT_CHANNEL_SLUG  = os.environ.get("OUTPUT_CHANNEL_SLUG", SOURCE_CHANNEL_SLUG)

# сколько новых референсов отправлять каждый день
DAILY_MIN = int(os.environ.get("DAILY_MIN", 20))
DAILY_MAX = int(os.environ.get("DAILY_MAX", 40))

# файл для хранения уже виденных block id
SEEN_IDS_FILE = Path(os.environ.get("SEEN_IDS_FILE", "seen_ids.json"))

ARENA_BASE  = "https://api.are.na/v2"
TG_BASE     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
PER_PAGE    = 100   # максимум для Are.na API
RATE_SLEEP  = 0.3   # секунды между запросами (rate limit)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Are.na API ────────────────────────────────────────────────────────────────

def arena_headers() -> dict:
    return {"Authorization": f"Bearer {ARENA_TOKEN}"}


def arena_get(path: str, params: dict = None, retries: int = 8) -> dict:
    """GET-запрос к Are.na API с простым retry."""
    url = f"{ARENA_BASE}{path}"
    for attempt in range(retries):
        try:
            r = httpx.get(url, headers=arena_headers(), params=params, timeout=30)
            if r.status_code == 429:
                wait = 15 * (attempt + 1)
                log.warning("Rate limit, жду %ss…", wait)
                time.sleep(wait)
                continue
            if r.status_code in (502, 503, 504):
                wait = 5 * (attempt + 1)
                log.warning("Are.na %s, повтор через %ss…", r.status_code, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            time.sleep(RATE_SLEEP)
            return r.json()
        except httpx.TimeoutException:
            wait = 5 * (attempt + 1)
            log.warning("Timeout, повтор через %ss…", wait)
            time.sleep(wait)
        except httpx.HTTPStatusError as e:
            log.error("HTTP error %s for %s", e.response.status_code, url)
            if attempt == retries - 1:
                return {}
            time.sleep(3)
    return {}


def arena_post(path: str, body: dict) -> dict:
    url = f"{ARENA_BASE}{path}"
    r = httpx.post(url, headers=arena_headers(), json=body, timeout=20)
    r.raise_for_status()
    time.sleep(RATE_SLEEP)
    return r.json()


def get_channel_blocks(slug: str) -> list[dict]:
    """Возвращает все блоки канала (постранично)."""
    log.info("Загружаю канал '%s'…", slug)
    first  = arena_get(f"/channels/{slug}")
    length = first.get("length", 0)
    pages  = (length // PER_PAGE) + 1

    blocks = []
    for page in range(1, pages + 1):
        data = arena_get(f"/channels/{slug}/contents", {"per": PER_PAGE, "page": page})
        blocks.extend(data.get("contents", []))
        log.info("  страница %d/%d — итого блоков: %d", page, pages, len(blocks))

    return blocks


def get_block_channels(block_id: int, max_pages: int = 3) -> list[dict]:
    """
    Возвращает каналы, в которых встречается данный блок.
    Ограничиваем max_pages, чтобы не делать слишком много запросов.
    """
    channels = []
    for page in range(1, max_pages + 1):
        data = arena_get(f"/blocks/{block_id}/channels", {"per": PER_PAGE, "page": page})
        batch = data.get("channels", [])
        channels.extend(batch)
        if len(batch) < PER_PAGE:
            break
    return channels


def get_channel_block_ids(channel_id: int) -> set[int]:
    """Быстро берём только id блоков из канала (первые 3 страницы)."""
    ids = set()
    for page in range(1, 4):
        data = arena_get(f"/channels/{channel_id}/contents", {"per": PER_PAGE, "page": page})
        for b in data.get("contents", []):
            ids.add(b["id"])
        if len(data.get("contents", [])) < PER_PAGE:
            break
    return ids


def get_channel_id(slug: str) -> Optional[int]:
    """Возвращает числовой ID канала по slug."""
    data = arena_get(f"/channels/{slug}")
    return data.get("id")


def add_block_to_channel(channel_slug: str, block: dict) -> bool:
    """Сохраняет блок в канал."""
    try:
        block_id = block.get("id")

        # 1. Прямой URL картинки (image block)
        img = block.get("image") or {}
        img_url = (
            (img.get("original") or {}).get("url") or
            (img.get("display") or {}).get("url")
        )
        if img_url:
            payload = {"content": img_url}

        # 2. Ссылка из source
        elif (block.get("source") or {}).get("url"):
            payload = {"content": block["source"]["url"]}

        # 3. Ссылка на Are.na блок как fallback
        elif block_id:
            payload = {"content": f"https://www.are.na/block/{block_id}"}

        else:
            return False

        arena_post(f"/channels/{channel_slug}/blocks", payload)
        return True
    except Exception as e:
        log.warning("Не удалось добавить блок %s: %s", block.get("id"), e)
        return False


# ── поиск новых референсов ────────────────────────────────────────────────────

def discover_new_blocks(
    source_blocks: list[dict],
    known_ids: set[int],
    target_count: int,
    max_source_sample: int = 60,
) -> list[dict]:
    """
    Граф-обход:
      source_blocks → смежные каналы → блоки из этих каналов
    Возвращает отсортированный по score список новых блоков.
    """
    source_ids = {b["id"] for b in source_blocks}
    all_known  = source_ids | known_ids

    # Не обходим все блоки — берём случайную выборку,
    # чтобы каждый день находки были разными
    sample = source_blocks.copy()
    random.shuffle(sample)
    sample = sample[:max_source_sample]

    # block_id → score (количество вхождений в связанные каналы)
    candidates: dict[int, dict] = {}

    log.info("Анализирую связи %d блоков…", len(sample))
    for i, block in enumerate(sample, 1):
        bid = block["id"]
        log.info("  [%d/%d] блок %s — ищу смежные каналы", i, len(sample), bid)

        related_channels = get_block_channels(bid, max_pages=2)
        log.info("    найдено %d каналов", len(related_channels))

        for ch in related_channels:
            ch_id   = ch.get("id")
            ch_slug = ch.get("slug")
            if not ch_id or not ch_slug:
                continue
            # пропускаем сам исходный канал
            if ch_slug == SOURCE_CHANNEL_SLUG:
                continue

            # получаем блоки из смежного канала
            ch_block_ids = get_channel_block_ids(ch_id)
            for new_id in ch_block_ids:
                if new_id in all_known:
                    continue
                if new_id not in candidates:
                    candidates[new_id] = {"score": 0, "channel": ch_slug}
                candidates[new_id]["score"] += 1

        if len(candidates) > target_count * 10:
            log.info("  достаточно кандидатов (%d), прерываю обход", len(candidates))
            break

    if not candidates:
        log.warning("Не найдено новых кандидатов.")
        return []

    # сортируем по score, потом перемешиваем внутри одного score-уровня
    sorted_ids = sorted(candidates.keys(), key=lambda x: candidates[x]["score"], reverse=True)
    top_ids    = sorted_ids[:target_count * 3]  # берём с запасом
    random.shuffle(top_ids[:target_count * 2])  # небольшое перемешивание
    selected   = top_ids[:target_count]

    log.info("Отобрано %d новых блоков (из %d кандидатов)", len(selected), len(candidates))

    # загружаем полные данные блоков
    result = []
    for bid in selected:
        try:
            data = arena_get(f"/blocks/{bid}")
            result.append(data)
        except Exception as e:
            log.warning("Не удалось получить блок %s: %s", bid, e)

    return result


# ── Telegram ──────────────────────────────────────────────────────────────────

def tg_send_message(text: str, parse_mode: str = "HTML") -> bool:
    r = httpx.post(f"{TG_BASE}/sendMessage", json={
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": False,
    }, timeout=15)
    return r.status_code == 200


def tg_send_photo(url: str, caption: str) -> bool:
    r = httpx.post(f"{TG_BASE}/sendPhoto", json={
        "chat_id": TELEGRAM_CHAT_ID,
        "photo":   url,
        "caption": caption,
        "parse_mode": "HTML",
    }, timeout=15)
    return r.status_code == 200


def block_image_url(block: dict) -> Optional[str]:
    """Пытается извлечь URL изображения из блока."""
    # image block
    img = block.get("image")
    if img:
        return (
            img.get("display", {}).get("url")
            or img.get("original", {}).get("url")
        )
    # attachment
    att = block.get("attachment")
    if att:
        return att.get("url")
    return None


def block_caption(block: dict) -> str:
    title   = block.get("title") or ""
    source  = block.get("source", {}) or {}
    src_url = source.get("url") or block.get("source_url") or ""
    arena_url = f"https://www.are.na/block/{block['id']}"

    parts = []
    if title:
        parts.append(f"<b>{title}</b>")
    if src_url:
        parts.append(f'<a href="{src_url}">источник</a>')
    parts.append(f'<a href="{arena_url}">Are.na</a>')
    return "  ·  ".join(parts)


def send_daily_digest(blocks: list[dict]) -> None:
    today = date.today().strftime("%d.%m.%Y")
    header = (
        f"🗂 <b>Референсы на {today}</b>\n"
        f"Нашёл {len(blocks)} новых блоков на основе твоей доски."
    )
    tg_send_message(header)
    time.sleep(0.5)

    sent = 0
    for block in blocks:
        img_url = block_image_url(block)
        caption = block_caption(block)
        arena_url = f"https://www.are.na/block/{block['id']}"

        ok = False
        if img_url:
            ok = tg_send_photo(img_url, caption)
        # если фото не отправилось или нет картинки — отправляем текстом со ссылкой
        if not ok:
            text = caption + f"\n{arena_url}"
            ok = tg_send_message(text)

        if ok:
            sent += 1
        time.sleep(0.4)  # Telegram rate limit

    log.info("Отправлено в Telegram: %d блоков", sent)


# ── seen ids ──────────────────────────────────────────────────────────────────

def load_seen_ids() -> set[int]:
    if SEEN_IDS_FILE.exists():
        data = json.loads(SEEN_IDS_FILE.read_text())
        return set(data)
    return set()


def save_seen_ids(ids: set[int]) -> None:
    SEEN_IDS_FILE.write_text(json.dumps(list(ids)))


# ── main ──────────────────────────────────────────────────────────────────────

def run():
    log.info("═══ Are.na Daily Refs Bot ═══")
    log.info("Источник: %s", SOURCE_CHANNEL_SLUG)
    log.info("Выход:    %s", OUTPUT_CHANNEL_SLUG)

    seen_ids     = load_seen_ids()
    source_blocks = get_channel_blocks(SOURCE_CHANNEL_SLUG)
    log.info("Блоков в источнике: %d, уже видели: %d", len(source_blocks), len(seen_ids))

    count = random.randint(DAILY_MIN, DAILY_MAX)
    log.info("Цель на сегодня: %d новых блоков", count)

    new_blocks = discover_new_blocks(source_blocks, seen_ids, count)

    if not new_blocks:
        tg_send_message("😶 Сегодня новых референсов не нашлось. Попробую завтра!")
        log.info("Новых блоков нет.")
        return

    # фильтруем блоки без id
    new_blocks = [b for b in new_blocks if b.get("id")]

    # сохраняем на Are.na
    saved = 0
    if OUTPUT_CHANNEL_SLUG and OUTPUT_CHANNEL_SLUG != SOURCE_CHANNEL_SLUG:
        log.info("Сохраняю %d блоков в канал '%s'…", len(new_blocks), OUTPUT_CHANNEL_SLUG)
        for block in new_blocks:
            if add_block_to_channel(OUTPUT_CHANNEL_SLUG, block):
                saved += 1
            time.sleep(0.5)
        log.info("Сохранено на Are.na: %d", saved)

    # отправляем в Telegram
    send_daily_digest(new_blocks)

    # обновляем seen_ids
    new_ids = {b["id"] for b in new_blocks}
    save_seen_ids(seen_ids | new_ids)
    log.info("Готово! Всего видели блоков: %d", len(seen_ids) + len(new_ids))


if __name__ == "__main__":
    run()
