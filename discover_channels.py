"""
discover_channels.py — Еженедельный дискавери соседних каналов Are.na
----------------------------------------------------------------------
Для каждого slug из DEFAULT_SLUGS вызывает GET /v2/channels/{slug}/channels
и собирает "соседние" каналы — те, чьи блоки пересекаются с данным каналом.

Результат записывается в discovered_channels.md (коммитится в репо).
Уже известные slugs (из DEFAULT_SLUGS и предыдущего discovered_channels.md)
исключаются — таблица содержит только новые каналы.

Запуск:
  ARENA_TOKEN=... DEFAULT_SLUGS=slug1+slug2 python discover_channels.py
"""

import os
import re
import sys
import time
import logging
import httpx
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── конфиг ────────────────────────────────────────────────────────────────────

ARENA_TOKEN = os.environ.get("ARENA_TOKEN", "")
if not ARENA_TOKEN:
    log.error("ARENA_TOKEN не задан — выход")
    sys.exit(1)

DEFAULT_SLUGS_RAW = os.environ.get("DEFAULT_SLUGS", "")
if not DEFAULT_SLUGS_RAW:
    log.error("DEFAULT_SLUGS не задан — выход")
    sys.exit(1)

DEFAULT_SLUGS: list[str] = [s.strip() for s in DEFAULT_SLUGS_RAW.split("+") if s.strip()]
log.info("DEFAULT_SLUGS: %s", DEFAULT_SLUGS)

ARENA_BASE = "https://api.are.na/v2"
RATE_SLEEP = 0.3  # секунды между запросами (rate limit)
DISCOVERED_FILE = "discovered_channels.md"

# ── Are.na API ────────────────────────────────────────────────────────────────

def arena_headers() -> dict:
    return {"Authorization": f"Bearer {ARENA_TOKEN}"}


def arena_get(path: str, params: dict = None, retries: int = 8) -> dict:
    """GET-запрос к Are.na API с retry на 429/502/503/504."""
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
            # 404/410 — канал не найден или удалён: возвращаем пустой dict
            if r.status_code in (404, 410):
                log.warning("Are.na вернул %s для %s — пропускаем", r.status_code, url)
                return {}
            r.raise_for_status()
            time.sleep(RATE_SLEEP)
            return r.json()
        except httpx.TimeoutException:
            wait = 5 * (attempt + 1)
            log.warning("Timeout, повтор через %ss…", wait)
            time.sleep(wait)
        except httpx.HTTPStatusError as e:
            log.error("HTTP error %s для %s", e.response.status_code, url)
            return {}
    log.error("Все %d попыток исчерпаны для %s", retries, url)
    return {}


# ── дискавери ─────────────────────────────────────────────────────────────────

def get_adjacent_channels(slug: str, already_known: set[str]) -> list[dict]:
    """
    GET /v2/channels/:slug/channels — соседние каналы.
    Возвращает каналы, блоки которых пересекаются с данным каналом.
    Исключает уже известные slugs и приватные каналы.
    """
    channels: list[dict] = []
    page = 1
    while True:
        data = arena_get(f"/channels/{slug}/channels", {"per": 100, "page": page})
        batch = data.get("channels", [])
        if not batch:
            break
        channels.extend(batch)
        log.info("  slug=%s page=%d: получено %d каналов", slug, page, len(batch))
        if len(batch) < 100:
            break
        page += 1

    # Фильтруем: только публичные, не в already_known
    result = []
    for ch in channels:
        ch_slug = ch.get("slug", "")
        if not ch_slug:
            continue
        if ch_slug in already_known:
            continue
        # Защита от приватных каналов (T-05-03)
        if ch.get("status") != "public":
            log.debug("Пропускаем приватный канал: %s (status=%s)", ch_slug, ch.get("status"))
            continue
        result.append(ch)

    log.info("slug=%s: найдено %d новых публичных каналов", slug, len(result))
    return result


def parse_known_slugs_from_file(filepath: str) -> set[str]:
    """Парсит slug'и из существующего discovered_channels.md (колонка Slug)."""
    known: set[str] = set()
    try:
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                # Строки таблицы вида: | some-slug | Title | ... |
                m = re.match(r"^\|\s*([\w\-]+)\s*\|", line)
                if m:
                    slug = m.group(1).strip()
                    if slug and slug.lower() not in ("slug", "---"):
                        known.add(slug)
        log.info("Из %s прочитано %d уже известных slugs", filepath, len(known))
    except FileNotFoundError:
        log.info("%s не существует — начинаем с чистого листа", filepath)
    return known


def write_discovered_channels(new_channels: list[dict], source_map: dict[str, str]) -> None:
    """Записывает discovered_channels.md с таблицей новых каналов."""
    today = date.today().isoformat()
    lines = [
        f"# Discovered Channels — {today}",
        "",
        f"Найдено {len(new_channels)} новых каналов через Are.na граф.",
        f"Источник: DEFAULT_SLUGS каналы ({', '.join(DEFAULT_SLUGS)}).",
        "",
        "| Slug | Title | Block Count | Connected via |",
        "|------|-------|-------------|---------------|",
    ]
    for ch in new_channels:
        slug = ch.get("slug", "")
        title = ch.get("title", "").replace("|", "\\|")
        block_count = ch.get("length", 0)
        via = source_map.get(slug, "")
        lines.append(f"| {slug} | {title} | {block_count} | {via} |")

    with open(DISCOVERED_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    log.info("Записан %s (%d строк)", DISCOVERED_FILE, len(new_channels))


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    # Уже известные slugs = DEFAULT_SLUGS + ранее найденные в discovered_channels.md
    already_known: set[str] = set(DEFAULT_SLUGS)
    already_known |= parse_known_slugs_from_file(DISCOVERED_FILE)
    log.info("Всего известных slugs: %d", len(already_known))

    # Дискавери: для каждого source slug собираем соседей
    new_channels: list[dict] = []
    source_map: dict[str, str] = {}  # slug -> source_slug (откуда пришёл)
    seen_new_slugs: set[str] = set()

    for source_slug in DEFAULT_SLUGS:
        log.info("Обрабатываем source slug: %s", source_slug)
        try:
            found = get_adjacent_channels(source_slug, already_known | seen_new_slugs)
        except Exception as e:
            log.warning("Ошибка при обработке slug=%s: %s — пропускаем", source_slug, e)
            continue

        for ch in found:
            ch_slug = ch.get("slug", "")
            if ch_slug and ch_slug not in seen_new_slugs:
                new_channels.append(ch)
                source_map[ch_slug] = source_slug
                seen_new_slugs.add(ch_slug)

    log.info("Итого новых каналов: %d", len(new_channels))

    if not new_channels:
        with open(DISCOVERED_FILE, "w", encoding="utf-8") as f:
            f.write(f"# Discovered Channels — {date.today().isoformat()}\n\nНовых каналов не найдено.\n")
        log.info("Новых каналов не найдено — записан пустой файл")
        sys.exit(0)

    # Сортируем по block_count убыванию
    new_channels.sort(key=lambda ch: ch.get("length", 0), reverse=True)

    write_discovered_channels(new_channels, source_map)
    log.info("Готово. Проверьте %s", DISCOVERED_FILE)


if __name__ == "__main__":
    main()
