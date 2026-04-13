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

# Подписчики: "chat_id:arena_slug,chat_id:arena_slug"
# Например: "278506234:interface-m0ymi5bf4dw,676321557:o_o-edoyoqb7e1m"
def _load_gist_subscribers(bot_name: str, default_slugs: list[str]) -> list[dict]:
    """Загружает подписчиков из GitHub Gist для данного бота."""
    gist_id = os.environ.get("GIST_ID", "")
    github_token = os.environ.get("GITHUB_TOKEN", "")
    if not gist_id or not github_token:
        return []
    try:
        r = httpx.get(
            f"https://api.github.com/gists/{gist_id}",
            headers={"Authorization": f"token {github_token}"},
            timeout=10,
        )
        r.raise_for_status()
        content = r.json()["files"]["subscribers.json"]["content"]
        data = json.loads(content)
        subs = [
            {"chat_id": s["chat_id"], "slugs": default_slugs, "slug": default_slugs[0] if default_slugs else ""}
            for s in data.get(bot_name, [])
        ]
        log.info("Gist: загружено %d подписчиков для '%s'", len(subs), bot_name)
        return subs
    except Exception as e:
        log.warning("Не удалось загрузить Gist: %s", e)
        return []


def _parse_subscribers() -> list[dict]:
    raw = os.environ.get("SUBSCRIBERS", "")
    result = []
    if raw:
        for item in raw.split(","):
            item = item.strip()
            if ":" in item:
                chat_id, slugs_str = item.split(":", 1)
                slugs = [s.strip() for s in slugs_str.split("+") if s.strip()]
                result.append({"chat_id": chat_id.strip(), "slugs": slugs, "slug": slugs[0] if slugs else ""})

    # Загружаем новых подписчиков из Gist (только если BOT_NAME задан)
    bot_name = os.environ.get("BOT_NAME", "")
    default_slugs_raw = os.environ.get("DEFAULT_SLUGS", "")
    default_slugs = [s.strip() for s in default_slugs_raw.split("+") if s.strip()]
    if bot_name and default_slugs:
        existing_ids = {s["chat_id"] for s in result}
        for sub in _load_gist_subscribers(bot_name, default_slugs):
            if sub["chat_id"] not in existing_ids:
                result.append(sub)
                log.info("Новый подписчик из Gist: %s", sub["chat_id"])

    if result:
        return result
    # fallback: старый формат
    chat_ids_raw = os.environ.get("TELEGRAM_CHAT_IDS", os.environ.get("TELEGRAM_CHAT_ID", ""))
    slug = os.environ.get("SOURCE_CHANNEL_SLUG", "interface-m0ymi5bf4dw")
    return [{"chat_id": cid.strip(), "slugs": [slug], "slug": slug} for cid in chat_ids_raw.split(",") if cid.strip()]

SUBSCRIBERS = _parse_subscribers()
TELEGRAM_CHAT_ID = SUBSCRIBERS[0]["chat_id"] if SUBSCRIBERS else ""

# исходный канал (fallback для обратной совместимости)
SOURCE_CHANNEL_SLUG  = os.environ.get("SOURCE_CHANNEL_SLUG", "interface-m0ymi5bf4dw")

# канал, куда будем сохранять новые находки
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
    source_slugs: set[str] = None,
    blocks_per_slug: dict = None,
) -> list[dict]:
    """
    Граф-обход:
      source_blocks → смежные каналы → блоки из этих каналов
    Возвращает отсортированный по score список новых блоков.
    """
    source_ids = {b["id"] for b in source_blocks}
    all_known  = source_ids | known_ids

    # Сэмплируем поровну из каждой доски, чтобы все источники были представлены
    if blocks_per_slug and len(blocks_per_slug) > 1:
        per_slug = max(5, max_source_sample // len(blocks_per_slug))
        sample = []
        for slug_blocks in blocks_per_slug.values():
            s = slug_blocks.copy()
            random.shuffle(s)
            sample.extend(s[:per_slug])
        random.shuffle(sample)
        log.info("Сбалансированный сэмпл: %d блоков из %d досок (%d на доску)", len(sample), len(blocks_per_slug), per_slug)
    else:
        sample = source_blocks.copy()
        random.shuffle(sample)
        sample = sample[:max_source_sample]

    # block_id → {score, channel, sources} где sources — доски, от которых нашли этот блок
    candidates: dict[int, dict] = {}

    def _traverse_blocks(blocks_sample: list[dict], source_slug: str, stop_at: int) -> None:
        """Обходит граф от blocks_sample, тегирует кандидатов source_slug."""
        for i, block in enumerate(blocks_sample, 1):
            bid = block["id"]
            log.info("  [%d/%d] блок %s (из '%s') — ищу смежные каналы", i, len(blocks_sample), bid, source_slug)

            related_channels = get_block_channels(bid, max_pages=2)
            log.info("    найдено %d каналов", len(related_channels))

            for ch in related_channels:
                ch_id   = ch.get("id")
                ch_slug = ch.get("slug")
                if not ch_id or not ch_slug:
                    continue
                if source_slugs and ch_slug in source_slugs:
                    continue

                ch_block_ids = get_channel_block_ids(ch_id)
                for new_id in ch_block_ids:
                    if new_id in all_known:
                        continue
                    if new_id not in candidates:
                        candidates[new_id] = {"score": 0, "channel": ch_slug, "sources": set()}
                    candidates[new_id]["score"] += 1
                    candidates[new_id]["sources"].add(source_slug)

            # early stop только в рамках этого slug'а
            slug_count = sum(1 for c in candidates.values() if source_slug in c["sources"])
            if slug_count >= stop_at:
                log.info("  достаточно кандидатов от '%s' (%d), перехожу к следующей доске", source_slug, slug_count)
                break

    if blocks_per_slug and len(blocks_per_slug) > 1:
        # Каждую доску обходим отдельно — гарантируем кандидатов от каждого источника
        per_slug_sample = max(5, max_source_sample // len(blocks_per_slug))
        per_slug_stop   = target_count * 8
        for slug, slug_blocks in blocks_per_slug.items():
            s = slug_blocks.copy()
            random.shuffle(s)
            log.info("Анализирую '%s' (%d блоков в сэмпле)…", slug, per_slug_sample)
            _traverse_blocks(s[:per_slug_sample], slug, per_slug_stop)
    else:
        log.info("Анализирую связи %d блоков…", len(sample))
        _traverse_blocks(sample, list(blocks_per_slug.keys())[0] if blocks_per_slug else "", target_count * 10)

    if not candidates:
        log.warning("Не найдено новых кандидатов.")
        return []

    # Balanced selection: берём поровну из каждой исходной доски
    if blocks_per_slug and len(blocks_per_slug) > 1:
        per_source = max(1, target_count // len(blocks_per_slug))
        selected = []
        selected_set = set()
        remainder = []  # кандидаты сверх квоты — добавим если не хватает

        for slug in blocks_per_slug:
            slug_cands = {bid: info for bid, info in candidates.items() if slug in info["sources"]}
            slug_sorted = sorted(slug_cands, key=lambda x: slug_cands[x]["score"], reverse=True)
            random.shuffle(slug_sorted[:per_source * 2])
            taken = [bid for bid in slug_sorted[:per_source * 2] if bid not in selected_set][:per_source]
            selected.extend(taken)
            selected_set.update(taken)
            log.info("  '%s' → %d кандидатов, выбрано %d", slug, len(slug_cands), len(taken))
            # остаток — на случай если другой источник пуст
            remainder.extend([bid for bid in slug_sorted[per_source:per_source * 4] if bid not in selected_set])

        # добираем из остатка если одна из досок дала мало
        if len(selected) < target_count:
            random.shuffle(remainder)
            for bid in remainder:
                if bid not in selected_set and len(selected) < target_count:
                    selected.append(bid)
                    selected_set.add(bid)

        random.shuffle(selected)
    else:
        sorted_ids = sorted(candidates.keys(), key=lambda x: candidates[x]["score"], reverse=True)
        top_ids    = sorted_ids[:target_count * 3]
        random.shuffle(top_ids[:target_count * 2])
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

    # применяем умный фильтр
    result = smart_filter(result, candidates, target_count)
    return result


def smart_filter(blocks: list[dict], candidates: dict, target_count: int) -> list[dict]:
    """
    Умный фильтр без AI:
    1. Приоритет — блоки с картинками (image блоки)
    2. Фильтрация по score — только сильно связанные
    3. Дедупликация по домену источника
    4. Логирование статистики
    """
    images     = []
    links      = []
    other      = []
    seen_domains: set[str] = set()

    for block in blocks:
        bid   = block.get("id")
        score = candidates.get(bid, {}).get("score", 0)

        # Фильтр по минимальному score — берём только хорошо связанные
        if score < 2 and len(blocks) > target_count:
            continue

        img = block.get("image") or {}
        has_image = bool(
            (img.get("original") or {}).get("url") or
            (img.get("display") or {}).get("url")
        )

        source  = block.get("source") or {}
        src_url = source.get("url") or ""

        # Дедупликация по домену
        domain = ""
        if src_url:
            try:
                from urllib.parse import urlparse
                domain = urlparse(src_url).netloc
            except Exception:
                pass

        if domain and domain in seen_domains:
            continue
        if domain:
            seen_domains.add(domain)

        if has_image:
            images.append(block)
        elif src_url:
            links.append(block)
        else:
            other.append(block)

    # Собираем: сначала картинки, потом ссылки, потом остальное
    filtered = images + links + other
    log.info(
        "Умный фильтр: %d картинок, %d ссылок, %d прочих → итого %d",
        len(images), len(links), len(other), len(filtered)
    )
    return filtered[:target_count]


# ── Telegram ──────────────────────────────────────────────────────────────────

def tg_send_message(text: str, parse_mode: str = "HTML", chat_id: str = "") -> bool:
    cid = chat_id or TELEGRAM_CHAT_ID
    r = httpx.post(f"{TG_BASE}/sendMessage", json={
        "chat_id":    cid,
        "text":       text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": False,
    }, timeout=15)
    return r.status_code == 200


def tg_send_photo(url: str, caption: str, chat_id: str = "") -> bool:
    cid = chat_id or TELEGRAM_CHAT_ID
    r = httpx.post(f"{TG_BASE}/sendPhoto", json={
        "chat_id": cid,
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
    log.info("Рассылаю %d блоков для %d подписчиков…", len(blocks), len(TELEGRAM_CHAT_IDS))

    for chat_id in TELEGRAM_CHAT_IDS:
        header = (
            f"🗂 <b>Референсы на {today}</b>\n"
            f"Нашёл {len(blocks)} новых блоков на основе доски."
        )
        tg_send_message(header, chat_id=chat_id)
        time.sleep(0.5)

        sent = 0
        for block in blocks:
            img_url   = block_image_url(block)
            caption   = block_caption(block)
            arena_url = f"https://www.are.na/block/{block['id']}"

            ok = False
            if img_url:
                ok = tg_send_photo(img_url, caption, chat_id=chat_id)
            if not ok:
                text = caption + f"\n{arena_url}"
                ok = tg_send_message(text, chat_id=chat_id)

            if ok:
                sent += 1
            time.sleep(0.4)

        log.info("  chat_id %s — отправлено %d блоков", chat_id, sent)


# ── seen ids ──────────────────────────────────────────────────────────────────

def load_seen_ids() -> set[int]:
    if SEEN_IDS_FILE.exists():
        data = json.loads(SEEN_IDS_FILE.read_text())
        return set(data)
    return set()


def save_seen_ids(ids: set[int]) -> None:
    SEEN_IDS_FILE.write_text(json.dumps(list(ids)))


# ── main ──────────────────────────────────────────────────────────────────────

def run_for_subscriber(sub: dict, all_seen_ids: set[int]) -> set[int]:
    """Запускает подборку для одного подписчика."""
    chat_id = sub["chat_id"]
    slugs   = sub.get("slugs") or [sub.get("slug", "")]

    log.info("── Подписчик %s (доски: %s) ──", chat_id, ", ".join(slugs))

    blocks_per_slug: dict[str, list[dict]] = {}
    source_blocks = []
    for slug in slugs:
        blocks = get_channel_blocks(slug)
        if blocks:
            blocks_per_slug[slug] = blocks
            source_blocks.extend(blocks)
            log.info("Загружено %d блоков из '%s'", len(blocks), slug)
        else:
            log.warning("Доска '%s' пуста или недоступна", slug)

    if not source_blocks:
        tg_send_message("😶 Не удалось загрузить доску. Попробую завтра!", chat_id=chat_id)
        return set()

    source_slugs = set(slugs)

    # seen_ids персональные для каждого подписчика
    seen_file = Path(f"seen_{chat_id}.json")
    seen_ids  = set(json.loads(seen_file.read_text())) if seen_file.exists() else set()

    # исключаем блоки из досок других подписчиков чтобы не повторяться
    other_slugs = []
    for s in SUBSCRIBERS:
        if s["chat_id"] != chat_id:
            other_slugs.extend(s.get("slugs") or [s.get("slug", "")])
    for other_slug in other_slugs:
        try:
            other_blocks = get_channel_blocks(other_slug)
            other_ids = {b["id"] for b in other_blocks}
            seen_ids = seen_ids | other_ids
            log.info("Исключаю %d блоков из доски '%s'", len(other_ids), other_slug)
        except Exception:
            pass

    count = random.randint(DAILY_MIN, DAILY_MAX)
    log.info("Блоков в доске: %d, уже видели/исключено: %d, цель: %d", len(source_blocks), len(seen_ids), count)

    new_blocks = discover_new_blocks(source_blocks, seen_ids, count, source_slugs=source_slugs, blocks_per_slug=blocks_per_slug)
    new_blocks = [b for b in new_blocks if b.get("id")]

    if not new_blocks:
        tg_send_message("😶 Сегодня новых референсов не нашлось. Попробую завтра!", chat_id=chat_id)
        log.info("Новых блоков нет для %s", chat_id)
        return set()

    # отправляем в Telegram
    today = date.today().strftime("%d.%m.%Y")
    header = f"🗂 <b>Референсы на {today}</b>\nНашёл {len(new_blocks)} новых блоков на основе твоей доски."
    tg_send_message(header, chat_id=chat_id)
    time.sleep(0.5)

    sent = 0
    for block in new_blocks:
        img_url   = block_image_url(block)
        caption   = block_caption(block)
        arena_url = f"https://www.are.na/block/{block['id']}"

        ok = False
        if img_url:
            ok = tg_send_photo(img_url, caption, chat_id=chat_id)
        if not ok:
            ok = tg_send_message(caption + f"\n{arena_url}", chat_id=chat_id)
        if ok:
            sent += 1
        time.sleep(0.4)

    log.info("Отправлено %s: %d блоков", chat_id, sent)

    # обновляем персональные seen_ids
    new_ids = {b["id"] for b in new_blocks}
    seen_file.write_text(json.dumps(list(seen_ids | new_ids)))
    return new_ids


def run():
    log.info("═══ Are.na Daily Refs Bot ═══")
    log.info("Подписчиков: %d", len(SUBSCRIBERS))

    for sub in SUBSCRIBERS:
        try:
            run_for_subscriber(sub, set())
        except Exception as e:
            log.error("Ошибка для подписчика %s: %s", sub.get("chat_id"), e)

    log.info("═══ Готово! ═══")


if __name__ == "__main__":
    run()
