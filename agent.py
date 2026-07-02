import os
import re
import random
import requests
import time
import json
import base64
from collections import Counter
from datetime import datetime

HH_TOKEN = os.environ['HH_ACCESS_TOKEN']
GPT_TOKEN = os.environ['GPT_MODELS_TOKEN']
GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
REPO = os.environ['GITHUB_REPOSITORY']

PROFILE = os.environ.get('PROFILE', '')

# ============================================================
# Компактный профиль для классификатора is_relevant.
# ============================================================
CANDIDATE_PROFILE = """
Кандидат: Руководитель отдела продаж / клиентского сервиса, Москва.
Грейд: руководящий (Senior / Lead / Head). НЕ рядовой менеджер.

Сильные стороны: построение отделов продаж с нуля, KPI и системы мотивации,
внедрение CRM (AmoCRM), B2B-продажи, переговоры, конкурентная разведка / SWOT,
ценообразование, обучение и развитие команд продаж и клиентского сервиса.

Целевые роли: Руководитель отдела продаж (РОП), Head of Sales,
Директор по продажам, Коммерческий директор, Руководитель направления продаж,
Руководитель отдела продаж B2B, Business Development Manager,
Руководитель отдела клиентского сервиса / клиентского обслуживания,
Sales Manager (только руководящего/lead-уровня).

Оценка идёт по РОЛИ (руководящая sales / клиентский сервис) и ГРЕЙДУ
(руководящий, не рядовой), НЕ по отрасли. Все отрасли равнозначны -
приоритета IT/tech нет, другие отрасли не понижаются.

Hard NO:
  - рядовой менеджер по продажам / клиентский менеджер (не руководитель)
  - junior / стажёр / без опыта / не-руководящие позиции
  - узкотехнические роли (разработка, аналитика, бухгалтерия, юристы)
  - не целевые роли (не про построение sales-команд): менеджер по работе с
    маркетплейсами, руководитель тендерного отдела / тендерный отдел,
    операционный директор маркетплейсов
  - HoReCa (рестораны / бары / кафе) — единственное исключение по отрасли
  - детейлинг / автомойки — единственное исключение по отрасли
""".strip()

RELEVANCE_THRESHOLD = 5

# ============================================================
# Параметры воронки.
# Снижены после инцидента с captcha_required от hh anti-fraud.
# ============================================================
MAX_PAGES = 1          # было 2; режет list-запросы вдвое
PER_PAGE = 100         # потолок hh API
SEARCH_PERIOD_DAYS = 2 # было 7; берём только свежие, дубликатов меньше

# ============================================================
# Sleeps подняты для снижения burstiness (всплесков частоты запросов).
# Стоимость: прогон станет ~60-90 минут вместо 45.
# Польза: hh anti-fraud не триггерится.
# ============================================================
SLEEP_BETWEEN_SEARCHES = 5      # было 2
SLEEP_BETWEEN_PAGES = 2         # было 0.5
SLEEP_BEFORE_DETAIL = 1.5       # было 1
SLEEP_AFTER_GPT = 1             # после успешного LLM-вызова


# ============================================================
# SEARCHES под руководителя продаж / клиентского сервиса.
# Запросы широкие — дадут рядовых менеджеров и разные отрасли; их отсортирует
# классификатор по РОЛИ и ГРЕЙДУ (руководящий vs рядовой). Отрасль на оценку
# не влияет — все отрасли равнозначны, кроме HoReCa и детейлинга (hard NO).
# ============================================================
SEARCHES = [
    "руководитель отдела продаж",
    "head of sales",
    "директор по продажам",
    "коммерческий директор",
    "руководитель направления продаж",
    "РОП",
    "руководитель отдела продаж B2B",
    "business development manager",
    "руководитель отдела клиентского сервиса",
]

APPLIED_FILE = "applied_ids.json"


# ============================================================
# Custom exception для circuit breaker'а на captcha.
# ============================================================
class CaptchaRequiredError(Exception):
    """hh anti-fraud потребовал капчу. Прерываем прогон, сохраняем state."""
    pass


# ============================================================
# Company blacklist.
# ============================================================
COMPANY_BLACKLIST = [
    "россельхозбанк",
    "рсхб",
]


def company_filter(vacancy):
    employer = vacancy.get("employer", {}) or {}
    employer_name = (employer.get("name") or "").strip()
    if not employer_name:
        return "pass", None
    employer_lower = employer_name.lower()
    for pattern in COMPANY_BLACKLIST:
        if pattern in employer_lower:
            return "reject", f"company: {employer_name}"
    return "pass", None


# ============================================================
# Гео-фильтр (сужен: только Москва + топ-5 городов РФ + KZ/KG).
# Правило (определяется по ГОРОДУ/area, не по стране):
#   Москва (area id=1)                     → pass при ЛЮБОМ формате.
#   СПб / Новосибирск / Екатеринбург /
#   Казань / Нижний Новгород               → pass ТОЛЬКО если формат remote.
#   Казахстан / Кыргызстан                 → pass ТОЛЬКО если формат remote.
#   Прочие города РФ (даже remote),
#   любая другая страна (напр. Ташкент/УЗ) → reject.
#
# Формат работы (remote) надёжно есть только в деталях вакансии
# (get_vacancy_detail), а не всегда в списке. Поэтому проверка формата
# вынесена на стадию ПОСЛЕ get_vacancy_detail (geo_remote_gate). На стадии
# списка geo_filter решает по городу/стране: Москва (pass), топ-5 РФ / KZ / KG
# (defer — ждём формат из деталей), всё остальное (reject).
# Так поиск и воронка не переписываются, московская ветка не меняется, а
# формат берётся из надёжного источника.
#
# Country/area ID проверены через https://api.hh.ru/areas (2026-07-01):
#   Россия=113, Казахстан=40, Кыргызстан=48 (НЕ 28 — 28 это Грузия), Москва=1.
#   Города РФ: СПб=2, Екатеринбург=3, Новосибирск=4, Нижний Новгород=66, Казань=88.
# ============================================================
MOSCOW_AREA_ID = "1"
RUSSIA_COUNTRY_ID = "113"
KZ_COUNTRY_ID = "40"
KG_COUNTRY_ID = "48"

# Топ-5 городов РФ, из которых пропускаем ТОЛЬКО remote (по area id + имени).
# Все прочие города РФ режутся, даже если формат remote.
REMOTE_RF_CITY_IDS = {"2", "3", "4", "66", "88"}
REMOTE_RF_CITY_NAMES = {
    "санкт-петербург",
    "новосибирск",
    "екатеринбург",
    "казань",
    "нижний новгород",
}

# Страны (кроме РФ), из которых пропускаем ТОЛЬКО remote. РФ намеренно НЕ здесь:
# для РФ проходят лишь города из REMOTE_RF_CITY_* (не вся страна).
GEO_ALLOWED_COUNTRY_IDS = {KZ_COUNTRY_ID, KG_COUNTRY_ID}


# ============================================================
# SEARCH_PLAN — порядок прогона. МОСКВА ПЕРВОЙ (главный рынок кандидата):
# captcha прерывает прогон в середине, поэтому Москва должна отработать до
# всего остального. Затем remote-регионы РФ (nationwide-поиск api.hh.ru; из
# них geo_filter оставит только топ-5 городов), затем Казахстан и Киргизия
# (нужен явный area — nationwide РФ их не возвращает).
# Каждый элемент — (search_text, area | None). Порядок: Москва → РФ remote →
# KZ → KG. Дубликаты между фазами отсекаются seen_ids.
# ============================================================
SEARCH_PLAN = (
    [(s, MOSCOW_AREA_ID) for s in SEARCHES]      # 1. Москва — первой, любой формат
    + [(s, None) for s in SEARCHES]              # 2. remote-регионы РФ (топ-5 через geo)
    + [(s, KZ_COUNTRY_ID) for s in SEARCHES]     # 3. Казахстан — только remote
    + [(s, KG_COUNTRY_ID) for s in SEARCHES]     # 4. Киргизия — только remote
)

# Ярлык фазы для логов (area id -> человекочитаемое имя).
AREA_LABELS = {
    None: "РФ remote",
    MOSCOW_AREA_ID: "Москва",
    KZ_COUNTRY_ID: "Казахстан",
    KG_COUNTRY_ID: "Киргизия",
}

_area_to_country = None


def _load_area_country_map():
    """Строит map area_id -> country_id из https://api.hh.ru/areas.
    Грузится один раз и кэшируется. При ошибке возвращает пустую карту
    (гео-фильтр деградирует мягко: неизвестные area деферятся, а не режутся)."""
    global _area_to_country
    if _area_to_country is not None:
        return _area_to_country
    mapping = {}
    try:
        headers = {"Authorization": f"Bearer {HH_TOKEN}"}
        r = requests.get("https://api.hh.ru/areas", headers=headers, timeout=30)
        if r.status_code == 200:
            def walk(node, country_id):
                mapping[str(node["id"])] = country_id
                for child in node.get("areas", []) or []:
                    walk(child, country_id)
            for country in r.json():
                walk(country, str(country["id"]))
        else:
            print(f"⚠️ /areas вернул {r.status_code}: гео-фильтр в fallback-режиме")
    except Exception as e:
        print(f"⚠️ Не удалось загрузить /areas ({e}): гео-фильтр в fallback-режиме")
    _area_to_country = mapping
    return mapping


def area_country_id(area_id):
    """country_id для area_id, либо None (карта недоступна / неизвестный area)."""
    if area_id is None:
        return None
    return _load_area_country_map().get(str(area_id))


def is_remote_format(vacancy):
    """True, если формат работы удалённый. Смотрит schedule ('remote') и
    work_format (['REMOTE', ...]). Надёжно доступно в get_vacancy_detail."""
    schedule = vacancy.get("schedule") or {}
    if str(schedule.get("id") or "").lower() == "remote":
        return True
    for wf in vacancy.get("work_format") or []:
        if str((wf or {}).get("id") or "").upper() == "REMOTE":
            return True
    return False


def _is_moscow(area):
    area_id = area.get("id")
    area_name = (area.get("name") or "").lower()
    return str(area_id) == MOSCOW_AREA_ID or "москва" in area_name


def _is_remote_allowed_rf_city(area):
    """True, если area — один из топ-5 городов РФ, откуда пропускаем remote
    (СПб / Новосибирск / Екатеринбург / Казань / Нижний Новгород).
    Проверяем и по area id, и по имени."""
    area_id = str(area.get("id") or "")
    area_name = (area.get("name") or "").strip().lower()
    return area_id in REMOTE_RF_CITY_IDS or area_name in REMOTE_RF_CITY_NAMES


def geo_filter(vacancy):
    """Стадия списка — решение по городу/стране, без формата.
    Возвращает (result, reason):
      "pass"   — Москва: проходит при любом формате.
      "defer"  — топ-5 городов РФ / КЗ / КГ: нужен remote,
                 финальное решение примет geo_remote_gate после деталей.
      "reject" — прочие города РФ, любая другая страна (напр. Ташкент/УЗ).
    """
    area = vacancy.get("area", {}) or {}
    area_id = area.get("id")
    area_name = (area.get("name") or "").strip()
    if area_id is None and not area_name:
        return "pass", None
    if _is_moscow(area):
        return "pass", None
    if _is_remote_allowed_rf_city(area):
        return "defer", None
    country = area_country_id(area_id)
    if country in GEO_ALLOWED_COUNTRY_IDS:
        return "defer", None
    return "reject", f"вне scope (не Москва / не топ-5 РФ / не KZ-KG): {area_name}"


def geo_remote_gate(detail):
    """Стадия после деталей для defer-вакансий. Москва — pass при любом
    формате (safety-net); топ-5 городов РФ / КЗ / КГ — pass только при remote;
    всё прочее — reject; допустимый город/страна без remote — reject."""
    area = detail.get("area", {}) or {}
    area_name = (area.get("name") or "").strip()
    if _is_moscow(area):
        return "pass", None
    allowed = (
        _is_remote_allowed_rf_city(area)
        or area_country_id(area.get("id")) in GEO_ALLOWED_COUNTRY_IDS
    )
    if not allowed:
        return "reject", f"вне scope (не Москва / не топ-5 РФ / не KZ-KG): {area_name}"
    if is_remote_format(detail):
        return "pass", None
    return "reject", f"вне Москвы без remote: {area_name}"


# ============================================================
# Префильтр по тайтлу.
# Под руководителя продаж: пропускаем руководящие sales-роли, режем
# junior/стажёров и явно не-sales узкотехнические роли. Рядовой
# "менеджер по продажам" регуляркой НЕ режем — грейд надёжнее определит
# LLM по описанию (рядовость тонко отличается от руководящей роли).
# ============================================================
TITLE_WHITELIST = [
    r"\bруководитель\s+отдела\s+продаж\b",
    r"\bруководитель\s+направления\s+продаж\b",
    r"\bруководитель\s+группы\s+продаж\b",
    r"\bначальник\s+отдела\s+продаж\b",
    r"\bhead\s+of\s+sales\b",
    r"\bдиректор\s+по\s+продажам\b",
    r"\bкоммерческий\s+директор\b",
    r"\bроп\b",
    r"\bbusiness\s+development\s+manager\b",
    r"\bsales\s+(lead|head|director)\b",
    r"\b(lead|head|director)\s+sales\b",
    r"\bруководитель\s+(отдела\s+)?клиентского\s+сервиса\b",
    r"\bруководитель\s+(отдела\s+)?клиентского\s+обслуживания\b",
    r"\bначальник\s+отдела\s+клиентского\s+(сервиса|обслуживания)\b",
    r"\bhead\s+of\s+customer\s+(service|support)\b",
    r"\bcustomer\s+service\s+(head|lead|director)\b",
]

TITLE_BLACKLIST = [
    r"\bjunior\b",
    r"\bстажёр\b",
    r"\bстажер\b",
    r"\bintern\b",
    r"\btrainee\b",
    r"\bразработчик\b",
    r"\bпрограммист\b",
    r"\bback[\s-]?end\b",
    r"\bfront[\s-]?end\b",
    r"\bfull[\s-]?stack\b",
    r"\bdevops\b",
    r"\bбухгалтер\b",
    r"\bюрист\b",
    r"\bдизайнер\b",
    # Не целевые роли (не про построение sales-команд) — см. is_relevant:
    r"\bмаркетплейс",   # менеджер по работе с маркетплейсами, операционный директор маркетплейсов
    r"\bтендерн",       # руководитель тендерного отдела / тендерный отдел
]


def prefilter_by_title(title):
    title_lower = title.lower()
    for pattern in TITLE_BLACKLIST:
        if re.search(pattern, title_lower):
            return "reject", f"blacklist: {pattern}"
    for pattern in TITLE_WHITELIST:
        if re.search(pattern, title_lower):
            return "fast_track", None
    return "pass", None


# ============================================================
# Captcha detection — общая утилита.
# Любой ответ, содержащий "captcha_required", — сигнал прерывать прогон.
# ============================================================
def is_captcha_response(response_text):
    """Проверяет, содержит ли ответ hh captcha challenge."""
    if not response_text:
        return False
    return "captcha_required" in response_text


# ============================================================
# applied_ids — incremental save.
# Раньше save был один в конце main(). Если прогон падал в середине
# (как на капче), все накопленные ID терялись.
# Теперь сохраняем после каждого нового отклика.
# ============================================================
def get_applied_ids():
    url = f"https://api.github.com/repos/{REPO}/contents/{APPLIED_FILE}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        content = r.json()["content"]
        sha = r.json()["sha"]
        data = json.loads(base64.b64decode(content).decode())
        return data, sha
    return [], None


def save_applied_ids(ids, sha):
    """Сохраняет applied_ids в репо. Возвращает новый sha."""
    url = f"https://api.github.com/repos/{REPO}/contents/{APPLIED_FILE}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }
    content = base64.b64encode(json.dumps(ids).encode()).decode()
    body = {
        "message": f"Update applied ids {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "content": content
    }
    if sha:
        body["sha"] = sha
    r = requests.put(url, headers=headers, json=body)
    if r.status_code in [200, 201]:
        return r.json()["content"]["sha"]
    print(f"⚠️ Не удалось сохранить applied_ids.json: {r.status_code} {r.text[:200]}")
    return sha


# ============================================================
# hh API клиенты с retry + exponential backoff + captcha detection.
# ============================================================
def get_vacancies(search, area=None):
    """Получаем вакансии с pagination.

    area=None — поведение как раньше (без area, регионы РФ режутся постфактум
    в geo_filter). area=<id> — явный регион (используется для ветки Алматы).
    """
    all_items = []
    for page in range(MAX_PAGES):
        url = "https://api.hh.ru/vacancies"
        params = {
            "text": search,
            "period": SEARCH_PERIOD_DAYS,
            "per_page": PER_PAGE,
            "page": page,
            "order_by": "publication_time",
        }
        if area is not None:
            params["area"] = area
        headers = {"Authorization": f"Bearer {HH_TOKEN}"}

        page_items = []
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, headers=headers, timeout=30)

                if is_captcha_response(r.text):
                    raise CaptchaRequiredError(
                        f"captcha при get_vacancies(search='{search}', page={page})"
                    )

                if r.status_code == 200:
                    page_items = r.json().get("items", [])
                    break

                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"⚠️ hh.ru {r.status_code} (search='{search}', page={page}), "
                      f"ретрай {attempt+1}/3 через {wait:.1f}с")
                time.sleep(wait)

            except CaptchaRequiredError:
                raise
            except Exception as e:
                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"⚠️ Ошибка hh.ru: {e}, ретрай {attempt+1}/3 через {wait:.1f}с")
                time.sleep(wait)

        all_items.extend(page_items)

        if len(page_items) < PER_PAGE:
            break

        time.sleep(SLEEP_BETWEEN_PAGES)

    return all_items


def get_vacancy_detail(vacancy_id):
    """Детали вакансии с retry на 4xx/5xx и captcha detection."""
    url = f"https://api.hh.ru/vacancies/{vacancy_id}"
    headers = {"Authorization": f"Bearer {HH_TOKEN}"}

    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=30)

            if is_captcha_response(r.text):
                raise CaptchaRequiredError(
                    f"captcha при get_vacancy_detail({vacancy_id})"
                )

            if r.status_code == 200:
                return r.json()

            if r.status_code in (404, 410):
                return {}

            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"⚠️ hh.ru detail {r.status_code} для {vacancy_id}, "
                  f"ретрай {attempt+1}/3 через {wait:.1f}с")
            time.sleep(wait)

        except CaptchaRequiredError:
            raise
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"⚠️ Соединение detail: {e}, ретрай {attempt+1}/3 через {wait:.1f}с")
            time.sleep(wait)

    return {}


# ============================================================
# OpenAI API клиенты.
# ============================================================
def ask_gpt(system, user):
    """GPT-вызов в свободном режиме (для cover letter)."""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
        "Content-Type": "application/json"
    }
    body = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        "max_tokens": 1000
    }
    r = requests.post(url, headers=headers, json=body)
    time.sleep(2)
    result = r.json()
    if "choices" not in result:
        print(f"⚠️ GPT ответил неожиданно: {result}")
        return "ПРОПУСТИТЬ"
    return result["choices"][0]["message"]["content"]


def ask_gpt_json(system, user, max_retries=3):
    """LLM-вызов с гарантированным JSON output."""
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
        "Content-Type": "application/json"
    }
    body = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        "max_tokens": 500,
        "temperature": 0.1,
        "response_format": {"type": "json_object"}
    }

    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=30)

            if r.status_code == 429:
                wait = 2 ** attempt
                print(f"⏳ OpenAI rate limit, ждём {wait}с")
                time.sleep(wait)
                continue

            if r.status_code != 200:
                print(f"⚠️ GPT HTTP {r.status_code}: {r.text[:200]}")
                time.sleep(2 ** attempt)
                continue

            result = r.json()
            if "choices" not in result:
                print(f"⚠️ GPT неожиданный ответ: {result}")
                time.sleep(2 ** attempt)
                continue

            time.sleep(SLEEP_AFTER_GPT)
            return result["choices"][0]["message"]["content"]

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Сетевая ошибка GPT: {e}, ретрай {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)

    return '{"decision": "no", "match_score": 0, "tier": "api_error", "concerns": ["API failure"], "reason": "API недоступен после 3 попыток"}'


# ============================================================
# LLM-классификатор.
# ============================================================
def is_relevant(vacancy):
    system = """Ты — ассистент, помогающий руководителю отдела продаж / клиентского сервиса оценивать релевантность вакансий с hh.ru.

Твоя задача — для каждой вакансии вернуть СТРОГО валидный JSON с оценкой релевантности.

ВАЖНО: гео-фильтрация и фильтр по работодателю уже выполнены на предыдущих шагах. Здесь оценивай ТОЛЬКО роль и грейд. Географию и название компании в reason не упоминай.

=== ПРОФИЛЬ КАНДИДАТА ===
""" + CANDIDATE_PROFILE + """

=== КАК ОЦЕНИВАТЬ ===

Главное правило: оценка идёт по РОЛИ и ГРЕЙДУ, НЕ по отрасли. Все отрасли
равнозначны. НЕ повышай оценку за IT/tech и НЕ понижай за другие отрасли
(логистика, производство, финтех, ритейл, услуги, оптовая торговля и т.д.) —
они равны. Единственное исключение по отрасли — HoReCa и детейлинг (hard NO).

Шаг 1. Тип роли (поле tier):
  - "sales_lead": руководящая роль в продажах
  - "cs_lead": руководящая роль в клиентском сервисе / обслуживании
  - "out_of_scope": рядовая / не-sales-не-CS роль либо hard NO

Шаг 2. Hard NO признаки → decision="no", score=0-2:
  - рядовой менеджер по продажам / клиентский менеджер БЕЗ управления командой —
    это НЕ руководящая роль. "Менеджер по продажам", "специалист по продажам",
    "менеджер по работе с клиентами" без подчинённых → NO.
  - junior / стажёр / без опыта / любая не-руководящая позиция
  - не-sales / не-CS роли: разработка, аналитика, бухгалтерия, юристы, дизайн,
    маркетинг/SMM, HR — всё, что не про управление продажами или клиентским сервисом
  - НЕ ЦЕЛЕВЫЕ РОЛИ (не про построение sales-команд) → out_of_scope, decision="no":
    "менеджер по работе с маркетплейсами", "руководитель тендерного отдела" /
    "тендерный отдел", "операционный директор маркетплейсов". Это управление
    закупками/тендерами/каналом маркетплейсов, а не построение отдела продаж.
  - ИСКЛЮЧЕНИЕ ПО ОТРАСЛИ (единственное): HoReCa (рестораны / бары / кафе) и
    детейлинг / автомойки → hard NO даже для руководящей роли.

Шаг 3. Позитивные сигналы (руководящая роль):
  - Продажи: Руководитель отдела продаж (РОП), Head of Sales, Директор по продажам,
    Коммерческий директор, Руководитель направления/группы продаж,
    Business Development Manager, Sales Lead/Head/Director
  - Клиентский сервис: Руководитель отдела клиентского сервиса / клиентского
    обслуживания / поддержки клиентов, Head of Customer Service, Customer Service Lead
  - построение отдела с нуля, KPI, системы мотивации, найм и обучение команды,
    внедрение CRM (AmoCRM), управление воронкой, наличие подчинённых

Шаг 4. ГРЕЙД — критично отличать руководящую роль от рядовой:
  - РОП / Head of Sales / Директор по продажам / Коммерческий директор /
    руководитель направления продаж / руководитель отдела клиентского сервиса
    → руководящая, ДА.
  - "Менеджер по продажам" / "специалист по продажам" / "клиентский менеджер"
    без управления командой → рядовая, НЕТ.
  - Если из описания видно управление командой/отделом, KPI команды, найм —
    это руководящая роль, принимаем, даже если в заголовке просто "Sales Manager".

Шаг 5. match_score 0-10 (по роли и грейду, НЕ по отрасли):
  - 8-10: чёткая руководящая sales/CS-роль без red flags (в ЛЮБОЙ отрасли)
  - 5-7: руководящая роль, грейд слегка неоднозначен, но красных флагов нет
  - 3-4: пограничная зона (грейд неясен / гибридная роль)
  - 0-2: hard NO (рядовой, не-sales-не-CS, junior, HoReCa, детейлинг)

Шаг 6. decision: "yes" — score >= 7, "maybe" — 5-6, "no" — <= 4

=== FEW-SHOT ===

Пример 1:
Вакансия: "Руководитель отдела продаж в IT-интегратора"
Ответ: {"decision": "yes", "match_score": 9, "tier": "sales_lead", "concerns": [], "reason": "Руководящая sales-роль — прямое попадание."}

Пример 2:
Вакансия: "Руководитель отдела продаж в логистическую компанию"
Ответ: {"decision": "yes", "match_score": 9, "tier": "sales_lead", "concerns": [], "reason": "Руководящая sales-роль. Отрасль значения не имеет — все отрасли равнозначны."}

Пример 3:
Вакансия: "Коммерческий директор производственной компании"
Ответ: {"decision": "yes", "match_score": 8, "tier": "sales_lead", "concerns": [], "reason": "Руководящая роль в продажах, отрасль равнозначна."}

Пример 4:
Вакансия: "Менеджер по продажам"
Ответ: {"decision": "no", "match_score": 2, "tier": "out_of_scope", "concerns": ["Нет признаков управления командой"], "reason": "Рядовая sales-роль, не руководящая."}

Пример 5:
Вакансия: "Руководитель отдела продаж в сеть ресторанов"
Ответ: {"decision": "no", "match_score": 1, "tier": "out_of_scope", "concerns": ["HoReCa"], "reason": "Руководящая, но отрасль HoReCa — hard NO."}

Пример 6:
Вакансия: "Backend-разработчик"
Ответ: {"decision": "no", "match_score": 0, "tier": "out_of_scope", "concerns": ["Не sales/CS"], "reason": "Техническая роль, не про управление продажами."}

Пример 7:
Вакансия: "Руководитель отдела клиентского сервиса"
Ответ: {"decision": "yes", "match_score": 8, "tier": "cs_lead", "concerns": [], "reason": "Руководящая роль в клиентском сервисе — целевая."}

Пример 8:
Вакансия: "Руководитель отдела продаж B2B (телеком)"
Ответ: {"decision": "yes", "match_score": 8, "tier": "sales_lead", "concerns": [], "reason": "Руководящая B2B sales-роль, отрасль равнозначна."}

=== ФОРМАТ ОТВЕТА ===

Верни СТРОГО JSON без markdown:
{"decision": "yes"|"maybe"|"no", "match_score": <0-10>, "tier": "sales_lead"|"cs_lead"|"out_of_scope", "concerns": [...], "reason": "..."}
"""

    user = f"""Вакансия: {vacancy['name']}
Компания: {vacancy.get('employer', {}).get('name', '')}
Город: {vacancy.get('area', {}).get('name', 'не указан')}
График: {vacancy.get('schedule', {}).get('name', 'не указан')}
Описание: {vacancy.get('description', '')[:2500]}

Оцени релевантность."""

    raw_answer = ask_gpt_json(system, user)

    try:
        result = json.loads(raw_answer)
        decision = result.get("decision", "no")
        score = int(result.get("match_score", 0))
        tier = result.get("tier", "unknown")
        reason = result.get("reason", "")
        concerns = result.get("concerns", [])
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        print(f"⚠️ JSON parse error: {e} | raw: {raw_answer[:200]}")
        return False, {
            "decision": "no",
            "match_score": 0,
            "tier": "parse_error",
            "reason": f"parse error: {raw_answer[:100]}",
            "concerns": ["JSON parsing failed"]
        }

    classification = {
        "decision": decision,
        "match_score": score,
        "tier": tier,
        "reason": reason,
        "concerns": concerns
    }
    is_match = score >= RELEVANCE_THRESHOLD
    return is_match, classification


def write_cover_letter(vacancy):
    system = """Ты помогаешь писать сопроводительные письма к вакансиям. Твоя задача — чтобы письмо звучало так, будто его написал живой человек, который реально прочитал вакансию, а не сгенерировала нейросеть.

СНАЧАЛА ПОДУМАЙ (про себя, в письмо это не попадает):
Прочитай описание вакансии и найди 1-2 КОНКРЕТНЫЕ детали: что за бизнес, какую задачу по продажам решает компания (построить отдел с нуля, вырасти в выручке, выйти в новый сегмент, перестроить процессы), какой рынок или сегмент (B2B, enterprise, конкретная отрасль). Письмо должно отталкиваться ИМЕННО от этих деталей, а не от общих фраз об опыте. Если конкретики мало - лучше короче и честнее, чем выдуманный энтузиазм.

КАК ДОЛЖНО ЗВУЧАТЬ ХОРОШЕЕ ПИСЬМО:
Будто человек прочитал вакансию и заинтересовался конкретикой. Структура из двух частей:
(1) что обратило внимание / какую задачу ты увидел в вакансии - со ссылкой на конкретную деталь из описания;
(2) что ты делал похожего и какой получался результат - коротко, по-человечески, без перечисления компетенций.
Показывай, что ты ПОНЯЛ задачу бизнеса и решал похожее. Глаголы действия: "строил", "запускал", "выстраивал", "настраивал", "вырастил", "внедрял", "управлял".

ЗАПРЕЩЁННЫЙ КАНЦЕЛЯРИТ (машинные маркеры, никогда не используй): "мой опыт включает", "мой опыт позволяет", "обладаю компетенциями", "обладаю навыками", "имею опыт в", "хочу применить свои знания", "буду рад применить компетенции", "хочу предложить кандидатуру", "очень хочу". Вместо "обладаю навыками" / "имею опыт" - просто "умею" или показывай через действие, что делал.

ПРИМЕР ТОНА (не шаблон, под каждую вакансию зацепка своя):
ПЛОХО: "Мой опыт включает управление продажами и позволяет применить компетенции в B2B."
ХОРОШО: "Обратил внимание, что нужно построить отдел продаж с нуля - делал это дважды: нанимал команду, ставил KPI, внедрял CRM и систему мотивации. В одном из проектов за полгода вывел отдел на стабильный план."

ЧЕСТНОСТЬ:
- Не выдумывай факты о компании, которых нет в описании.
- Зацепка опирается на то, что РЕАЛЬНО написано в вакансии.
- НИКОГДА не указывай конкретное число лет опыта или стажа в тексте письма - ни "16 лет", ни любую другую цифру. Опыт кандидата разнородный, общий стаж вводит в заблуждение. Показывай опыт ТОЛЬКО через конкретные действия и результаты (что строил, что вырастил, что внедрял), а не через счётчик лет.

СТРОГИЕ ПРАВИЛА:
1. Начинай ВСЕГДА: "Добрый день! Заинтересовала вакансия [название вакансии]"
2. Заканчивай ВСЕГДА: "С уважением, Алексей. тг @alexotkidach"
3. Максимум 2 абзаца, чётко и по делу.
4. Предложения максимально простые.
5. НЕЛЬЗЯ упоминать названия проектов и работодателей - обобщённо.
6. НЕЛЬЗЯ добавлять контакты (телефон, email).
7. НЕЛЬЗЯ слово "очень".
8. НЕЛЬЗЯ метафоры и пафос.
9. НЕЛЬЗЯ оценочные слова про опыт: "большой", "успешный", "успешно", "результативный", "глубокий", "уверенно".
10. НЕЛЬЗЯ указывать число лет опыта/стажа (см. блок ЧЕСТНОСТЬ).
11. Кандидат - мужчина. Мужской род: "управлял", "строил", "работал", "выстраивал".
12. ЗАПРЕЩЕНО длинное тире "—" и короткое "–". Только дефис "-" или запятая.
13. Проверяй орфографию и грамматику.
14. От первого лица, на русском."""

    user = f"""Профиль кандидата:
{PROFILE}

Вакансия: {vacancy['name']}
Компания: {vacancy.get('employer', {}).get('name', '')}
Описание: {vacancy.get('description', '')[:2000]}

Напиши сопроводительное письмо."""
    letter = ask_gpt(system, user)

    # Пост-обработка: гарантированно убираем тире (маркер AI-текста),
    # независимо от того, послушалась ли модель.
    letter = letter.replace(" — ", " - ").replace("—", "-")
    letter = letter.replace(" – ", " - ").replace("–", "-")

    return letter


# ============================================================
# apply() с обработкой 4xx — self-healing на «уже откликались».
# ============================================================
def apply(vacancy_id, resume_id, cover_letter):
    url = "https://api.hh.ru/negotiations"
    headers = {
        "Authorization": f"Bearer {HH_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "vacancy_id": vacancy_id,
        "resume_id": resume_id,
        "message": cover_letter
    }
    r = requests.post(url, headers=headers, data=data)

    if is_captcha_response(r.text):
        raise CaptchaRequiredError(f"captcha при apply({vacancy_id})")

    already_applied_signals = [
        "already_applied",
        "negotiation_exists",
        "vacancy_negotiation_already_exists",
        "negotiations_limit_exceeded",
    ]
    response_text_lower = r.text.lower()
    is_already_applied = any(sig in response_text_lower for sig in already_applied_signals)

    error_text = r.text[:300]

    return r.status_code, is_already_applied, error_text


def get_resume_id():
    url = "https://api.hh.ru/resumes/mine"
    headers = {"Authorization": f"Bearer {HH_TOKEN}"}
    r = requests.get(url, headers=headers)
    if is_captcha_response(r.text):
        raise CaptchaRequiredError("captcha при get_resume_id")
    resumes = r.json().get("items", [])
    if resumes:
        return resumes[0]["id"]
    return None


# ============================================================
# main — с circuit breaker на CaptchaRequiredError.
# ============================================================
def main():
    try:
        resume_id = get_resume_id()
    except CaptchaRequiredError as e:
        print(f"🛑 {e}")
        print("🛑 hh заблокировал на этапе get_resume_id. Прогон прерван.")
        return 1

    if not resume_id:
        print("Резюме не найдено!")
        return 1

    applied_ids, sha = get_applied_ids()
    applied = []
    skipped_by_llm = []
    skipped_by_prefilter = []
    skipped_by_geo = []
    skipped_by_company = []
    error_samples = []
    seen_ids = set()

    funnel = {
        "fetched_total": 0,
        "duplicates_in_search": 0,
        "already_applied": 0,
        "company_rejected": 0,
        "geo_rejected": 0,
        "prefilter_rejected": 0,
        "prefilter_fast_track": 0,
        "prefilter_pass": 0,
        "detail_fetch_failed": 0,
        "llm_rejected": 0,
        "llm_approved": 0,
        "applied_success": 0,
        "applied_failed": 0,
        "applied_already": 0,
    }

    captcha_hit = False

    try:
        for search, area in SEARCH_PLAN:
            print(f"\n🔍 Ищем: {search} [{AREA_LABELS.get(area, area)}]")
            vacancies = get_vacancies(search, area=area)
            funnel["fetched_total"] += len(vacancies)

            for v in vacancies:
                if v['id'] in seen_ids:
                    funnel["duplicates_in_search"] += 1
                    continue
                if v['id'] in applied_ids:
                    funnel["already_applied"] += 1
                    continue
                seen_ids.add(v['id'])

                company_result, company_reason = company_filter(v)
                if company_result == "reject":
                    funnel["company_rejected"] += 1
                    skipped_by_company.append({
                        "name": v['name'],
                        "employer": v.get('employer', {}).get('name', ''),
                        "reason": company_reason
                    })
                    continue

                geo_result, geo_reason = geo_filter(v)
                if geo_result == "reject":
                    funnel["geo_rejected"] += 1
                    skipped_by_geo.append({
                        "name": v['name'],
                        "area": v.get('area', {}).get('name', ''),
                        "reason": geo_reason
                    })
                    continue
                # "defer" — страна допустима, но формат (remote) проверим
                # после get_vacancy_detail, где поле надёжно (см. geo_remote_gate).
                geo_needs_remote = (geo_result == "defer")

                prefilter_result, prefilter_reason = prefilter_by_title(v['name'])
                if prefilter_result == "reject":
                    funnel["prefilter_rejected"] += 1
                    skipped_by_prefilter.append({
                        "name": v['name'],
                        "reason": prefilter_reason
                    })
                    continue

                if prefilter_result == "fast_track":
                    funnel["prefilter_fast_track"] += 1
                else:
                    funnel["prefilter_pass"] += 1

                time.sleep(SLEEP_BEFORE_DETAIL)
                detail = get_vacancy_detail(v['id'])
                if not detail or 'name' not in detail:
                    print(f"⚠️ Не получили детали, пропускаем")
                    funnel["detail_fetch_failed"] += 1
                    continue

                # Гео-формат: для не-московских (defer) вакансий пропускаем
                # только remote. Москва прошла на стадии списка и сюда не
                # доходит с флагом defer.
                if geo_needs_remote:
                    geo2_result, geo2_reason = geo_remote_gate(detail)
                    if geo2_result == "reject":
                        funnel["geo_rejected"] += 1
                        skipped_by_geo.append({
                            "name": v['name'],
                            "area": detail.get('area', {}).get('name', ''),
                            "reason": geo2_reason
                        })
                        continue

                is_match, classification = is_relevant(detail)

                log_prefix = (
                    f"[score={classification['match_score']}, "
                    f"tier={classification['tier']}, "
                    f"decision={classification['decision']}]"
                )

                if is_match:
                    funnel["llm_approved"] += 1
                    print(f"✅ Подходит {log_prefix}: {v['name']} — {v.get('employer', {}).get('name', '')}")
                    print(f"   reason: {classification['reason']}")
                    if classification['concerns']:
                        print(f"   concerns: {'; '.join(classification['concerns'])}")

                    if detail.get('response_letter_required'):
                        print(f"✉️ Письмо обязательно — пишем...")
                        letter = write_cover_letter(detail)
                    else:
                        letter = ""

                    status, is_already, error_text = apply(v['id'], resume_id, letter)

                    if status in [200, 201]:
                        print(f"📨 Отклик отправлен!")
                        applied_ids.append(v['id'])
                        applied.append(f"{v['name']} — {v.get('employer', {}).get('name', '')}")
                        funnel["applied_success"] += 1
                        sha = save_applied_ids(applied_ids, sha)
                    elif is_already:
                        print(f"⏭️ hh говорит «уже откликались», добавляем в state")
                        applied_ids.append(v['id'])
                        funnel["applied_already"] += 1
                        sha = save_applied_ids(applied_ids, sha)
                    else:
                        print(f"⚠️ Ошибка отклика: {status} | {error_text}")
                        funnel["applied_failed"] += 1
                        vacancy_url = detail.get('alternate_url') or f"https://hh.ru/vacancy/{v['id']}"
                        error_samples.append({
                            "name": v['name'],
                            "employer": v.get('employer', {}).get('name', ''),
                            "status": status,
                            "error": error_text,
                            "url": vacancy_url
                        })
                else:
                    funnel["llm_rejected"] += 1
                    print(f"❌ Не подходит {log_prefix}: {v['name']}")
                    print(f"   reason: {classification['reason']}")
                    skipped_by_llm.append({
                        "name": v['name'],
                        "score": classification['match_score'],
                        "tier": classification['tier'],
                        "reason": classification['reason']
                    })

            time.sleep(SLEEP_BETWEEN_SEARCHES)

    except CaptchaRequiredError as e:
        print(f"\n🛑 CIRCUIT BREAKER: {e}")
        print(f"🛑 hh anti-fraud сработал. Прогон прерван, чтобы не усугублять блокировку.")
        print(f"🛑 Уже отправленные {funnel['applied_success']} откликов сохранены.")
        captcha_hit = True

    sha = save_applied_ids(applied_ids, sha)

    print("\n" + "=" * 60)
    print("📊 ВОРОНКА:")
    print("=" * 60)
    if captcha_hit:
        print("  ⚠️ ПРОГОН ПРЕРВАН ПО CAPTCHA — данные ниже частичные ⚠️")
        print()
    unique_after_dedup = funnel["fetched_total"] - funnel["duplicates_in_search"]
    fresh_vacancies = unique_after_dedup - funnel["already_applied"]
    print(f"  Получено от hh:            {funnel['fetched_total']}")
    print(f"  Дубликаты между search:    -{funnel['duplicates_in_search']}")
    print(f"  Уникальных:                {unique_after_dedup}")
    print(f"  Уже откликались:           -{funnel['already_applied']}")
    print(f"  Свежих к обработке:        {fresh_vacancies}")
    print(f"  ────────────────────────────")
    print(f"  Отсеяно по company:        -{funnel['company_rejected']}")
    print(f"  Отсеяно гео-фильтром:      -{funnel['geo_rejected']}")
    print(f"  Отсеяно префильтром:       -{funnel['prefilter_rejected']}")
    print(f"  Прошло префильтр:          {funnel['prefilter_fast_track'] + funnel['prefilter_pass']}")
    print(f"    └ fast-track:              {funnel['prefilter_fast_track']}")
    print(f"    └ pass:                    {funnel['prefilter_pass']}")
    print(f"  Не получили детали:        -{funnel['detail_fetch_failed']}")
    print(f"  ────────────────────────────")
    print(f"  LLM одобрила:              {funnel['llm_approved']}")
    print(f"  LLM отклонила:             {funnel['llm_rejected']}")
    print(f"  ────────────────────────────")
    print(f"  Откликов отправлено:       {funnel['applied_success']}")
    print(f"  «Уже откликались» (heal):  {funnel['applied_already']}")
    print(f"  Ошибок отклика:            {funnel['applied_failed']}")

    print("\n" + "=" * 60)
    print("📊 ИТОГ ПО ОТКЛИКАМ:")
    print("=" * 60)
    print(f"Откликнулся: {len(applied)}")
    for a in applied:
        print(f"  ✅ {a}")

    maybe_zone = [s for s in skipped_by_llm if s['score'] in (5, 6)]
    mid_zone = [s for s in skipped_by_llm if 3 <= s['score'] <= 4]
    hard_no = [s for s in skipped_by_llm if s['score'] <= 2]

    print(f"\n🟡 Maybe-зона (5-6): {len(maybe_zone)}")
    for s in maybe_zone[:15]:
        print(f"  [{s['score']}] {s['name']} — {s['reason']}")

    print(f"\n🟠 Mid-зона (3-4): {len(mid_zone)}")
    for s in mid_zone[:5]:
        print(f"  [{s['score']}] {s['name']} — {s['reason']}")

    print(f"\n🔴 Hard NO (LLM): {len(hard_no)}")

    print(f"\n🔴 Company-фильтр: {len(skipped_by_company)}")
    if skipped_by_company:
        company_counts = Counter(s['employer'] for s in skipped_by_company)
        for emp, cnt in company_counts.most_common(10):
            print(f"  [{cnt}x] {emp}")

    print(f"\n🔴 Гео: {len(skipped_by_geo)}")
    if skipped_by_geo:
        geo_counts = Counter(s['area'] for s in skipped_by_geo)
        for area, cnt in geo_counts.most_common(10):
            print(f"  [{cnt}x] {area}")

    print(f"\n🔴 Префильтр: {len(skipped_by_prefilter)}")
    if skipped_by_prefilter:
        reason_counts = Counter(s['reason'] for s in skipped_by_prefilter)
        for reason, cnt in reason_counts.most_common(10):
            print(f"  [{cnt}x] {reason}")

    if error_samples:
        print(f"\n🔴 Ошибки отклика по типам: {len(error_samples)}")
        error_types = []
        test_required = []
        for s in error_samples:
            err_type = None
            try:
                parsed = json.loads(s['error'])
                errors = parsed.get('errors', [])
                if errors:
                    err_type = errors[0].get('type')
            except (ValueError, AttributeError, TypeError):
                err_type = None
            error_types.append(err_type or str(s['status']))
            err_blob = f"{err_type or ''} {s.get('error', '')}".lower()
            if 'test' in err_blob or err_type == 'negotiations':
                test_required.append(s)
        error_counts = Counter(error_types)
        for err_type, cnt in error_counts.most_common(10):
            print(f"  [{cnt}x] {err_type}")

        if test_required:
            print(f"\n📋 Вакансии с обязательным тестом — откликнуться вручную:")
            for s in test_required:
                print(f"  • {s['name']} — {s.get('employer', '')}")
                print(f"    {s.get('url', '')}")

    return 1 if captcha_hit else 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
