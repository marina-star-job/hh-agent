import os
import re
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
# Полный PROFILE используется в write_cover_letter (там нужен контекст).
# Для классификации хватает 500-800 символов с ключевыми сигналами —
# это экономит токены (на каждый прогон сотни LLM-вызовов).
# ============================================================
CANDIDATE_PROFILE = """
Кандидат: ML/AI Project Manager, женщина, 32 года, Москва.
Опыт: ~5.5 лет (Product Manager → Project Manager → ML PM).
Текущая роль: ML PM в банке (NLP/LLM/RAG проекты).

Грейд: middle → middle+ → senior-light.
Сильные стороны: LLM, NLP, RAG, semantic search, оценка ML-моделей
(Precision/Recall/F1, BERTScore), Agile/Scrum, бизнес-анализ, CustDev,
Jira/Confluence, Swagger.

Целевые роли: Product Manager, Project Manager, Product Owner,
ML PM, AI PM, Program Manager, Delivery Manager, Scrum Master,
Руководитель проектов / продукта.

Приоритет доменов:
  TIER 1 (best fit): AI / ML / NLP / LLM / RAG
  TIER 2 (strong): HR-tech, IT/SaaS, продуктовые компании
  TIER 3 (ok): финтех, банки, EdTech, e-commerce, прочий IT

Lead/Head-of роли: ОК только в стартапах и AI-командах до ~20 человек.
В крупных корпорациях — нет.

Hard NO:
  - 1С (любые роли)
  - продажи / B2B sales / аккаунт-менеджмент
  - дизайн / арт-директор / UX-lead
  - чистая разработка (Backend/Frontend/Data Engineer)
  - HR-роли (HR BP, HRD, рекрутер)
  - маркетинг / SMM / контент
  - стажировки / junior
  - C-level в крупных корпорациях (CPO, CTO, VP, Director)
""".strip()

# Порог match_score, начиная с которого откликаемся.
RELEVANCE_THRESHOLD = 6

# Параметры воронки (top of the funnel).
MAX_PAGES = 2          # сколько страниц hh обходим на каждый search query
PER_PAGE = 100         # потолок hh API на одну страницу

SEARCHES = [
    # Широкие запросы по роли
    "Product Manager",
    "Project Manager",
    "Руководитель проектов",
    "Руководитель продукта",
    "Менеджер продукта",
    "Владелец продукта",
    "Product Owner",
    # AI/ML направление
    "руководитель AI",
    "руководитель ML",
    "AI менеджер",
    "ML менеджер",
    "AI Project",
    "ML Project",
    # Финтех и банки
    "Product Manager банк",
    "Project Manager финтех",
    # IT продукты
    "Product Manager IT",
    "Project Manager IT",
]

APPLIED_FILE = "applied_ids.json"

# ============================================================
# Company blacklist — компании, на которые не откликаемся.
# Срабатывает первым в воронке (Stage 0) — до гео, до префильтра,
# до LLM. Самая дешёвая отсечка: проверка подстроки в employer.name.
#
# Паттерны case-insensitive substring match. Подстрока "рсхб" ловит
# все дочки (РСХБ-Интех, РСХБ-Страхование) — broad match by design.
# ============================================================
COMPANY_BLACKLIST = [
    "россельхозбанк",   # ловит "АО Россельхозбанк", 'АО "Россельхозбанк"'
    "рсхб",             # ловит "РСХБ", "РСХБ-Интех", "РСХБ-Страхование"
]


def company_filter(vacancy):
    """Stage 0: отсев по работодателю.

    Returns:
        ("pass", None) — компания не в blacklist
        ("reject", reason) — попала в blacklist
    """
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
# Гео-фильтр: где работа допустима.
#
# Правило: Москва ИЛИ ближнее МО (де-факто Москва) ИЛИ зарубежье.
# Дальнее Подмосковье и другие регионы РФ — reject.
#
# Реализация: hh API в area.name отдаёт конкретный город. Москва
# определяется по подстроке "москва". Регионы РФ определяются через
# blacklist (RUSSIAN_REGIONS_BLACKLIST). Всё, что не в blacklist
# и не Москва — считаем зарубежьем.
#
# Это hybrid filter — детерминированный сигнал (город) проверяем
# в коде, не в LLM.
# ============================================================

# Ближнее МО — фактически часть Москвы, оставляем.
NEAR_MOSCOW_REGION = [
    "химки", "реутов", "мытищи", "балашиха",
    "королёв", "королев",
    "люберцы", "красногорск", "одинцово",
    "долгопрудный", "котельники", "видное",
    "дзержинский", "лыткарино", "юбилейный",
    "железнодорожный",
]

# Регионы РФ кроме Москвы и ближнего МО — режем.
# Включает крупные города + дальнее Подмосковье.
RUSSIAN_REGIONS_BLACKLIST = [
    # Дальнее МО
    "московская область", "подмосковье",
    "подольск", "серпухов", "клин",
    "сергиев посад", "раменское", "жуковский",
    "чехов", "дмитров", "истра", "можайск",
    "ногинск", "электросталь", "орехово-зуево",
    "пушкино", "щёлково", "щелково",
    "звенигород", "коломна", "наро-фоминск",

    # Крупные города РФ
    "санкт-петербург", "спб", "питер",
    "новосибирск", "екатеринбург", "нижний новгород",
    "казань", "челябинск", "омск", "самара",
    "ростов-на-дону", "ростов",
    "уфа", "красноярск", "воронеж", "пермь",
    "волгоград", "краснодар", "саратов",
    "тюмень", "тольятти", "ижевск", "барнаул",
    "ульяновск", "иркутск", "хабаровск",
    "ярославль", "владивосток", "махачкала",
    "оренбург", "томск", "кемерово", "новокузнецк",
    "рязань", "астрахань", "набережные челны",
    "пенза", "липецк", "киров",
    "тула", "чебоксары", "калининград",
    "брянск", "курск", "иваново",
    "магнитогорск", "тверь", "ставрополь",
    "симферополь", "белгород", "архангельск",
    "владимир", "сочи", "курган", "смоленск",
    "калуга", "чита", "орёл", "орел", "волжский",
    "череповец", "владикавказ", "мурманск",
    "сургут", "вологда", "тамбов", "стерлитамак",
    "грозный", "якутск", "кострома",
    "комсомольск-на-амуре", "петрозаводск",
    "таганрог", "нижневартовск", "йошкар-ола",
    "братск", "новороссийск", "дзержинск",
    "шахты", "нальчик", "орск", "сыктывкар",
    "нижнекамск", "ангарск", "старый оскол",
    "великий новгород", "благовещенск", "псков",
    "уссурийск", "бийск", "энгельс",
    "находка", "норильск", "армавир",
    "сызрань", "новочеркасск", "каменск-уральский",
    "златоуст", "альметьевск",
    "салават", "миасс", "копейск",
]


def geo_filter(vacancy):
    """Гео-фильтр: Москва + ближнее МО + зарубежье — pass.
    Дальнее Подмосковье и регионы РФ — reject.

    Returns:
        ("pass", None) — гео подходит
        ("reject", reason) — режем по гео
    """
    area = vacancy.get("area", {}) or {}
    area_name = (area.get("name") or "").strip()

    # Если area не указана — пропускаем (LLM разберётся по описанию).
    if not area_name:
        return "pass", None

    area_lower = area_name.lower()

    # Москва (включая "Москва, метро ...") — всегда ОК.
    # Зеленоград формально часть Москвы, попадёт сюда автоматически.
    if "москва" in area_lower:
        return "pass", None

    # Ближнее МО — whitelist, ОК.
    for city in NEAR_MOSCOW_REGION:
        if city in area_lower:
            return "pass", None

    # Регионы РФ и дальнее МО — reject.
    for region in RUSSIAN_REGIONS_BLACKLIST:
        if region in area_lower:
            return "reject", f"регион РФ: {area_name}"

    # Не Москва, не известный регион РФ — считаем зарубежьем, ОК.
    return "pass", None


# ============================================================
# Префильтр по тайтлу — дешёвая отсечка перед дорогой LLM.
# Паттерн ML system design: cheap filter → expensive model.
# Экономит ~40-60% LLM-вызовов на типичной выборке.
# ============================================================

# Whitelist — явно наша роль. Fast-track в LLM (LLM нужна для нюансов
# грейда/стека/корпорации vs стартапа, но префильтр здесь не режет).
TITLE_WHITELIST = [
    r"\bproduct manager\b",
    r"\bproject manager\b",
    r"\bproduct owner\b",
    r"\bml\s*pm\b",
    r"\bai\s*pm\b",
    r"\bml\s+project\b",
    r"\bai\s+project\b",
    r"\bprogram manager\b",
    r"\bdelivery manager\b",
    r"\bscrum master\b",
    r"\bруководитель\s+(проектов|продукта|продуктов)\b",
    r"\bменеджер\s+(проектов|продукта|продуктов)\b",
    r"\bменеджер\s+\w+\s+проектов\b",   # "менеджер количественных проектов"
    r"\bпродакт[\s-]?менеджер\b",        # "Продакт-менеджер" через дефис
    r"\bвладелец\s+продукта\b",
    r"\bml\s+менеджер\b",
    r"\bai\s+менеджер\b",
]

# Blacklist — hard NO по тайтлу. Режем БЕЗ LLM-вызова.
# Blacklist имеет приоритет над whitelist.
TITLE_BLACKLIST = [
    r"\b1с\b",
    r"\bback[\s-]?end\b",
    r"\bfront[\s-]?end\b",
    r"\bfull[\s-]?stack\b",
    r"\bdata\s+engineer\b",
    r"\bdevops\b",
    r"\bqa\s+engineer\b",
    r"\bтестировщик\b",
    r"\bразработчик\b",
    r"\bпрограммист\b",
    r"\bинженер\b",
    r"\bаналитик\b(?!\s+проектов)",   # "Аналитик", но НЕ "Аналитик проектов"
    r"\bдизайнер\b",
    r"\bарт[\s-]?директор\b",
    r"\bui[/\s]ux\b",
    r"\bрекрутер\b",
    r"\bhr\s*(bp|d|директор|менеджер)\b",
    r"\bкадровик\b",
    r"\bменеджер\s+по\s+продажам\b",
    r"\bменеджер\s+по\s+работе\s+с\s+клиентами\b",
    r"\bb2b\s+sales\b",
    r"\bsmm\b",
    r"\bконтент[\s-]?менеджер\b",
    r"\bкопирайтер\b",
    r"\bстажёр\b",
    r"\bстажер\b",
    r"\bintern\b",
    r"\btrainee\b",
    r"\bjunior\b",
    r"\bбухгалтер\b",
    r"\bюрист\b",
    r"\bоператор\b",
    r"\bмастер\s+смены\b",
]


def prefilter_by_title(title):
    """Дешёвая отсечка по заголовку до LLM.

    Returns:
        ("fast_track", None) — явно наша роль, отправляем в LLM
        ("reject", reason)   — явный hard NO, режем без LLM
        ("pass", None)       — серая зона, отправляем в LLM
    """
    title_lower = title.lower()

    # Blacklist первым — приоритет над whitelist.
    for pattern in TITLE_BLACKLIST:
        if re.search(pattern, title_lower):
            return "reject", f"blacklist: {pattern}"

    # Затем whitelist — fast-track для явно своих ролей.
    for pattern in TITLE_WHITELIST:
        if re.search(pattern, title_lower):
            return "fast_track", None

    # Серая зона — не уверены, отдаём LLM.
    return "pass", None


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
    requests.put(url, headers=headers, json=body)


def get_vacancies(search):
    """Получаем вакансии с pagination, без geo-фильтра в API.

    MAX_PAGES страниц по PER_PAGE=100 = до 200 вакансий на один query.
    Без area — ищем по всем регионам РФ + ближнее зарубежье + удалёнка.
    Гео-фильтрация делается в коде через geo_filter().
    """
    all_items = []
    for page in range(MAX_PAGES):
        url = "https://api.hh.ru/vacancies"
        params = {
            "text": search,
            "period": 7,
            "per_page": PER_PAGE,
            "page": page,
            "order_by": "publication_time",
        }
        headers = {"Authorization": f"Bearer {HH_TOKEN}"}

        page_items = []
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, headers=headers, timeout=30)
                page_items = r.json().get("items", [])
                break
            except Exception as e:
                print(f"⚠️ Ошибка hh.ru (search='{search}', page={page}): {e}, попытка {attempt+1}/3")
                time.sleep(5)

        all_items.extend(page_items)

        if len(page_items) < PER_PAGE:
            break

        time.sleep(0.5)

    return all_items


def get_vacancy_detail(vacancy_id):
    url = f"https://api.hh.ru/vacancies/{vacancy_id}"
    headers = {"Authorization": f"Bearer {HH_TOKEN}"}
    for i in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code != 200:
                print(f"⚠️ hh.ru ответил: {r.status_code} {r.text[:300]}")
            return r.json()
        except Exception as e:
            print(f"⚠️ Ошибка соединения: {e}, попытка {i+1}/3")
            time.sleep(5)
    return {}


def ask_gpt(system, user):
    """GPT-вызов в свободном текстовом режиме.
    Используется в write_cover_letter — там нужен живой текст, не JSON.
    """
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
    """LLM-вызов с гарантированным JSON output для классификатора.

    response_format={"type": "json_object"} — модель обязана вернуть
    валидный JSON. Без этого флага gpt-4o-mini иногда оборачивает
    ответ в ```json``` или добавляет преамбулу — типичный production-pitfall.

    Retry с exponential backoff (1s → 2s → 4s) на сетевых ошибках и 429.
    """
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
                print(f"⏳ Rate limit, ждём {wait}с (попытка {attempt+1}/{max_retries})")
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

            time.sleep(1)
            return result["choices"][0]["message"]["content"]

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Сетевая ошибка GPT: {e}, попытка {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)

    return '{"decision": "no", "match_score": 0, "tier": "api_error", "concerns": ["API failure"], "reason": "API недоступен после 3 попыток"}'


def is_relevant(vacancy):
    """Троичная классификация: yes / maybe / no + match_score 0-10.

    Гео-фильтрация и company-фильтрация уже сделаны в коде —
    здесь LLM работает только с ролью/доменом/стеком.

    Возвращает (is_match: bool, classification: dict).
    is_match=True если match_score >= RELEVANCE_THRESHOLD.
    """

    system = """Ты — ассистент, помогающий ML/AI Project Manager оценивать релевантность вакансий с hh.ru.

Твоя задача — для каждой вакансии вернуть СТРОГО валидный JSON с оценкой релевантности.

ВАЖНО: гео-фильтрация и фильтр по работодателю уже выполнены на предыдущих шагах. Здесь оценивай ТОЛЬКО роль, домен, грейд и стек. Географию и название компании в reason не упоминай.

=== ПРОФИЛЬ КАНДИДАТА ===
""" + CANDIDATE_PROFILE + """

=== КАК ОЦЕНИВАТЬ ===

Шаг 1. Определи tier (уровень соответствия домена):
  - "tier_1": вакансия про AI / ML / NLP / LLM / RAG / GenAI / Computer Vision
  - "tier_2": HR-tech, IT/SaaS, продуктовые компании, цифровая трансформация
  - "tier_3": финтех, банки, EdTech, e-commerce, прочий IT
  - "out_of_scope": не IT или роль из hard NO списка

Шаг 2. Проверь hard NO признаки. Если хотя бы один срабатывает — decision="no", score=0-2:
  - роль про 1С (1С-аналитик, 1С-архитектор, 1С-разработчик, 1С-консультант)
  - роль про чистые продажи / sales / аккаунт-менеджмент
  - роль про дизайн (арт-директор, дизайн-лид, UX/UI-лид)
  - роль про чистую разработку (Backend/Frontend/Data Engineer/DevOps без PM-функций)
  - HR-роли (HR BP, HRD, T&D, рекрутер, HR-директор)
  - маркетинг / SMM / контент-менеджер
  - стажировка / junior / trainee
  - C-level (CPO/CTO/VP/Director) в крупной корпорации (банк, телеком, ритейл-гигант)

Шаг 3. Проверь позитивные сигналы:
  - роль из списка: PM, Product Manager, Project Manager, Product Owner, ML PM, AI PM, Program Manager, Delivery Manager, Scrum Master, Руководитель проектов/продукта
  - упоминания AI/ML/NLP/LLM/RAG в требованиях или продукте
  - product/project ownership, работа с requirements, Agile/Scrum
  - грейд middle / middle+ / senior

Шаг 4. ВАЖНО про lead/head-of роли:
  - "Head of AI Projects", "COO в AI-стартапе", "Lead PM в небольшой команде" — это OK (decision="yes" или "maybe")
  - "Head of Product в Сбере", "Director of PMO в крупной корпорации" — это NO
  - Признаки крупной корпорации: банки топ-20, телеком (МТС/Билайн/Мегафон), ритейл-гиганты (X5/Магнит), госкомпании
  - Признаки стартапа: маленькая команда, упоминание "стартап", "молодая компания", series A/B/seed

Шаг 5. Выстави match_score 0-10:
  - 9-10: tier_1 (AI/ML/NLP) + middle/senior PM-роль + стартап или продуктовая компания
  - 7-8: tier_1 (AI/ML) + PM-роль, ИЛИ tier_2 + явная PM-роль с релевантным стеком
  - 5-6: tier_2/tier_3 + PM-роль, без явных red flags, но домен не идеальный
  - 3-4: пограничная зона — есть подозрительные признаки, но не явный hard NO
  - 0-2: hard NO сработал, либо явное несоответствие профилю

Шаг 6. Реши decision:
  - "yes" — score >= 7
  - "maybe" — score 5-6
  - "no" — score <= 4

=== FEW-SHOT ПРИМЕРЫ ===

Пример 1:
Вакансия: "ML Project Manager — внедрение LLM в банке"
Описание: "Управление проектами по внедрению LLM-решений, NLP, RAG-пайплайны, Agile..."
Ответ:
{"decision": "yes", "match_score": 10, "tier": "tier_1", "concerns": [], "reason": "Прямое попадание: ML PM с LLM/RAG в банке — точный матч с текущим опытом кандидата."}

Пример 2:
Вакансия: "COO ИИ / Руководитель проектов в ИИ образовательной сфере / COO AI EdTech"
Описание: "AI-стартап в EdTech, команда 15 человек, управление продуктовыми и исследовательскими проектами..."
Ответ:
{"decision": "yes", "match_score": 8, "tier": "tier_1", "concerns": ["Роль COO формально C-level, но в стартапе на 15 человек это операционный руководитель"], "reason": "AI EdTech стартап + руководство проектами — попадает в tier_1 и в зону 'lead в стартапе ОК'."}

Пример 3:
Вакансия: "Аккаунт менеджер / Менеджер проектов (IT)"
Описание: "Работа с ключевыми клиентами, ведение IT-проектов внедрения..."
Ответ:
{"decision": "maybe", "match_score": 5, "tier": "tier_3", "concerns": ["Гибридная роль аккаунт+PM, неясно соотношение продаж и проектного управления"], "reason": "Половина роли — продажи (hard NO), половина — IT PM. Нужен ручной просмотр описания."}

Пример 4:
Вакансия: "Ведущий аналитик 1С / Функциональный архитектор 1С"
Описание: "Разработка и внедрение конфигураций 1С..."
Ответ:
{"decision": "no", "match_score": 1, "tier": "out_of_scope", "concerns": ["Стек 1С — hard NO"], "reason": "1С-направление, не совпадает с LLM/NLP опытом кандидата."}

Пример 5:
Вакансия: "Product Manager в e-commerce маркетплейс"
Описание: "Управление продуктом онлайн-маркетплейса, A/B тесты, работа с метриками, средняя команда..."
Ответ:
{"decision": "yes", "match_score": 7, "tier": "tier_3", "concerns": [], "reason": "PM-роль в продуктовой компании, есть продуктовые метрики и A/B — релевантно опыту, хотя домен не tier_1."}

Пример 6:
Вакансия: "Арт-директор (руководитель отдела дизайна)"
Описание: "Руководство командой дизайнеров, развитие визуального стиля бренда..."
Ответ:
{"decision": "no", "match_score": 0, "tier": "out_of_scope", "concerns": ["Дизайн-направление — hard NO"], "reason": "Дизайн-руководство, не соответствует PM/Product/Project профилю."}

=== ФОРМАТ ОТВЕТА ===

Верни СТРОГО JSON без markdown-обёртки, без пояснений до или после.
Структура:
{
  "decision": "yes" | "maybe" | "no",
  "match_score": <число от 0 до 10>,
  "tier": "tier_1" | "tier_2" | "tier_3" | "out_of_scope",
  "concerns": [<массив строк с потенциальными проблемами, может быть пустым>],
  "reason": "<одно короткое предложение — почему такое решение>"
}
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
    system = """Ты помогаешь писать сопроводительные письма к вакансиям.

СТРОГИЕ ПРАВИЛА:
1. Начинай ВСЕГДА с фразы: "Добрый день! Заинтересовала вакансия [название вакансии]"
2. Тон — деловой, сдержанный, живой. Не канцелярит.
3. Письмо должно быть КОНКРЕТНЫМ под эту вакансию — упоминай детали из описания вакансии
4. Опыт кандидата — 3-5 лет в роли PM/Project Manager. НИКОГДА не пиши "10 лет", и не преувеличивай опыт 5 лет 
5. НЕЛЬЗЯ использовать шаблонные фразы: "буду рада применить компетенции", "хочу предложить кандидатуру", "очень хочу"
6. НЕЛЬЗЯ использовать метафоры и пафос
7. НЕЛЬЗЯ упоминать конкретные названия проектов и работодателей — описывай опыт обобщённо
8. НЕЛЬЗЯ использовать слово "очень"
9. НЕЛЬЗЯ добавлять контакты — телефон, email
10. Каждое письмо должно отличаться от других — подбирай релевантный опыт под конкретную вакансию
11. Письмо — максимум 2 абзаца, чётко и по делу
12. Предложения - максимально простые. НЕЛЬЗЯ писать "обладаю навыками", вместо этого пиши просто "умею"
13. НЕЛЬЗЯ использовать оценочные прилагательные и наречия в применении к опыту работы, такие как "большой", "успешный", "успешно", "результативный", "глубокий", "уверенно" и прочие
14. Заканчивай ВСЕГДА так: "С уважением, Марина. тг @Marina_Usckova"
15. Кандидат — девушка. Используй женский род: "управляла", "работала", "занималась", а не "управлял", "работал", "занимался"
16. Тщательно проверяй орфографию и грамматику русского языка
17. Пиши от первого лица, на русском языке"""

    user = f"""Профиль кандидата:
{PROFILE}

Вакансия: {vacancy['name']}
Компания: {vacancy.get('employer', {}).get('name', '')}
Описание: {vacancy.get('description', '')[:2000]}

Напиши сопроводительное письмо."""
    return ask_gpt(system, user)


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
    return r.status_code


def get_resume_id():
    url = "https://api.hh.ru/resumes/mine"
    headers = {"Authorization": f"Bearer {HH_TOKEN}"}
    r = requests.get(url, headers=headers)
    resumes = r.json().get("items", [])
    if resumes:
        return resumes[0]["id"]
    return None


def main():
    resume_id = get_resume_id()
    if not resume_id:
        print("Резюме не найдено!")
        return

    applied_ids, sha = get_applied_ids()
    applied = []
    skipped_by_llm = []         # отклонённые LLM (с classification dict)
    skipped_by_prefilter = []   # отклонённые префильтром
    skipped_by_geo = []         # отклонённые гео-фильтром
    skipped_by_company = []     # отклонённые company-фильтром
    seen_ids = set()

    # Funnel counters — observability воронки.
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
    }

    for search in SEARCHES:
        print(f"\n🔍 Ищем: {search}")
        vacancies = get_vacancies(search)
        funnel["fetched_total"] += len(vacancies)

        for v in vacancies:
            if v['id'] in seen_ids:
                funnel["duplicates_in_search"] += 1
                continue
            if v['id'] in applied_ids:
                funnel["already_applied"] += 1
                continue
            seen_ids.add(v['id'])

            # === Stage 0: company blacklist (самый дешёвый фильтр) ===
            company_result, company_reason = company_filter(v)
            if company_result == "reject":
                funnel["company_rejected"] += 1
                skipped_by_company.append({
                    "name": v['name'],
                    "employer": v.get('employer', {}).get('name', ''),
                    "reason": company_reason
                })
                continue

            # === Stage 1: гео-фильтр ===
            geo_result, geo_reason = geo_filter(v)
            if geo_result == "reject":
                funnel["geo_rejected"] += 1
                skipped_by_geo.append({
                    "name": v['name'],
                    "area": v.get('area', {}).get('name', ''),
                    "reason": geo_reason
                })
                continue

            # === Stage 2: prefilter по тайтлу ===
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

            # === Stage 3: получаем детали и кидаем в LLM ===
            time.sleep(1)
            detail = get_vacancy_detail(v['id'])
            if not detail or 'name' not in detail:
                print(f"⚠️ Не удалось получить детали вакансии, пропускаем")
                funnel["detail_fetch_failed"] += 1
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
                    print(f"📨 Письмо не обязательно — откликаемся без письма")
                    letter = ""

                status = apply(v['id'], resume_id, letter)
                if status in [200, 201]:
                    print(f"📨 Отклик отправлен!")
                    applied_ids.append(v['id'])
                    applied.append(f"{v['name']} — {v.get('employer', {}).get('name', '')}")
                    funnel["applied_success"] += 1
                else:
                    print(f"⚠️ Ошибка отклика: {status}")
                    funnel["applied_failed"] += 1
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
        time.sleep(2)
    save_applied_ids(applied_ids, sha)

    # ============================================================
    # Funnel breakdown — observability на уровне всей воронки.
    # ============================================================
    print("\n" + "=" * 60)
    print("📊 ВОРОНКА:")
    print("=" * 60)
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
    print(f"    └ fast-track (whitelist):  {funnel['prefilter_fast_track']}")
    print(f"    └ pass (серая зона):       {funnel['prefilter_pass']}")
    print(f"  Не получили детали:        -{funnel['detail_fetch_failed']}")
    print(f"  ────────────────────────────")
    print(f"  LLM одобрила:              {funnel['llm_approved']}")
    print(f"  LLM отклонила:             {funnel['llm_rejected']}")
    print(f"  ────────────────────────────")
    print(f"  Откликов отправлено:       {funnel['applied_success']}")
    print(f"  Ошибок отклика:            {funnel['applied_failed']}")

    # ============================================================
    # Детали отклонённых LLM — Maybe-зона важна для threshold tuning.
    # ============================================================
    print("\n" + "=" * 60)
    print("📊 ИТОГ ПО ОТКЛИКАМ:")
    print("=" * 60)
    print(f"Откликнулся: {len(applied)}")
    for a in applied:
        print(f"  ✅ {a}")

    maybe_zone = [s for s in skipped_by_llm if s['score'] in (5, 6)]
    mid_zone = [s for s in skipped_by_llm if 3 <= s['score'] <= 4]
    hard_no = [s for s in skipped_by_llm if s['score'] <= 2]

    print(f"\n🟡 Maybe-зона (score 5-6): {len(maybe_zone)}")
    for s in maybe_zone[:15]:
        print(f"  [{s['score']}] {s['name']} — {s['reason']}")

    print(f"\n🟠 Mid-зона (score 3-4): {len(mid_zone)}")
    for s in mid_zone[:5]:
        print(f"  [{s['score']}] {s['name']} — {s['reason']}")

    print(f"\n🔴 Hard NO (LLM, score 0-2): {len(hard_no)}")

    # Company / гео / префильтр — выводим агрегатами по причинам.
    print(f"\n🔴 Company-фильтр отклонил: {len(skipped_by_company)}")
    if skipped_by_company:
        company_counts = Counter(s['employer'] for s in skipped_by_company)
        for employer, count in company_counts.most_common(10):
            print(f"  [{count}x] {employer}")

    print(f"\n🔴 Гео-фильтр отклонил: {len(skipped_by_geo)}")
    if skipped_by_geo:
        geo_counts = Counter(s['area'] for s in skipped_by_geo)
        for area, count in geo_counts.most_common(10):
            print(f"  [{count}x] {area}")

    print(f"\n🔴 Префильтр отклонил: {len(skipped_by_prefilter)}")
    if skipped_by_prefilter:
        reason_counts = Counter(s['reason'] for s in skipped_by_prefilter)
        for reason, count in reason_counts.most_common(10):
            print(f"  [{count}x] {reason}")


if __name__ == "__main__":
    main()
