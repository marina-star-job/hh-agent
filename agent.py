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
# SEARCHES сокращены с 17 до 9.
# Раньше пересекающиеся query генерили 27% дубликатов
# (782 duplicates на 2902 fetched). Убраны:
#   - "AI Project" / "ML Project" / "AI менеджер" / "ML менеджер" —
#     дублировали "руководитель AI/ML"
#   - "Product Manager IT" / "банк" / "Project Manager финтех" —
#     subset'ы "Product Manager" / "Project Manager"
#   - "Менеджер продукта" — дубль "Product Manager" на русском
# ============================================================
SEARCHES = [
    "Product Manager",
    "Project Manager",
    "Руководитель проектов",
    "Руководитель продукта",
    "Product Owner",
    "Владелец продукта",
    "руководитель AI",
    "руководитель ML",
    "AI Product",
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
# Гео-фильтр: Москва + ближнее МО + зарубежье. РФ-регионы — reject.
# ============================================================
NEAR_MOSCOW_REGION = [
    "химки", "реутов", "мытищи", "балашиха",
    "королёв", "королев",
    "люберцы", "красногорск", "одинцово",
    "долгопрудный", "котельники", "видное",
    "дзержинский", "лыткарино", "юбилейный",
    "железнодорожный",
]

RUSSIAN_REGIONS_BLACKLIST = [
    "московская область", "подмосковье",
    "подольск", "серпухов", "клин",
    "сергиев посад", "раменское", "жуковский",
    "чехов", "дмитров", "истра", "можайск",
    "ногинск", "электросталь", "орехово-зуево",
    "пушкино", "щёлково", "щелково",
    "звенигород", "коломна", "наро-фоминск",
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
    area = vacancy.get("area", {}) or {}
    area_name = (area.get("name") or "").strip()
    if not area_name:
        return "pass", None
    area_lower = area_name.lower()
    if "москва" in area_lower:
        return "pass", None
    for city in NEAR_MOSCOW_REGION:
        if city in area_lower:
            return "pass", None
    for region in RUSSIAN_REGIONS_BLACKLIST:
        if region in area_lower:
            return "reject", f"регион РФ: {area_name}"
    return "pass", None


# ============================================================
# Префильтр по тайтлу.
# ============================================================
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
    r"\bменеджер\s+\w+\s+проектов\b",
    r"\bпродакт[\s-]?менеджер\b",
    r"\bвладелец\s+продукта\b",
    r"\bml\s+менеджер\b",
    r"\bai\s+менеджер\b",
]

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
    r"\bаналитик\b(?!\s+проектов)",
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
def get_vacancies(search):
    """Получаем вакансии с pagination."""
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
    system = """Ты — ассистент, помогающий ML/AI Project Manager оценивать релевантность вакансий с hh.ru.

Твоя задача — для каждой вакансии вернуть СТРОГО валидный JSON с оценкой релевантности.

ВАЖНО: гео-фильтрация и фильтр по работодателю уже выполнены на предыдущих шагах. Здесь оценивай ТОЛЬКО роль, домен, грейд и стек. Географию и название компании в reason не упоминай.

=== ПРОФИЛЬ КАНДИДАТА ===
""" + CANDIDATE_PROFILE + """

=== КАК ОЦЕНИВАТЬ ===

Шаг 1. Определи tier:
  - "tier_1": AI / ML / NLP / LLM / RAG / GenAI / Computer Vision
  - "tier_2": HR-tech, IT/SaaS, продуктовые компании, цифровая трансформация
  - "tier_3": финтех, банки, EdTech, e-commerce, прочий IT
  - "out_of_scope": не IT или роль из hard NO

Шаг 2. Hard NO признаки → decision="no", score=0-2:
  - 1С (любые роли)
  - чистые продажи / sales / аккаунт-менеджмент
  - дизайн / арт-директор / UX-lead
  - чистая разработка (Backend/Frontend/Data Engineer/DevOps)
  - HR-роли (HR BP, HRD, T&D, рекрутер)
  - маркетинг / SMM / контент-менеджер
  - стажировка / junior / trainee
  - C-level в крупной корпорации

Шаг 3. Позитивные сигналы:
  - PM, Product Manager, Project Manager, Product Owner, ML PM, AI PM, Program Manager, Delivery Manager, Scrum Master, Руководитель проектов/продукта
  - AI/ML/NLP/LLM/RAG в требованиях
  - product/project ownership, requirements, Agile/Scrum
  - грейд middle / middle+ / senior

Шаг 4. Lead/head-of роли:
  - "Head of AI Projects", "COO в AI-стартапе", "Lead PM в небольшой команде" — OK
  - "Head of Product в Сбере", "Director of PMO" — NO
  - крупная корп: банки топ-20, телеком, ритейл-гиганты, госкомпании

Шаг 5. match_score 0-10:
  - 9-10: tier_1 + middle/senior PM + стартап/продуктовая
  - 7-8: tier_1 + PM, ИЛИ tier_2 + явная PM
  - 5-6: tier_2/tier_3 + PM, без red flags
  - 3-4: пограничная зона
  - 0-2: hard NO либо явное несоответствие

Шаг 6. decision: "yes" — score >= 7, "maybe" — 5-6, "no" — <= 4

=== FEW-SHOT ===

Пример 1:
Вакансия: "ML Project Manager — внедрение LLM в банке"
Ответ: {"decision": "yes", "match_score": 10, "tier": "tier_1", "concerns": [], "reason": "Прямое попадание: ML PM с LLM/RAG в банке."}

Пример 2:
Вакансия: "COO ИИ / Руководитель проектов в ИИ образовательной сфере"
Ответ: {"decision": "yes", "match_score": 8, "tier": "tier_1", "concerns": ["COO формально C-level, но в стартапе на 15 чел. это операционный руководитель"], "reason": "AI EdTech стартап + руководство проектами."}

Пример 3:
Вакансия: "Аккаунт менеджер / Менеджер проектов (IT)"
Ответ: {"decision": "maybe", "match_score": 5, "tier": "tier_3", "concerns": ["Гибрид аккаунт+PM"], "reason": "Половина продажи (hard NO), половина IT PM."}

Пример 4:
Вакансия: "Ведущий аналитик 1С"
Ответ: {"decision": "no", "match_score": 1, "tier": "out_of_scope", "concerns": ["Стек 1С — hard NO"], "reason": "1С-направление."}

Пример 5:
Вакансия: "Product Manager в e-commerce маркетплейс"
Ответ: {"decision": "yes", "match_score": 7, "tier": "tier_3", "concerns": [], "reason": "PM в продуктовой компании с метриками и A/B."}

Пример 6:
Вакансия: "Арт-директор"
Ответ: {"decision": "no", "match_score": 0, "tier": "out_of_scope", "concerns": ["Дизайн — hard NO"], "reason": "Дизайн-руководство."}

=== ФОРМАТ ОТВЕТА ===

Верни СТРОГО JSON без markdown:
{"decision": "yes"|"maybe"|"no", "match_score": <0-10>, "tier": "tier_1"|"tier_2"|"tier_3"|"out_of_scope", "concerns": [...], "reason": "..."}
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

    return r.status_code, is_already_applied


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

                    status, is_already = apply(v['id'], resume_id, letter)

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

    return 1 if captcha_hit else 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
