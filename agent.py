import os
import requests
import time
import json
import base64
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
# это экономит токены (134 вакансии × лишние 5KB = много денег и времени).
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
# Вынесено в константу для быстрого threshold tuning без правки логики.
RELEVANCE_THRESHOLD = 6

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
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": search,
        "area": 1,
        "period": 7,
        "per_page": 10,
        "order_by": "publication_time",
    }
    headers = {"Authorization": f"Bearer {HH_TOKEN}"}
    for i in range(3):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            return r.json().get("items", [])
        except Exception as e:
            print(f"⚠️ Ошибка запроса к hh.ru: {e}, попытка {i+1}/3")
            time.sleep(5)
    return []


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
    """Старый GPT-вызов в свободном текстовом режиме.
    Используется только в write_cover_letter — там нужен живой текст,
    JSON не нужен."""
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
    """Вызов LLM с гарантированным JSON output для классификатора.

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
        "temperature": 0.1,  # детерминированность важнее креативности для классификатора
        "response_format": {"type": "json_object"}
    }

    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=30)

            # 429 = rate limit. Backoff даёт API остыть.
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

            time.sleep(1)  # лёгкая пауза, чтобы не упереться в rate limit
            return result["choices"][0]["message"]["content"]

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Сетевая ошибка GPT: {e}, попытка {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)

    # Все retry исчерпаны — возвращаем валидный JSON со score=0.
    # Вакансия отсеется мягко, пайплайн не упадёт.
    return '{"decision": "no", "match_score": 0, "tier": "api_error", "concerns": ["API failure"], "reason": "API недоступен после 3 попыток"}'


def is_relevant(vacancy):
    """Троичная классификация: yes / maybe / no + match_score 0-10.

    Возвращает кортеж (is_match: bool, classification: dict),
    чтобы верхний уровень мог логировать reason/concerns.

    is_match=True если match_score >= RELEVANCE_THRESHOLD.
    """

    system = """Ты — ассистент, помогающий ML/AI Project Manager оценивать релевантность вакансий с hh.ru.

Твоя задача — для каждой вакансии вернуть СТРОГО валидный JSON с оценкой релевантности.

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
Описание: {vacancy.get('description', '')[:2500]}

Оцени релевантность."""

    raw_answer = ask_gpt_json(system, user)

    # Defensive parsing — обработка случая, когда модель всё-таки
    # вернула невалидный JSON (например, при API-ошибке или редком сбое
    # response_format). Пайплайн не должен падать — мягко отклоняем.
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
    skipped = []  # теперь хранит dict с метаданными, а не просто строки
    seen_ids = set()

    for search in SEARCHES:
        print(f"\n🔍 Ищем: {search}")
        vacancies = get_vacancies(search)

        for v in vacancies:
            if v['id'] in seen_ids:
                continue
            if v['id'] in applied_ids:
                print(f"⏭️ Уже откликались: {v['name']}")
                continue
            seen_ids.add(v['id'])

            time.sleep(1)
            detail = get_vacancy_detail(v['id'])
            if not detail or 'name' not in detail:
                print(f"⚠️ Не удалось получить детали вакансии, пропускаем")
                continue

            is_match, classification = is_relevant(detail)

            # Структурированный префикс для лога — теперь видим
            # score, tier и decision в каждой строке.
            log_prefix = (
                f"[score={classification['match_score']}, "
                f"tier={classification['tier']}, "
                f"decision={classification['decision']}]"
            )

            if is_match:
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
                else:
                    print(f"⚠️ Ошибка отклика: {status}")
            else:
                print(f"❌ Не подходит {log_prefix}: {v['name']}")
                print(f"   reason: {classification['reason']}")
                skipped.append({
                    "name": v['name'],
                    "score": classification['match_score'],
                    "tier": classification['tier'],
                    "reason": classification['reason']
                })

    save_applied_ids(applied_ids, sha)

    # ============================================================
    # Финальный отчёт со структурированной разбивкой по зонам.
    # Maybe-зона (5-6) — главный сигнал для threshold tuning:
    # если там много явно подходящих вакансий — стоит снизить порог.
    # ============================================================
    print("\n📊 ИТОГ:")
    print(f"Откликнулся: {len(applied)}")
    for a in applied:
        print(f"  ✅ {a}")

    print(f"\nПропустил: {len(skipped)}")

    maybe_zone = [s for s in skipped if s['score'] in (5, 6)]
    mid_zone = [s for s in skipped if 3 <= s['score'] <= 4]
    hard_no = [s for s in skipped if s['score'] <= 2]

    print(f"\n  🟡 Maybe-зона (score 5-6): {len(maybe_zone)}")
    for s in maybe_zone[:10]:
        print(f"    [{s['score']}] {s['name']} — {s['reason']}")

    print(f"\n  🟠 Mid-зона (score 3-4): {len(mid_zone)}")
    for s in mid_zone[:5]:
        print(f"    [{s['score']}] {s['name']} — {s['reason']}")

    print(f"\n  🔴 Hard NO (score 0-2): {len(hard_no)}")


if __name__ == "__main__":
    main()
