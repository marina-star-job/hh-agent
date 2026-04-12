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

PROFILE = open('profile.md', 'r', encoding='utf-8').read()

SEARCHES = [
    "Product Manager",
    "Project Manager",
    "Руководитель проектов",
    "Product Owner",
    "PM AI",
    "PM ML",
    "PM финтех",
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
    r = requests.get(url, params=params, headers=headers)
    return r.json().get("items", [])

def get_vacancy_detail(vacancy_id):
    url = f"https://api.hh.ru/vacancies/{vacancy_id}"
    headers = {"Authorization": f"Bearer {HH_TOKEN}"}
    r = requests.get(url, headers=headers)
    return r.json()

def ask_gpt(system, user):
    url = "https://models.inference.ai.azure.com/chat/completions"
    headers = {
        "Authorization": f"Bearer {GPT_TOKEN}",
        "Content-Type": "application/json"
    }
    body = {
        "model": "gpt-4o",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        "max_tokens": 1000
    }
    r = requests.post(url, headers=headers, json=body)
    time.sleep(7)
    result = r.json()
    if "choices" not in result:
        print(f"⚠️ GPT ответил неожиданно: {result}")
        return "НЕТ"
    return result["choices"][0]["message"]["content"]

def is_relevant(vacancy):
    system = """Ты помогаешь искать работу для кандидата уровня Middle PM / Project Manager с опытом 3-5 лет.

Оцени подходит ли вакансия по следующим критериям:

ПОДХОДИТ если:
- Уровень: middle, senior, lead (но не C-level: CPO, CTO, VP, Director)
- Роль: Product Manager, Project Manager, Руководитель проектов, Product Owner, Scrum Master
- Направление: IT, AI/ML, финтех, банки, e-commerce, SaaS и другие продуктовые компании
- Требования соответствуют опыту 3-5 лет
- Город Москва или другая страна

НЕ ПОДХОДИТ если:
- C-level позиции: CPO, CTO, VP of Product, Director, Head of (крупного департамента)
- Требуют 7+ лет опыта
- Это не IT/продуктовая роль: продажи, маркетинг, дизайн, разработка
- Стажировка или junior уровень

Ответь ТОЛЬКО одним словом: ДА или НЕТ."""

    user = f"""Профиль кандидата:
{PROFILE}

Вакансия: {vacancy['name']}
Компания: {vacancy.get('employer', {}).get('name', '')}
Требования: {vacancy.get('description', '')[:3000]}

Подходит ли эта вакансия кандидату?"""
    answer = ask_gpt(system, user)
    return "ДА" in answer.upper()

def write_cover_letter(vacancy):
    system = """Ты помогаешь писать сопроводительные письма к вакансиям.

СТРОГИЕ ПРАВИЛА:
1. Начинай ВСЕГДА с фразы: "Добрый день! Заинтересовала вакансия [название вакансии]"
2. Тон — деловой, сдержанный, без пафоса
3. НЕЛЬЗЯ использовать метафоры ("профессиональный дом", "правила игры", "импульс" и подобное)
4. НЕЛЬЗЯ писать разговорные фразы ("работать руками и головой", "ухитрялась", "очень хочу" и подобное)
5. НЕЛЬЗЯ упоминать конкретные названия проектов и работодателей — описывай опыт обобщённо
6. НЕЛЬЗЯ использовать слово "очень"
7. НЕЛЬЗЯ добавлять скобки с личными комментариями
8. НЕЛЬЗЯ добавлять контакты — телефон, email
9. Письмо заканчивай ВСЕГДА так: "С уважением, Марина. тг @Marina_Usckova"
10. Письмо — максимум 3 абзаца, чётко и по делу
11. Пиши от первого лица, на русском языке"""

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
    skipped = []
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

            detail = get_vacancy_detail(v['id'])

            if is_relevant(detail):
                print(f"✅ Подходит: {v['name']} — {v.get('employer', {}).get('name', '')}")
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
                print(f"❌ Не подходит: {v['name']}")
                skipped.append(v['name'])

    save_applied_ids(applied_ids, sha)

    print("\n📊 ИТОГ:")
    print(f"Откликнулся: {len(applied)}")
    for a in applied:
        print(f"  ✅ {a}")
    print(f"Пропустил: {len(skipped)}")

if __name__ == "__main__":
    main()
