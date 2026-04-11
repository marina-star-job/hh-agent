import os
import requests
import json

HH_TOKEN = os.environ['HH_ACCESS_TOKEN']
GPT_TOKEN = os.environ['GPT_MODELS_TOKEN']

PROFILE = open('profile.md', 'r', encoding='utf-8').read()

SEARCHES = [
    "Руководитель проектов IT",
    "Project Manager IT",
    "Product Manager IT",
    "PM ML",
    "PM NLP",
]

def get_vacancies(search):
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": search,
        "area": 1,
        "period": 1,
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
    return r.json()["choices"][0]["message"]["content"]

def is_relevant(vacancy):
    system = """Ты помогаешь искать работу. Оцени подходит ли вакансия кандидату.
Ответь ТОЛЬКО одним словом: ДА или НЕТ."""
    user = f"""Профиль кандидата:
{PROFILE}

Вакансия:
{vacancy['name']}
{vacancy.get('description', '')[:2000]}

Подходит ли эта вакансия кандидату?"""
    answer = ask_gpt(system, user)
    return "ДА" in answer.upper()

def write_cover_letter(vacancy):
    system = """Ты помогаешь писать сопроводительные письма к вакансиям.
Пиши живо, по-человечески, без шаблонных фраз типа "рад сообщить" или "хочу предложить свою кандидатуру".
Письмо должно быть коротким — 3-4 абзаца максимум.
Пиши от первого лица, на русском языке."""
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

    applied = []
    skipped = []
    seen_ids = set()

    for search in SEARCHES:
        print(f"\n🔍 Ищем: {search}")
        vacancies = get_vacancies(search)

        for v in vacancies:
            if v['id'] in seen_ids:
                continue
            seen_ids.add(v['id'])

            detail = get_vacancy_detail(v['id'])

            if is_relevant(detail):
                print(f"✅ Подходит: {v['name']} — {v.get('employer', {}).get('name', '')}")
                letter = write_cover_letter(detail)
                status = apply(v['id'], resume_id, letter)
                if status in [200, 201]:
                    print(f"📨 Отклик отправлен!")
                    applied.append(f"{v['name']} — {v.get('employer', {}).get('name', '')}")
                else:
                    print(f"⚠️ Ошибка отклика: {status}")
            else:
                print(f"❌ Не подходит: {v['name']}")
                skipped.append(v['name'])

    print("\n📊 ИТОГ:")
    print(f"Откликнулся: {len(applied)}")
    for a in applied:
        print(f"  ✅ {a}")
    print(f"Пропустил: {len(skipped)}")

if __name__ == "__main__":
    main()
