#!/usr/bin/env python3
"""
Скрипт для парсинга форума VGD.RU - тема #166633
Собирает все записи со всех 228 страниц и сохраняет в CSV.

Установка зависимостей:
    pip install requests beautifulsoup4

Запуск:
    python vgd_scraper.py
"""

import csv
import time
import re
import sys
import os
from datetime import datetime

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Установите зависимости: pip install requests beautifulsoup4")
    sys.exit(1)

BASE_URL = "https://forum.vgd.ru/9103/166633/{offset}.htm?a=stdforum_view&o="
TOTAL_PAGES = int(os.getenv("TOTAL_PAGES", "228"))
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "vgd_list.csv")
ENCODING = "windows-1251"
DELAY = 0.5  # секунд между запросами, чтобы не перегружать сервер

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

def fetch_page(offset: int) -> str | None:
    url = BASE_URL.format(offset=offset)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.encoding = ENCODING
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"  Ошибка при загрузке offset={offset}: {e}")
        return None


def parse_records(html: str) -> list[dict]:
    """Извлекает строки данных из HTML-страницы."""
    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Ищем все блоки сообщений (каждый пост содержит таблицу данных)
    # Данные идут после строки-заголовка "USER_LAST_NAME USER_NAME USER_TITLE DATE_B NAME_SITE"
    all_text = soup.get_text("\n")

    # Находим блоки после маркера заголовка
    marker = "USER_LAST_NAME USER_NAME USER_TITLE DATE_B NAME_SITE"
    parts = all_text.split(marker)

    for part in parts[1:]:  # пропускаем всё до первого блока
        lines = part.strip().splitlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Останавливаемся на следующих служебных блоках
            if "alexalex1804" in line or "##" in line or "VGD.ru" in line:
                break
            records.append(parse_line(line))

    return [r for r in records if r]


# Паттерн для даты вида дд.мм.гг или дд.мм.гггг
DATE_PATTERN = re.compile(r"\d{2}\.\d{2}\.\d{2,4}")


def parse_line(line: str) -> dict | None:
    """Разбирает одну строку записи в словарь."""
    # Ищем дату в строке
    m = DATE_PATTERN.search(line)
    if not m:
        # Строка без даты — пропускаем (или сохраняем как есть)
        if len(line) < 3:
            return None
        return {"raw": line, "last_name": "", "first_name": "", "patronymic": "",
                "date_birth": "", "place": ""}

    date_str = m.group()
    before_date = line[:m.start()].strip()
    after_date = line[m.end():].strip()

    # Разбиваем часть до даты на ФИО + суффикс (дд / мм / жж)
    # Формат: [ФАМИЛИЯ] [ИМЯ] [ОТЧЕСТВО] [пол] date place
    # Суффикс пола может быть "дд", "мм", "жж" перед датой
    gender = ""
    gender_match = re.search(r"\s+(дд|мм|жж)\s*$", before_date)
    if gender_match:
        gender = gender_match.group(1)
        before_date = before_date[:gender_match.start()].strip()

    parts = before_date.split()

    last_name = parts[0] if len(parts) > 0 else ""
    first_name = parts[1] if len(parts) > 1 else ""
    patronymic = parts[2] if len(parts) > 2 else ""
    extra = " ".join(parts[3:]) if len(parts) > 3 else ""

    return {
        "last_name": last_name,
        "first_name": first_name,
        "patronymic": patronymic,
        "extra": extra,
        "gender": gender,
        "date_birth": date_str,
        "place": after_date,
        "raw": line,
    }


def main():
    all_records = []
    start_time = datetime.now()

    print(f"Начало парсинга: {start_time.strftime('%H:%M:%S')}")
    print(f"Всего страниц: {TOTAL_PAGES}")
    print("-" * 50)

    for page_num in range(1, TOTAL_PAGES + 1):
        offset = (page_num - 1) * 10
        print(f"Страница {page_num}/{TOTAL_PAGES} (offset={offset})...", end=" ", flush=True)

        html = fetch_page(offset)
        if html is None:
            print("ПРОПУЩЕНА")
            # Небольшая задержка перед повтором при ошибке
            time.sleep(2)
            continue

        records = parse_records(html)
        all_records.extend(records)
        print(f"найдено {len(records)} записей (всего: {len(all_records)})")

        time.sleep(DELAY)

    # Сохраняем в CSV
    print("-" * 50)
    print(f"Сохранение в {OUTPUT_FILE}...")

    fieldnames = ["last_name", "first_name", "patronymic", "extra",
                  "gender", "date_birth", "place", "raw"]

    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_records)

    elapsed = datetime.now() - start_time
    print(f"Готово! Сохранено {len(all_records)} записей.")
    print(f"Время выполнения: {elapsed}")
    print(f"Файл: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
