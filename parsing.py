import re
import csv
import time
import signal
import traceback
import pickle as pkl

from math import floor
from pathlib import Path
from types import FrameType
from typing import Any, Optional
from datetime import datetime, timedelta

import requests

from dotenv import dotenv_values


CUR_DIR = Path(__file__).resolve().parent

ENV_FILE = CUR_DIR / ".env"

DATA_DIR = CUR_DIR / "data/"
POSTS_FILE_PATH = DATA_DIR / "posts.csv"
USERS_FILE_PATH = DATA_DIR / "users.csv"

POSTS_ID_FILE_PATH = DATA_DIR / "posts_id.pkl"
USERS_ID_FILE_PATH = DATA_DIR / "users_id.pkl"
OFFSET_FILE_PATH = DATA_DIR / "offset_list.pkl"


USERS_FIELDS = [
    'id',
    'domain',
    'first_name',
    'last_name',
    'status',
    'about',
]

POST_FIELDS = [
    "id",
    "text",
    "from_id",
    "owner_id",
    "timestamp",
    "comment_count",
    "reposted_text",
]

SHUTDOWN = False
CONFIG = dotenv_values(ENV_FILE)


def load_from_pkl(path: str | Path) -> Any:
    with open(path, "rb") as pkl_file:
        return pkl.load(pkl_file)


def write_to_pkl(path: str | Path, data: Any) -> None:
    with open(path, "wb") as pkl_file:
        return pkl.dump(data, pkl_file)


def get_elapsed_time(td: timedelta) -> str:
    days = td.seconds // 86400
    hours = td.seconds // 3600 - days * 24
    minutes = td.seconds // 60 - 60 * hours
    seconds = td.seconds - 60 * minutes - 3600 * hours - 86400 * days

    return " ".join(
        t
        for t in [
            (f"{td.days}д." if td.days > 0 else ""),
            (f"{hours}ч." if hours > 0 else ""),
            (f"{minutes}мин." if minutes > 0 else ""),
            (f"{seconds}сек." if td.seconds > 0 else ""),
        ]
        if t != ""
    )


def get_users_info(user_ids: list[str], captcha=None):
    req_url = f"https://api.vk.com/method/users.get?user_ids={user_ids}&fields={','.join(USERS_FIELDS)}&access_token={CONFIG['VK_ACCESS_TOKEN']}&v=5.131"

    if captcha:
        req_url += f"&captcha_sid={captcha['sid']}&captcha_key={captcha['key']}"

    return requests.get(req_url).json()


def get_wall_posts(
    user_id: str, offset: int = 0, count: int = 100, captcha=None
) -> dict:
    req_url = f"https://api.vk.com/method/wall.get?domain={user_id}&access_token={CONFIG['VK_ACCESS_TOKEN']}&v=5.131&offset={offset}&count={count}"

    if captcha:
        req_url += f"&captcha_sid={captcha['sid']}&captcha_key={captcha['key']}"

    return requests.get(req_url).json()


def sigint_handler(signum: int, frame: Optional[FrameType]) -> None:
    global SHUTDOWN
    SHUTDOWN = True


def main():
    global SHUTDOWN

    # Пользовательские короткие именные ссылки
    users = [
    ]

    # Проверяем какие пользователи уже есть
    users_id = []
    if USERS_ID_FILE_PATH.exists():
        users_id = load_from_pkl(USERS_ID_FILE_PATH)

    if not USERS_FILE_PATH.exists():
        with open(USERS_FILE_PATH, 'w') as users_file:
            writer = csv.DictWriter(users_file, fieldnames=USERS_FIELDS)
            writer.writeheader()

    # С какого отступа дальше парсить посты пользователей
    offset_list = [0] * len(users)
    if OFFSET_FILE_PATH.exists():
        saved_offset_list = load_from_pkl(OFFSET_FILE_PATH)

        offset_list[:len(saved_offset_list)] = saved_offset_list

    # Какие посты уже есть
    posts_id = []
    if POSTS_ID_FILE_PATH.exists():
        posts_id = load_from_pkl(POSTS_ID_FILE_PATH)

    if not POSTS_FILE_PATH.exists():
        with open(POSTS_FILE_PATH, "w") as posts_file:
            writer = csv.DictWriter(posts_file, fieldnames=POST_FIELDS)
            writer.writeheader()

    captcha = None
    request_count = 0
    new_post_count = 0
    rate_sleep_interval = 300
    request_sleep_interval = 1

    start_dt = datetime.now()
    print(f"--- Старт парсинга ({start_dt.strftime('%d %h %Y %H:%M:%S')})")

    # Парсим пользователей
    print(f"Собираю информацию о пользователях...")
    try:
        res = get_users_info(','.join(users), captcha=captcha)
    except requests.RequestException as e:
        print(f'[ОШИБКА] Поймал request исключение {e}. Не удалось собрать пользовательскую информацию')
        return
    except Exception as e:
        print(f'[ОШИБКА] Поймал исключение {e}! Завершаюсь.')
        print(traceback.print_exc())
        return

    with open(USERS_FILE_PATH, 'a') as users_file:
        writer = csv.DictWriter(users_file, fieldnames=USERS_FIELDS)

        for user in res['response']:
            if str(user['id']) in users_id:
                print(f"Пользователь {user['id']} уже есть. Пропускаю.")
                continue

            writer.writerow({
                'id': user['id'],
                'about': user.get('about'),
                'domain': user['domain'],
                'status': user['status'],
                'last_name': user['last_name'],
                'first_name': user['first_name'],
            })
    print('Закончил собирать информацию о пользователях.')

    print('Начинаю собирать посты пользователей.')
    # Парсим посты
    with open(POSTS_FILE_PATH, "a") as posts_file:
        post_writer = csv.DictWriter(posts_file, fieldnames=POST_FIELDS)

        while True:
            if SHUTDOWN:
                end_dt = datetime.now()

                elapsed_td = end_dt - start_dt

                write_to_pkl(POSTS_ID_FILE_PATH, posts_id)
                write_to_pkl(USERS_ID_FILE_PATH, users_id)
                write_to_pkl(OFFSET_FILE_PATH, offset_list)

                posts_file.flush()

                print(f"--- Финиш парсинга ({end_dt.strftime('%d %h %Y %H:%M:%S')})")
                print(f"Парсинг занял {get_elapsed_time(elapsed_td)}.")
                print(f"Было спаршено {new_post_count}.")
                print(
                    f"Скорость парсинга ~{floor(new_post_count / elapsed_td.seconds)} пост/сек."
                )

                break

            for i, user in enumerate(users):
                if SHUTDOWN:
                    break

                if request_count == 3:
                    request_count = 0
                    time.sleep(request_sleep_interval)

                try:
                    res = get_wall_posts(user, offset=offset_list[i], captcha=captcha)
                except requests.RequestException as e:
                    print(f'[ОШИБКА] Поймал request исключение {e}. Засыпаю на 5 минут.')
                    time.sleep(300)
                    continue
                except Exception as e:
                    SHUTDOWN = True
                    print(f'[ОШИБКА] Поймал исключение {e}! Завершаюсь.')
                    print(traceback.format_exc())
                    continue

                if captcha:
                    captcha = None

                if "error" in res:
                    error = res["error"]

                    match error["error_code"]:
                        case 1:  # Unknown error
                            print("[ОШИБКА] Произошла неизвестная ошибка. Засыпаю")
                            time.sleep(5)
                        case 6:  # Too many requests error
                            request_count = 3
                            request_sleep_interval += 1
                            print(
                                f"Слишком много запросов в секунду! Увеличиваю интервал ожидания до {request_sleep_interval} сек."
                            )
                        case 10:  # Internal error
                            print(
                                "[ОШИБКА] Произошла внутренняя ошибка сервера. Засыпаю"
                            )
                            time.sleep(5)
                        case 14:  # Captcha
                            captcha = {"sid": error["captcha_sid"]}
                            captcha["key"] = input(
                                f"[ОШИБКА] Словил каптчу! Введите код с картинки по адресу {error['captcha_img']}: "
                            )
                        case 29:  # Rate error
                            print(
                                f"[ОШИБКА] Достигнут количественный лимит вызова метода. Засыпаю на {rate_sleep_interval // 60} минут."
                            )
                            time.sleep(rate_sleep_interval)
                        case any_code:  # Unhandled errors
                            print(f"[ОШИБКА] Необработанная ошибка с кодом {any_code}")
                            SHUTDOWN = True
                    continue

                if not "response" in res or not "items" in res["response"]:
                    print(
                        f"[ОШИБКА] Не обнаружен результат запроса. Засыпаю на {rate_sleep_interval}."
                    )
                    time.sleep(rate_sleep_interval)
                    continue

                for post in res["response"]["items"]:
                    post_id = f"{post['owner_id']}_{post['id']}"

                    if post_id in posts_id:
                        print(f"{post_id} уже есть. Пропускаю")
                        continue

                    text = post.get('text', '')
                    text = re.sub(r'\s+', ' ', text)

                    reposted_text = ''
                    copy_history = post.get('copy_history')
                    if copy_history and copy_history[-1].get('text'):
                        reposted_text = copy_history[-1].get('text')

                    if not (text or reposted_text):
                        print(f"У {post_id} нет пользовательского текста или нет текста у оригинального поста. Пропускаю.")
                        continue

                    post_writer.writerow(
                        {
                            "text": text,
                            "id": post["id"],
                            "timestamp": post["date"],
                            "from_id": post["from_id"],
                            "owner_id": post["owner_id"],
                            "reposted_text": reposted_text,
                            "comment_count": post["comments"]["count"],
                        }
                    )
                    posts_id.append(post_id)
                    new_post_count += 1
                    print(f"Добавил {post_id}")

                request_count += 1
                offset_list[i] += len(res["response"]["items"])


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)

    main()
