# pip install clickhouse_driver

import time
import json
import requests
import pandas as pd


FIELDS = "bdate,last_seen,contacts,city"
METHOD_GROUP_MEMBERS = "groups.getMembers"
GROUPID = "vk_fishing"
TOKEN = "*****"

# Первым этапом получим поля user_id_vk, first_name,
# last_name, last_seen, contacts, town, а так же bdate
# (т.к. далее требуется построить диаграмму рассеяния,
# в которой требуется возраст).

# Узнаем общее кол-во необходимых запросов, зная, что
# один запрос возвращает 1000 записей
req = requests.get(f"https://api.vk.com/method/{METHOD_GROUP_MEMBERS}?group_id={GROUPID}&fields={FIELDS}&access_token={TOKEN}&v=5.199 HTTP/1.1").json()
NUM_PAGES = int(req["response"]["count"] / 1000)

# Базовый сбор данных, получаем все поля кроме friends_count
res = []
for i in range(NUM_PAGES+1):
    pagination = i * 1000
    try:
        req = requests.get(f"https://api.vk.com/method/{METHOD_GROUP_MEMBERS}?group_id={GROUPID}&offset={pagination}&fields={FIELDS}&access_token={TOKEN}&v=5.199 HTTP/1.1").json()
        if "response" in req:
            res.extend(req["response"]["items"])
            if i % 50 == 0:
                print(req["response"]["items"][1])  # Проверка каждой 1й записи каждой 50й страницы, что не получили капчу или ограничения
        else:
            raise ValueError(f"{req}")
    except Exception as e:
        print(f"An error occurred: {e}")
    time.sleep(0.35)

with open('raw_group_members.json', 'w') as f:
    json.dump(res, f)

with open('raw_group_members.json', 'r') as f:
    df_raw_json = json.load(f)

df_without_friends = pd.json_normalize(df_raw_json)     
# Загрузка данных по друзьям

# Список для хранения результатов
results = []

# Список id пользователей
user_id = df_without_friends['id'].tolist()
user_id.sort()

# 1й метод - friends.get

# Этим методом получается отправлять в день
# около 5000 запросов, даже через метод execute,
# затем идет блокировка на день. В сумме им получилось
# достать данные по 10999 пользователям.

# Перебираем список ID и отправляем запросы по 25 ID за раз c
# помощью метода execute и VKScript
def get_friends(user_ids):
    """
    Функция отправляет запросы к API VK, чтобы получить количество друзей
    для каждого пользователя в списке user_ids. Запросы отправляются пакетами
    по 25 пользователей с задержкой между запросами для предотвращения
    превышения лимита запросов.

    Параметры:
    user_ids (list): Список идентификаторов пользователей.

    Обработка ошибок:
    - Если ошибка связана с достижением лимита запросов (error_code == 29),
      выполнение цикла прекращается.
    - Все другие исключения обрабатываются в блоке except.
    """
    try:
        for i in range(0, len(user_ids), 25):
            time.sleep(0.35)
            batch = user_ids[i:i + 25]
            code = """
            var users = %s;
            var result = [];
            var i = 0;
            while (i < users.length) {
                var friends = API.friends.get({"user_id": users[i]});
                if (friends) {
                    result.push({"user_id": users[i], "friends_count": friends.count});
                }
                i = i + 1;
            }
            return result;
            """ % json.dumps(batch)
          
            response = requests.post(
                'https://api.vk.com/method/execute',
                data={
                    'code': code,
                    'access_token': TOKEN,
                    'v': 5.199
                }
            ).json()     
            if 'response' in response:
                results.extend(response['response'])
            elif 'error' in response:
                error_code = response['error']['error_code']
                print(f"Error for batch starting with ID {batch[0]}: {response['error']}")
                if error_code == 29:
                    print("Rate limit reached. Stopping the execution.")
                    break
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


get_friends(user_id)

with open('friends.json', 'w') as f:
    json.dump(results, f)

# 2й метод - users.get

# У этого метода нет суточных ограничений, но он
# довольно медленный. Им получилось скачать данные
# по 80000 за ночь через execute. Я его запускала в
# другом ноутбуке, поэтому здесь только пример функции
# и готовый результат скачивания.

# def get_friends(user_ids):
#     try:
#         for i in tqdm(range(0, 3)):
#             time.sleep(0.35)
#             batch = user_ids[i:i + 25]
#             code = """
#             var users = %s;
#             var result = [];
#             var i = 0;
#             while (i < users.length) {
#                 var user_info = API.users.get({
#                     "user_ids": users[i],
#                     "fields": "counters"
#                 })[0];
#                 if (user_info) {
#                     var friends_count = user_info.counters ? user_info.counters.friends : null;
#                     result.push({"user_id": users[i], "friends_count": friends_count});
#                 }
#                 i = i + 1;
#             }
#             return result;
#             """ % json.dumps(batch)
            
#             response = requests.post(
#                 'https://api.vk.com/method/execute',
#                 data={
#                     'code': code,
#                     'access_token': TOKEN,
#                     'v': 5.199
#                 }
#             ).json()      
#             if 'response' in response:
#                 results.extend(response['response'])
#             elif 'error' in response:
#                 error_code = response['error']['error_code']
#                 print(f"Error for batch starting with ID {batch[0]}: {response['error']}")
#                 if error_code == 29:
#                     print("Rate limit reached. Stopping the execution.")
#                     break
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")

with open('friends_2.json', 'r') as f:
    friends_2 = json.load(f)

with open('friends.json', 'r') as f:
    friends_1 = json.load(f)

# Объединим списки

friends_data = friends_1 + friends_2

# ОБЪЕДИНЕНИЕ ДАННЫХ

with open('raw_group_members.json', 'r') as f:
    df_raw_json = json.load(f)

# Переводим данные в табличный вид
df_without_friends = pd.json_normalize(df_raw_json)
df_friends = pd.DataFrame(friends_data)

df_friends = df_friends.drop_duplicates()

df = pd.merge(df_without_friends, df_friends, left_on='id', right_on='user_id', how='left')

# ОБРАБОТКА ДАННЫХ

# Оставляем только нужные столбцы
df = df[["id", "first_name", "last_name", "city.title", "last_seen.time", "bdate", "mobile_phone", "home_phone", "friends_count"]]

# Переименовываем
df = df.rename({'id': 'user_id_vk', 'city.title': 'town', 'last_seen.time': 'last_seen'}, axis=1)

# Столбцы mobile_phone и home_phone заменяем на один столбец contacts
df['contacts'] = df['mobile_phone'].combine_first(df['home_phone'])
df = df.drop(['mobile_phone', 'home_phone'], axis=1)

df = df[['user_id_vk', 'first_name', 'last_name', 'last_seen', 'contacts', 'friends_count', 'town', 'bdate']]

# Поменяем типы данных на более подходящие
# в Clickhouse невозможно загрузить типы данных такие как NaN, pd.NA, 
# NaT, поэтому на данном этапе придумала только загрузку в виде строк.

df['last_seen'] = df['last_seen'].apply(lambda x: None if pd.isna(x) else str(x))
df['last_seen'] = df['last_seen'].apply(lambda x: None if pd.isna(x) else (x[:(len(x)-2)]))

df['contacts'] = df['contacts'].apply(lambda x: None if pd.isna(x) else str(x))

df['town'] = df['town'].apply(lambda x: None if pd.isna(x) else str(x))

df['bdate'] = df['bdate'].apply(lambda x: None if pd.isna(x) else str(x))

df['friends_count'] = df['friends_count'].apply(lambda x: None if pd.isna(x) else str(x))
df['friends_count'] = df['friends_count'].apply(lambda x: None if pd.isna(x) else (x[:(len(x)-2)]))

data = df.values.tolist()

# Финальный набор данных
df.to_csv("vk_group_members", index=False)