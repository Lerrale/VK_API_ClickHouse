import pandas as pd
import matplotlib.pyplot as plt
from clickhouse_driver import Client

client = Client(host='localhost', port=9000, user='****', password='****', database='****')

# Топ - 5 самых популярных имен
query_top_names = """
SELECT first_name, 
       COUNT(*) as popular_names
FROM group_members
GROUP BY first_name
ORDER BY popular_names DESC
LIMIT 5;
"""
result = client.execute(query_top_names)

df_top_names = pd.DataFrame(result, columns=['name', 'count'])

plt.figure(figsize=(10, 6))
plt.bar(df_top_names['name'], df_top_names['count'], color='skyblue')
plt.title('Топ - 5 самых популярных имен')
plt.savefig('top_5_names.png')

# Диаграммы рассеяния - Сколько лет (Кол-во друзей)
query_num_friends = """
SELECT
    CASE
        WHEN length(bdate) - length(replaceAll(bdate, '.', '')) = 2 THEN dateDiff('year', toDate(parseDateTimeBestEffortOrNull(bdate)), today())
        ELSE NULL
    END AS age,
    toInt32(friends_count) friends_int
FROM group_members
WHERE age IS NOT NULL AND friends_int IS NOT NULL;
"""
result = client.execute(query_num_friends)
df_num_friends = pd.DataFrame(result, columns=['age', 'friends_int'])

plt.figure(figsize=(10, 6))
plt.scatter(df_num_friends['age'], df_num_friends['friends_int'], alpha=0.5)
plt.xlabel('Возраст')
plt.ylabel('Кол-во друзей')
plt.title('Сколько лет (Кол-во друзей)')
plt.savefig('top_5_names.png')

# Выдать топ-3 города, в которых среднее кол-во друзей 
# участников группы самое наибольшее

query_top_town_friends = """
SELECT
    town,
    AVG(toInt32(friends_count)) average_friends
FROM group_members
WHERE town IS NOT NULL
GROUP BY town
ORDER BY average_friends DESC
LIMIT 3;
"""
result = client.execute(query_top_town_friends)
df_top_town_friends = pd.DataFrame(result, columns=['city', 'count'])

print(df_top_town_friends)

# Какой город самый часто встречаемый у участников этой группы

query_popular_town = """
SELECT town, 
       COUNT(*) as count
FROM group_members
WHERE town IS NOT NULL
GROUP BY town
ORDER BY count DESC
LIMIT 1;
"""
result = client.execute(query_popular_town)
df_popular_town = pd.DataFrame(result, columns=['city', 'count'])

print(df_popular_town)




