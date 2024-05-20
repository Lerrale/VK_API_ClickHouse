import pandas as pd
from clickhouse_driver import Client

df = pd.from_csv("vk_group_members")

# Подключение к ClickHouse
client = Client(host='localhost', port=9000, user='****', password='****', database='****')

client.execute('DROP TABLE IF EXISTS group_members')

client.execute('''
CREATE TABLE IF NOT EXISTS group_members (
    user_id_vk UInt32,
    first_name Nullable(String), 
    last_name Nullable(String),
    town Nullable(String),
    contacts Nullable(String),  
    last_seen Nullable(String),
    friends_count Nullable(String),
    bdate Nullable(String)
) ENGINE = MergeTree()
ORDER BY user_id_vk
''')

# Вставка данных в таблицу vk_users
data_users = df[['user_id_vk', 'first_name', 'last_name', 'town', 'contacts', 'last_seen', 'friends_count', 'bdate']].values.tolist()
client.execute('INSERT INTO group_members VALUES', data_users, types_check=True)
