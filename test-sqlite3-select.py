import os
import sqlite3
import random
import string
import time
import sys
import resource

#max_memory = 512 * 1024 * 1024
max_memory = 5120 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (max_memory, max_memory))

# データベースファイルのパス
db_file = 'test-sqlite.db'

# データベースファイルが存在する場合は削除
if os.path.exists(db_file):
    os.remove(db_file)

# SQLiteデータベースの作成と接続
conn = sqlite3.connect(db_file)
#conn = sqlite3.connect(':memory:')
c = conn.cursor()

# テーブルの作成 (dict 相当)
c.execute('''CREATE TABLE data (key TEXT PRIMARY KEY, value TEXT)''')
c.execute('PRAGMA synchronous = OFF')  # 効果は小さい
c.execute('PRAGMA journal_mode = OFF')  # disable rollback

# データの挿入
total_entries = 1000000000  # 10億エントリ
#total_entries = 100000000  # 1億エントリ
#total_entries =  10000000 # 1000万
#total_entries =  1000000 # 100万

batch_size = 10000000  # バッチサイズ
#batch_size = 1000000  # バッチサイズ
#batch_size = 10000  # バッチサイズ
#batch_size = 1  # バッチサイズ
progress_interval = 1000000

start_time = time.time()
previous = start_time

def benchmark_select(num):
    a = time.time()
    test_count = 1000
    for i in range(test_count):
        res = c.execute(f"SELECT value FROM data WHERE key = ?", (str(num),))
        row = res.fetchone()
        if not row:
            print("unexpected!!!!")
    b = time.time()
    print(f'select: {num} | {b - a}')
    
#for i in range(total_entries, 0, -batch_size): # 降順
for i in range(0, total_entries, batch_size): # 照準
    data = []
    now = time.time()
    if i % progress_interval == 0:
        print(f'progress: {i}/{total_entries} | {now - previous} | {now - start_time}')
    previous = now
    for j in range(i, i + batch_size):
        data.append([str(j), str(j)])
    c.executemany('INSERT INTO data VALUES (?, ?)', data)
    benchmark_select(i)

conn.commit()

insertion_time = time.time() - start_time
print(f'Data insertion took {insertion_time} seconds')

conn.close()
