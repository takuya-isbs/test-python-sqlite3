import os
import sqlite3
import random
import string
import time
import sys
import resource

# usage:


max_memory = 512 * 1024 * 1024
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

# テーブルの作成
c.execute('''CREATE TABLE data (path TEXT, mode INTEGER, type TEXT, user TEXT, gname TEXT, size INTEGER, mtime INTEGER, linkname TEXT)''')
c.execute('PRAGMA synchronous = OFF')  # 効果は小さい
#c.execute('CREATE INDEX idx_path ON data (path)')  # INDEX 作成しながらだと効率悪い

# データの挿入
total_entries = 100000000  # 1億エントリ
#total_entries =  10000000 # 1000万
#total_entries =  1000000 # 100万

#batch_size = 1000000  # バッチサイズ
#batch_size = 10000  # バッチサイズ
batch_size = 1  # バッチサイズ
progress_interval = 1000000

start_time = time.time()
previous = start_time

for i in range(total_entries, 0, -batch_size): # 降順
    data = []
    now = time.time()
    if i % progress_interval == 0:
        print(f'progress: {i}/{total_entries} | {now - previous} | {now - start_time}')
    previous = now
    for j in range(i, i + batch_size):
        # path, mode, file_type(str), uname, gname, size, mtime, linkname(str)
        data.append([hex(j) * 10, 0o600, 'FILE', 'user001122', 'user001122', 1000, 1707405506, ""])
    c.executemany('INSERT INTO data VALUES (?, ?, ?, ?, ?, ?, ?, ?)', data)

conn.commit()

insertion_time = time.time() - start_time
print(f'Data insertion took {insertion_time} seconds')

start_time = time.time()
c.execute('CREATE INDEX idx_path ON data (path)') # あとから INDEX 作成
index_time = time.time() - start_time
print(f'Data index creation took {index_time} seconds')

# ソートされたデータの取得と表示
#start_time = time.time()

# ソート済みの一時テーブルを作成
#c.execute("CREATE TEMPORARY TABLE sorted_table AS SELECT * FROM data ORDER BY path DESC")

#sort_time = time.time() - start_time
#print(f'Data pre sorting took {sort_time} seconds')

# テーブルの全体数を取得
start_time = time.time()
c.execute("SELECT COUNT(*) FROM data")
total_rows = c.fetchone()[0]
count_time = time.time() - start_time
print(f'Data counting took {count_time} seconds')

if total_rows != total_entries:
    print(f'total_rows{total_rows} != total_entries{total_entries}')
    sys.exit(1)

# グループごとに分けて取得
# 各グループのサイズを計算
group_num = 4
group_size = int(total_entries / group_num)
remainder = total_entries % group_num

# グループごとの範囲を定義
ranges = []
start_index = 0
for i in range(group_num):
    end_index = start_index + group_size + (1 if i < remainder else 0)
    ranges.append((start_index, end_index))
    start_index = end_index

#sorted_data = c.execute('SELECT * FROM data ORDER BY path DESC') # 降順
#sorted_data = c.execute('SELECT * FROM data ORDER BY path ASC') # 昇順

group_sql = []
for start, end in ranges:
    # LIMIT offset, count
    count = end - start
    sorted_sql = f'SELECT * FROM data ORDER BY path DESC LIMIT {start}, {count}'
    group_sql.append(sorted_sql)


# 最初の3行を表示
start_time = time.time()
for g in group_sql:
    print("-------------------------------------------")
    result = c.execute(g)
    for row in result.fetchmany(3):
        print(row)

fetch_time = time.time() - start_time
print(f'Data fetching(with sorting) (1) took {fetch_time} seconds')

# 最初の3行を表示 2回目
# start_time = time.time()
# for g in group_sql:
#     print("-------------------------------------------")
#     result = c.execute(g)
#     for row in result.fetchmany(3):
#         print(row)
# fetch_time = time.time() - start_time
# print(f'Data fetching(with sorting) (2) took {fetch_time} seconds')

print("-------------------------------------------")
# ジェネレータ関数を定義してデータを1エントリずつ取得する
def fetch_data(cursor):
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        yield row

start_time = time.time()
for g in group_sql:
    result = c.execute(g)
    for row in fetch_data(result):
        #print(row)
        pass
fetch_time = time.time() - start_time
print(f'Data fetching all took {fetch_time} seconds')

# データベースファイルのサイズを取得
db_size = os.path.getsize(db_file)

# データベースファイルのサイズを表示
print(f'Database Size: {db_size} bytes')

# 接続のクローズ
conn.close()
