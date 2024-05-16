import os
import sys
import time
import sqlite3
import resource
import json
import tempfile
import hashlib

print(str(sys.argv))

test_num = int(sys.argv[1])
tmpdir_path = sys.argv[2]

# メモリ使用量を 512MB に制限
max_memory = 512 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (max_memory, max_memory))

# データベースに接続（存在しない場合は作成）
tmpd = tempfile.TemporaryDirectory(dir=tmpdir_path)
tmpfname = os.path.join(tmpd.name, 'test-sqlite3.db')
conn = sqlite3.connect(tmpfname)

print(f'tempfile: {tmpfname}')

c = conn.cursor()

# ジャーナルモードをWALに設定  遅くなるようだ
#c.execute('PRAGMA journal_mode = WAL')

# 自動コミットを無効にする
c.execute('PRAGMA synchronous = OFF')

# 64MBのキャッシュサイズ (単位: ページ, デフォルトは1024バイト/ページ)
c.execute('PRAGMA cache_size = -64000')

class SQLObj():
    @classmethod
    def dumps(cls, obj):
        raise NotImplementedError

    @classmethod
    def loads(cls, key, txt):
        raise NotImplementedError


class SQLMap():
    def __init__(self, cls, table_name, clear=False):
        self.cls = cls
        self.table_name = table_name
        self.c = conn.cursor()
        if clear:
            self.clear()
        self._count = 0
        # c.execute('''
        # CREATE TABLE IF NOT EXISTS {} (
        # key TEXT PRIMARY KEY,
        # value TEXT
        # )
        # '''.format(table_name))
        self.c.execute('''
        CREATE TABLE IF NOT EXISTS {} (
        key TEXT,
        value TEXT
        )
        '''.format(table_name))

    def index(self):
        self.c.execute('CREATE INDEX idx_key ON {} (key)'.format(self.table_name))

    def count(self):
        return self._count

    def put(self, key, value, commit=False):
        serialized_data = self.cls.dumps(value)
        #print(serialized_data)
        #print(len(serialized_data))
        self.c.execute('''
        INSERT OR REPLACE INTO {} (key, value)
        VALUES (?, ?)
        '''.format(self.table_name), (key, serialized_data))
        self._count += 1
        if commit:
            conn.commit()

    def get(self, key):
        result = self.c.execute('''
        SELECT value FROM {} WHERE key = ?
        '''.format(self.table_name), (key,))
        row = result.fetchone()
        return self.cls.loads(key, row[0]) if row else None

    # sort: None, 'ASC', 'DESC'
    def iterator(self, sort=None, offset=0, limit=-1):
        sql = f'SELECT key,value FROM {self.table_name}'
        if sort is not None:
            if sort.upper() == 'ASC':
                sql += ' ORDER BY key ASC'
            elif sort.upper() == 'DESC':
                sql += ' ORDER BY key DESC'
        sql += f' LIMIT {limit} OFFSET {offset}'
        print(f'sql={sql}')
        result = self.c.execute(sql)
        while True:
            row = result.fetchone()
            if row is None:
                break
            yield row[0], self.cls.loads(row[0], row[1])

    def clear(self):
        self.c.execute('DROP TABLE IF EXISTS {}'.format(self.table_name))


class SQLList():
    def __init__(self, cls, table_name, clear=False):
        self.cls = cls
        self.table_name = table_name
        self.c = conn.cursor()
        if clear:
            self.clear()
        self.c.execute('''
        CREATE TABLE IF NOT EXISTS {} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data BLOB
        )
        '''.format(table_name))

    def insert(self, data, commit=False):
        serialized_data = self.cls.dumps(data, for_list=True)
        self.c.execute('''
        INSERT INTO {} (data) VALUES (?)
        '''.format(self.table_name), (serialized_data,))
        if commit:
            conn.commit()

    def get(self, data_id):
        result = self.c.execute('SELECT id,data FROM {} WHERE id = ?'.format(self.table_name), (data_id,))
        row = result.fetchone()
        return self.cls.loads(row[0], row[1], for_list=True) if row else None

    # sort: None, 'ASC', 'DESC'
    def iterator(self, sort=None, offset=0, limit=-1):
        sql = f'SELECT id,data FROM {self.table_name}'
        if sort is not None:
            if sort.upper() == 'ASC':
                sql += ' ORDER BY id ASC'
            elif sort.upper() == 'DESC':
                sql += ' ORDER BY id DESC'
        sql += f' LIMIT {limit} OFFSET {offset}'
        print(f'sql={sql}')
        result = self.c.execute(sql)
        while True:
            row = result.fetchone()
            if row is None:
                break
            yield self.cls.loads(row[0], row[1], for_list=True)

    def clear(self):
        self.c.execute('DROP TABLE IF EXISTS {}'.format(self.table_name))


class Path(SQLObj):
    def __init__(self, path):
        self.path = path

    @classmethod
    def dumps(cls, obj, for_list=False):
        return obj

    @classmethod
    def loads(cls, key, txt, for_list=False):
        return txt


class Entry(SQLObj):
    TYPE_FILE = 'FILE'
    TYPE_DIR = 'DIR'
    TYPE_SYMLINK = 'SYMLINK'
    TYPE_OTHER = 'OTHER'

    type_map = {
        TYPE_FILE: 1,
        TYPE_DIR: 2,
        TYPE_SYMLINK: 3,
        TYPE_OTHER: 4,
    }
    type_map_reverse = {v: k for k, v in type_map.items()}

    user_map = {}
    user_map_count = 0
    group_map = {}
    group_map_count = 0

    def __init__(self, path, mode, file_type, uname, gname,
                 size, mtime, linkname):
        self.path = path
        self.mode = mode
        self.file_type = file_type
        self.uname = uname
        self.gname = gname
        self.size = size
        self.mtime = mtime
        self.linkname = linkname

        cls = type(self)
        #print('DEBUG um: ' + str(self.user_map))
        um = cls.user_map.get(uname)
        if um is None:
            cls.user_map[uname] = self.user_map_count
            cls.user_map_count += 1
        gm = cls.group_map.get(gname)
        if gm is None:
            cls.group_map[gname] = self.group_map_count
            cls.group_map_count += 1

    def __repr__(self):
        return f'Entry(path={self.path},mode={oct(self.mode)},user={self.uname},group={self.gname})'

    @classmethod
    def dumps(cls, obj, for_list=False):
        t = cls.type_map[obj.file_type]
        u = cls.user_map[obj.uname]
        g = cls.group_map[obj.gname]
        array = [obj.mode, t, u, g, obj.size, obj.mtime, obj.linkname]
        if for_list:
            array.append(obj.path)  # o[7]
        return json.dumps(array, separators=(',', ':'))

    @classmethod
    def loads(cls, key, txt, for_list=False):
        o = json.loads(txt)
        if for_list:
            path = o[7]
        else:
            path = key
        t = cls.type_map_reverse[o[1]]
        u = [key for key, val in cls.user_map.items() if val == o[2]][0]
        g = [key for key, val in cls.group_map.items() if val == o[3]][0]
        return Entry(path, o[0], t, u, g, o[4], o[5], o[6])


def test_many(num):
    em = SQLMap(Entry, 'entrymap', clear=True)

    start_time = time.time()
    previous = start_time
    for i in range(num):
        path = hashlib.sha512(i.to_bytes(4, 'big')).hexdigest()
        #path = hashlib.sha256(i.to_bytes(4, 'big')).hexdigest()
        e = Entry(path, 0o755, Entry.TYPE_FILE, 'user1', 'group1', 0, 100, None)
        em.put(e.path, e)
        if i % 1000000 == 0:
            now = time.time()
            print(f'progress: {i}/{num} | {now - previous} | {now - start_time}')
            previous = now
    insertion_time = time.time() - start_time
    print(f'Data insertion took {insertion_time} seconds')

    start_time = time.time()
    em.index()
    index_time = time.time() - start_time
    print(f'Data index creation took {index_time} seconds')

    start_time = time.time()
    count = em.count()
    center = int(count/2)
    if count > 100:
        for path, ent in em.iterator(offset=0, limit=center, sort='DESC'):
            pass
        start_time2 = time.time()
        for path, ent in em.iterator(offset=center, limit=-1, sort='DESC'):
            pass
    else:
        for path, ent in em.iterator(offset=0, limit=center, sort='DESC'):
            print('entry(part1): ' + str(ent))
        start_time2 = time.time()
        for path, ent in em.iterator(offset=center, limit=-1, sort='DESC'):
            print('entry(part2): ' + str(ent))
    fetch1_time = start_time2 - start_time
    fetch2_time = time.time() - start_time2
    print(f'Data fetching 1/2 took {fetch1_time} seconds')
    print(f'Data fetching 2/2 took {fetch2_time} seconds')

    start_time = time.time()
    pl = SQLList(Path, 'sorted_pathlist')
    for path, ent in em.iterator(sort='DESC'):
        #print(f'DEBUG path={path}, ent={str(ent)}')
        #print(f'DEBUG path={path}')
        pl.insert(path)
    create_sorted_time = time.time() - start_time
    print(f'Sorted data creation took {create_sorted_time} seconds')

    start_time = time.time()
    if count > 100:
        for path in pl.iterator():
            ent = em.get(path)
    else:
        #for path in pl.iterator(sort='DESC'):
        for path in pl.iterator():
            #print('path: ' + path)
            ent = em.get(path)
            print('entry(sorted): ' + str(ent))
    fetch3_time = time.time() - start_time
    print(f'Data fetching (sorted) took {fetch3_time} seconds')


def test():
    el = SQLList(Entry, 'entrylist')
    ent1 = Entry('abc1', 0o777, Entry.TYPE_FILE, 'user1', 'group1', 0, 100, None)
    ent2 = Entry('abc2', 0o777, Entry.TYPE_FILE, 'user1', 'group2', 0, 100, None)
    ent3 = Entry('abc3', 0o777, Entry.TYPE_DIR, 'user2', 'group1', 0, 100, None)
    ent4 = Entry('abc4', 0o777, Entry.TYPE_DIR, 'user3', 'group3', 0, 100, None)
    allent = [ent1, ent2, ent3, ent4]
    for e in allent:
        el.insert(e)
    ent = el.get(2)
    print(ent)
    for ent in el.iterator(offset=1, sort='DESC'):
        print('from list: ' + str(ent))

    em = SQLMap(Entry, 'entrymap')
    for e in allent:
        em.put(e.path, e)
    ent = em.get(ent2.path)
    print(ent)
    for ent in em.iterator(offset=1, sort='DESC'):
        print('from map: ' + str(ent))

#test()

test_many(test_num)

#conn.commit()

# データベースファイルのサイズを取得
db_size = os.path.getsize(tmpfname)
# データベースファイルのサイズを表示
print(f'Database Size: {db_size} bytes')

tmpd.cleanup()
