import sqlite3
import resource
import json

# メモリ使用量を 512MB に制限
max_memory = 512 * 1024 * 1024
resource.setrlimit(resource.RLIMIT_AS, (max_memory, max_memory))

# データベースに接続（存在しない場合は作成）
conn = sqlite3.connect('test-sqlite3.db')
c = conn.cursor()

# ジャーナルモードをWALに設定
c.execute('PRAGMA journal_mode = WAL')
# 自動コミットを無効にする
c.execute('PRAGMA synchronous = OFF')
# 64MBのキャッシュサイズ (単位: ページ, デフォルトは1024バイト/ページ)
c.execute('PRAGMA cache_size = -64000')

class SQLObj():
    @classmethod
    def dumps(cls, obj):
        raise NotImplementedError

    @classmethod
    def loads(cls, txt):
        raise NotImplementedError


class SQLMap():
    def __init__(self, cls, table_name):
        self.cls = cls
        self.table_name = table_name
        c.execute('''
        CREATE TABLE IF NOT EXISTS {} (
        key TEXT PRIMARY KEY,
        value TEXT
        )
        '''.format(table_name))

    def put(self, key, value, commit=False):
        serialized_data = self.cls.dumps(value)
        print(serialized_data)
        print(len(serialized_data))
        c.execute('''
        INSERT OR REPLACE INTO {} (key, value)
        VALUES (?, ?)
        '''.format(self.table_name), (key, serialized_data))
        if commit:
            conn.commit()

    def get(self, key):
        result = c.execute('''
        SELECT value FROM {} WHERE key = ?
        '''.format(self.table_name), (key,))
        row = result.fetchone()
        return self.cls.loads(row[0]) if row else None

    # sort: None, 'ASC', 'DESC'
    def iterator(self, sort=None, offset=0, limit=-1):
        sql = f'SELECT value FROM {self.table_name}'
        if sort is not None:
            if sort.upper() == 'ASC':
                sql += ' ORDER BY key ASC'
            elif sort.upper() == 'DESC':
                sql += ' ORDER BY key DESC'
        sql += f' LIMIT {limit} OFFSET {offset}'
        print(f'sql={sql}')
        result = c.execute(sql)
        while True:
            row = result.fetchone()
            if row is None:
                break
            yield self.cls.loads(row[0])


class SQLList():
    def __init__(self, cls, table_name):
        self.cls = cls
        self.table_name = table_name
        c.execute('''
        CREATE TABLE IF NOT EXISTS {} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data BLOB
        )
        '''.format(table_name))

    def insert(self, data, commit=False):
        serialized_data = self.cls.dumps(data)
        c.execute('''
        INSERT INTO {} (data) VALUES (?)
        '''.format(self.table_name), (serialized_data,))
        if commit:
            conn.commit()

    def get(self, data_id):
        result = c.execute('SELECT data FROM {} WHERE id = ?'.format(self.table_name), (data_id,))
        row = result.fetchone()
        return self.cls.loads(row[0]) if row else None

    # sort: None, 'ASC', 'DESC'
    def iterator(self, sort=None, offset=0, limit=-1):
        sql = f'SELECT data FROM {self.table_name}'
        if sort is not None:
            if sort.upper() == 'ASC':
                sql += ' ORDER BY id ASC'
            elif sort.upper() == 'DESC':
                sql += ' ORDER BY id DESC'
        sql += f' LIMIT {limit} OFFSET {offset}'
        print(f'sql={sql}')
        result = c.execute(sql)
        while True:
            row = result.fetchone()
            if row is None:
                break
            yield self.cls.loads(row[0])


class Entry(SQLObj):
    TYPE_FILE = 'F'
    TYPE_DIR = 'D'
    TYPE_SYMLINK = 'S'
    TYPE_OTHER = 'O'

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
        print('um: ' + str(self.user_map))
        um = cls.user_map.get(uname)
        if um is None:
            cls.user_map[uname] = self.user_map_count
            cls.user_map_count += 1
        gm = cls.group_map.get(gname)
        if gm is None:
            cls.group_map[gname] = self.group_map_count
            cls.group_map_count += 1

    def __repr__(self):
        return f'Entry(path={self.path},mode={self.mode},user={self.uname},group={self.gname})'

    @classmethod
    def dumps(cls, obj):
        t = cls.type_map[obj.file_type]
        u = cls.user_map[obj.uname]
        g = cls.group_map[obj.gname]
        return json.dumps([obj.path, obj.mode, t, u, g, obj.size, obj.mtime, obj.linkname], separators=(',', ':'))

    @classmethod
    def loads(cls, txt):
        o = json.loads(txt)
        t = cls.type_map_reverse[o[2]]
        u = [key for key, val in cls.user_map.items() if val == o[3]][0]
        g = [key for key, val in cls.group_map.items() if val == o[4]][0]
        return Entry(o[0], o[1], t, u, g, o[5], o[6], o[7])


def main():
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
        print('???: ' + str(ent))

    em = SQLMap(Entry, 'entrymap')
    for e in allent:
        em.put(e.path, e)
    ent = em.get(ent2.path)
    print(ent)
    for ent in em.iterator(offset=1, sort='DESC'):
        print('???: ' + str(ent))

main()
