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

    def __repr__(self):
        return f'Entry(path={self.path},mode={self.mode})'

    @classmethod
    def dumps(cls, obj):
        return json.dumps([obj.path, obj.mode, obj.file_type, obj.uname, obj.gname, obj.size, obj.mtime, obj.linkname], separators=(',', ':'))

    @classmethod
    def loads(cls, txt):
        o = json.loads(txt)
        return Entry(o[0], o[1], o[2], o[3], o[4], o[5], o[6], o[7])


def main():
    el = SQLList(Entry, 'entrylist')
    ent1 = Entry('abc1', 0o777, Entry.TYPE_FILE, 'user1', 'group1', 0, 100, None)
    ent2 = Entry('abc2', 0o777, Entry.TYPE_FILE, 'user1', 'group1', 0, 100, None)
    ent3 = Entry('abc3', 0o777, Entry.TYPE_FILE, 'user1', 'group1', 0, 100, None)
    el.insert(ent1)
    el.insert(ent2)
    el.insert(ent3)
    ent = el.get(2)
    print(ent)
    for ent in el.iterator(offset=1, sort='DESC'):
        print('???: ' + str(ent))

    em = SQLMap(Entry, 'entrymap')
    em.put(ent1.path, ent1)
    em.put(ent2.path, ent2)
    em.put(ent3.path, ent3)
    ent = em.get(ent2.path)
    print(ent)
    for ent in em.iterator(offset=1, sort='DESC'):
        print('???: ' + str(ent))

main()
