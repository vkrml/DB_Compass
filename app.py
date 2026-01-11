import os
import json
import logging
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from flask import Flask, request, redirect, url_for, session, render_template, flash, Markup
from jinja2 import DictLoader
from bson import json_util, ObjectId

# Database Drivers
from pymongo import MongoClient, ASCENDING, DESCENDING
from sqlalchemy import create_engine, inspect, text
import redis

# ==========================================
# CONFIGURATION
# ==========================================
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'links4u_ultimate_key')
app.jinja_env.globals.update(max=max, min=min, str=str, type=type, len=len, list=list, int=int)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
ROWS_PER_PAGE = 50

# ==========================================
# UTILITIES
# ==========================================
@app.template_filter('highlight')
def highlight_filter(s, query):
    if not query or not s: return s
    # Escape HTML first to prevent injection, then highlight
    s = str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    pattern = re.compile(re.escape(query), re.IGNORECASE)
    return Markup(pattern.sub(lambda m: f'<mark class="bg-yellow-500/50 text-white rounded-sm px-0.5">{m.group(0)}</mark>', s))

@app.template_filter('to_json')
def to_json_filter(value):
    return json_util.dumps(value, indent=2)

# ==========================================
# ADAPTERS
# ==========================================
class DatabaseAdapter:
    def connect(self, uri, db_name=None): raise NotImplementedError
    def list_databases(self): raise NotImplementedError
    def drop_database(self, db_name): raise NotImplementedError
    def list_tables(self, db_name): raise NotImplementedError
    def drop_table(self, db_name, table): raise NotImplementedError
    def get_rows(self, db_name, table, page, search=None, sort_col=None, sort_dir='desc'): raise NotImplementedError
    def get_row(self, db_name, table, id): raise NotImplementedError
    def save_row(self, db_name, table, id, data, is_new): raise NotImplementedError
    def delete_row(self, db_name, table, id): raise NotImplementedError

class MongoAdapter(DatabaseAdapter):
    def __init__(self): self.client = None
    
    def connect(self, uri, db_name=None):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.client.server_info()
        return True

    def list_databases(self): return sorted(self.client.list_database_names())
    def drop_database(self, db_name): self.client.drop_database(db_name)
    def list_tables(self, db_name): return sorted(self.client[db_name].list_collection_names())
    def drop_table(self, db_name, table): self.client[db_name].drop_collection(table)

    def get_rows(self, db_name, table, page, search=None, sort_col=None, sort_dir='desc'):
        col = self.client[db_name][table]
        query = {}
        
        if search:
            search = search.strip()
            # Deep search using $regex on fields is hard generically. 
            # We try ID match first, then a "catch-all" if user provides JSON, 
            # OR we rely on a specific logic. 
            # For simplicity in this generic tool: Search ID OR try string match on specific known text fields?
            # Better: $where (slow) or just ID.
            # PRO APPROACH: We will use regex on the JSON string representation (Client side is easier, but Server side requires $where)
            # Safe Fallback: Search ID.
            try: query = {'_id': ObjectId(search)}
            except: query = {'_id': {'$regex': search, '$options': 'i'}}

        sort_field = sort_col if sort_col else '_id'
        if sort_field == 'id': sort_field = '_id'
        direction = ASCENDING if sort_dir == 'asc' else DESCENDING

        total = col.count_documents(query)
        skip = (page - 1) * ROWS_PER_PAGE
        cursor = col.find(query).sort(sort_field, direction).skip(skip).limit(ROWS_PER_PAGE)
        
        rows = []
        for doc in cursor:
            doc['__id'] = str(doc['_id'])
            # Add string representation for raw view
            doc['__raw'] = json_util.dumps(doc) 
            rows.append(doc)
        return rows, total

    def get_row(self, db_name, table, id):
        try: oid = ObjectId(id)
        except: oid = id
        return self.client[db_name][table].find_one({'_id': oid})

    def save_row(self, db_name, table, id, data, is_new):
        col = self.client[db_name][table]
        if is_new: col.insert_one(data)
        else:
            try: oid = ObjectId(id)
            except: oid = id
            if '_id' in data: del data['_id']
            col.replace_one({'_id': oid}, data)

    def delete_row(self, db_name, table, id):
        try: oid = ObjectId(id)
        except: oid = id
        self.client[db_name][table].delete_one({'_id': oid})

class SQLAdapter(DatabaseAdapter):
    def __init__(self): 
        self.engine = None
        self.base_uri = ""

    def connect(self, uri, db_name=None):
        if uri.startswith("postgres://"): uri = uri.replace("postgres://", "postgresql://", 1)
        try:
            parsed = urlparse(uri)
            qs = parse_qs(parsed.query)
            if 'channel_binding' in qs: del qs['channel_binding']
            uri = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
        except: pass
        
        if db_name:
            u = urlparse(uri)
            uri = urlunparse(u._replace(path=f"/{db_name}"))

        self.engine = create_engine(uri)
        with self.engine.connect() as conn: pass
        return True

    def list_databases(self):
        dialect = self.engine.dialect.name
        try:
            with self.engine.connect() as conn:
                if dialect == 'postgresql':
                    res = conn.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false;"))
                    return sorted([r[0] for r in res])
                elif dialect == 'mysql':
                    res = conn.execute(text("SHOW DATABASES;"))
                    return sorted([r[0] for r in res])
                else: return [urlparse(str(self.engine.url)).database or 'main']
        except: return ['default']

    def drop_database(self, db_name):
        # NOTE: Dropping DB in SQL usually requires AUTOCOMMIT isolation level
        # This is a dangerous op, implemented minimally for Postgres
        if 'postgresql' in self.engine.dialect.name:
            eng = create_engine(self.engine.url.set(database='postgres'))
            conn = eng.connect()
            conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(f"DROP DATABASE {db_name}"))
            conn.close()

    def list_tables(self, db_name): return sorted(inspect(self.engine).get_table_names())
    def drop_table(self, db_name, table): 
        with self.engine.begin() as conn: conn.execute(text(f"DROP TABLE {table}"))

    def get_pk(self, table):
        try:
            pk = inspect(self.engine).get_pk_constraint(table)
            return pk['constrained_columns'][0] if pk['constrained_columns'] else 'id'
        except: return 'id'

    def get_rows(self, db_name, table, page, search=None, sort_col=None, sort_dir='desc'):
        pk = self.get_pk(table)
        sort_field = sort_col if sort_col else pk
        offset = (page - 1) * ROWS_PER_PAGE
        
        where_clause = ""
        params = {}
        
        if search:
            # POSTGRES MAGIC: Convert row to text and search inside
            if 'postgresql' in self.engine.dialect.name:
                where_clause = f"WHERE {table}::text ILIKE :search"
                params['search'] = f"%{search}%"
            else:
                # Basic SQL: Search PK only
                where_clause = f"WHERE {pk} LIKE :search"
                params['search'] = f"%{search}%"

        sql_count = text(f"SELECT COUNT(*) FROM {table} {where_clause}")
        sql_rows = text(f"SELECT * FROM {table} {where_clause} ORDER BY {sort_field} {sort_dir.upper()} LIMIT {ROWS_PER_PAGE} OFFSET {offset}")

        with self.engine.connect() as conn:
            try: total = conn.execute(sql_count, params).scalar()
            except: total = 0
            
            result = conn.execute(sql_rows, params)
            rows = []
            for r in result:
                d = dict(r._mapping)
                for k,v in d.items():
                    if hasattr(v, 'isoformat'): d[k] = v.isoformat()
                    if isinstance(v, bytes): d[k] = "<binary>"
                d['__id'] = str(d.get(pk))
                d['__raw'] = json.dumps(d, default=str)
                rows.append(d)
            return rows, total

    def get_row(self, db_name, table, id):
        pk = self.get_pk(table)
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT * FROM {table} WHERE {pk} = :id"), {"id": id}).mappings().first()
            if res:
                d = dict(res)
                for k,v in d.items():
                    if hasattr(v, 'isoformat'): d[k] = v.isoformat()
                return d
            return None

    def save_row(self, db_name, table, id, data, is_new):
        pk = self.get_pk(table)
        if '__id' in data: del data['__id']
        with self.engine.begin() as conn:
            if is_new:
                cols = ", ".join(data.keys())
                vals = ", ".join([f":{k}" for k in data.keys()])
                conn.execute(text(f"INSERT INTO {table} ({cols}) VALUES ({vals})"), data)
            else:
                sets = ", ".join([f"{k}=:{k}" for k in data.keys()])
                data['pk_val'] = id
                conn.execute(text(f"UPDATE {table} SET {sets} WHERE {pk} = :pk_val"), data)

    def delete_row(self, db_name, table, id):
        pk = self.get_pk(table)
        with self.engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {table} WHERE {pk} = :id"), {"id": id})

class RedisAdapter(DatabaseAdapter):
    def __init__(self): self.r = None
    def connect(self, uri, db_name=None):
        self.r = redis.from_url(uri, decode_responses=True)
        if db_name:
            try:
                self.r = redis.Redis(**self.r.connection_pool.connection_kwargs, db=int(db_name.replace("DB", "").strip()), decode_responses=True)
            except: pass
        self.r.ping()
        return True

    def list_databases(self): return [f"DB {i}" for i in range(16)]
    def drop_database(self, db_name): self.r.flushdb()
    def list_tables(self, db_name): return ["Keys"]
    def drop_table(self, db_name, table): self.r.flushdb()

    def get_rows(self, db_name, table, page, search=None, sort_col=None, sort_dir='desc'):
        pattern = f"*{search}*" if search else "*"
        keys = sorted(self.r.keys(pattern), reverse=(sort_dir=='desc'))
        total = len(keys)
        start = (page - 1) * ROWS_PER_PAGE
        page_keys = keys[start : start + ROWS_PER_PAGE]
        rows = []
        for k in page_keys:
            t = self.r.type(k)
            v = self.r.get(k) if t == 'string' else f"({t})"
            rows.append({'__id': k, 'type': t, 'value': v, '__raw': json.dumps({'key':k, 'val': v})})
        return rows, total

    def get_row(self, db_name, table, id):
        t = self.r.type(id)
        if t == 'none': return None
        val = None
        if t == 'string': val = self.r.get(id)
        elif t == 'hash': val = self.r.hgetall(id)
        elif t == 'list': val = self.r.lrange(id, 0, -1)
        elif t == 'set': val = list(self.r.smembers(id))
        return {'key': id, 'type': t, 'value': val}

    def save_row(self, db_name, table, id, data, is_new):
        key = data.get('key', id)
        val = data.get('value')
        if isinstance(val, dict): self.r.hset(key, mapping=val)
        elif isinstance(val, list): 
            self.r.delete(key)
            self.r.rpush(key, *val)
        else: self.r.set(key, str(val))
    def delete_row(self, db_name, table, id): self.r.delete(id)

def get_adapter(db_name=None):
    uri = session.get('db_uri')
    if not uri: return None
    try:
        if uri.startswith('mongo'): adp = MongoAdapter()
        elif 'redis' in uri: adp = RedisAdapter()
        else: adp = SQLAdapter()
        adp.connect(uri, db_name)
        return adp
    except Exception as e:
        logging.error(e)
        return None

# ==========================================
# UI TEMPLATES
# ==========================================

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="en" class="h-full bg-gray-900 text-gray-100 antialiased">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Links4u DB Compass</title>
    
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js" defer></script>
    
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/theme/material-darker.min.css">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/mode/javascript/javascript.min.js"></script>

    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: { sans: ['Inter', 'sans-serif'], mono: ['JetBrains Mono', 'monospace'] },
                    colors: { 
                        gray: { 750: '#2d3748', 850: '#1a202c', 900: '#111827', 950: '#0B0F19' },
                        brand: { 500: '#10B981', 600: '#059669' }
                    }
                }
            }
        }
    </script>
    <style>
        .scrollbar-thin::-webkit-scrollbar { width: 6px; height: 6px; }
        .scrollbar-thin::-webkit-scrollbar-track { background: #111827; }
        .scrollbar-thin::-webkit-scrollbar-thumb { background: #374151; border-radius: 3px; }
        .CodeMirror { height: 100%; font-family: 'JetBrains Mono'; font-size: 13px; background: #0B0F19; }
        mark { color: white; background: #d97706; }
    </style>
</head>
<body class="h-full flex flex-col md:flex-row overflow-hidden" x-data="{ sidebarOpen: false }">

    <header class="md:hidden flex items-center justify-between p-4 border-b border-gray-800 bg-gray-900 z-20">
        <div class="flex items-center gap-2 text-brand-500 font-bold">
            <svg class="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" /></svg>
            <span>L4U Compass</span>
        </div>
        <button @click="sidebarOpen = !sidebarOpen" class="text-gray-400 hover:text-white">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path></svg>
        </button>
    </header>

    <aside :class="sidebarOpen ? 'translate-x-0' : '-translate-x-full'" class="fixed inset-y-0 left-0 z-30 w-72 bg-gray-950 border-r border-gray-800 transition-transform duration-300 md:relative md:translate-x-0 flex flex-col">
        <div class="p-5 border-b border-gray-800 hidden md:flex items-center gap-3">
            <div class="p-1.5 bg-brand-500/20 rounded text-brand-500">
                <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" /></svg>
            </div>
            <h1 class="font-bold text-gray-100 tracking-tight">Links4u Compass</h1>
        </div>

        {% if session.get('db_uri') %}
        <nav class="flex-grow overflow-y-auto p-4 space-y-8 scrollbar-thin">
            <div>
                <h3 class="text-xs font-bold text-gray-500 uppercase tracking-wider mb-3 px-2">Databases</h3>
                <ul class="space-y-0.5">
                    {% for db in dbs %}
                    <li>
                        <a href="{{ url_for('list_tables', db_name=db) }}" class="flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-colors {{ 'bg-brand-500/10 text-brand-500 font-medium' if session.get('current_db_name') == db else 'text-gray-400 hover:text-gray-200 hover:bg-gray-800' }}">
                            <svg class="w-4 h-4 opacity-70" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 01-2 2v4a2 2 0 012 2h14a2 2 0 012-2v-4a2 2 0 01-2-2m-2-4h.01M17 16h.01" /></svg>
                            <span class="truncate">{{ db }}</span>
                        </a>
                    </li>
                    {% endfor %}
                </ul>
            </div>
        </nav>
        <div class="p-4 border-t border-gray-800">
             <a href="{{ url_for('logout') }}" class="flex items-center justify-center w-full py-2 text-xs font-bold text-red-400 hover:bg-red-500/10 rounded-md transition-colors gap-2">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" /></svg>
                DISCONNECT
             </a>
        </div>
        {% endif %}
    </aside>

    <div x-show="sidebarOpen" @click="sidebarOpen = false" class="fixed inset-0 bg-black/80 z-20 md:hidden backdrop-blur-sm"></div>

    <main class="flex-grow flex flex-col h-full overflow-hidden relative bg-gray-900">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
            <div class="absolute top-6 right-6 z-50 w-full max-w-sm space-y-2 pointer-events-none">
                {% for category, message in messages %}
                <div class="pointer-events-auto shadow-lg rounded-lg p-4 border flex justify-between items-start {{ 'bg-red-900/90 border-red-700 text-red-100' if category == 'error' else 'bg-green-900/90 border-green-700 text-green-100' }}">
                    <span class="text-sm font-medium">{{ message }}</span>
                    <button onclick="this.parentElement.remove()" class="text-white/60 hover:text-white">&times;</button>
                </div>
                {% endfor %}
            </div>
            {% endif %}
        {% endwith %}

        {% block content %}{% endblock %}
    </main>
</body>
</html>
"""

LOGIN_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex items-center justify-center p-4">
    <div class="w-full max-w-md">
        <div class="text-center mb-10">
            <h1 class="text-3xl font-bold text-white mb-2">Welcome Back</h1>
            <p class="text-gray-500">Enter your connection string to access your data.</p>
        </div>
        
        <form method="POST" action="{{ url_for('connect_db') }}" class="space-y-6">
            <div>
                <div class="relative">
                    <input type="text" name="db_uri" placeholder="postgresql://... or mongodb://..." required autofocus
                           class="w-full bg-gray-950 border border-gray-700 text-white p-4 rounded-lg focus:ring-2 focus:ring-brand-500 focus:border-transparent outline-none font-mono text-sm shadow-xl transition-all">
                </div>
            </div>
            <button type="submit" class="w-full py-3.5 bg-brand-600 hover:bg-brand-500 text-white font-bold rounded-lg shadow-lg shadow-brand-500/20 transition-all transform hover:scale-[1.01]">
                Connect Database
            </button>
            
            <div class="grid grid-cols-4 gap-2 text-[10px] font-mono text-gray-600 uppercase text-center mt-8">
                <span class="bg-gray-800/50 py-1 rounded">Postgres</span>
                <span class="bg-gray-800/50 py-1 rounded">Mongo</span>
                <span class="bg-gray-800/50 py-1 rounded">MySQL</span>
                <span class="bg-gray-800/50 py-1 rounded">Redis</span>
            </div>
        </form>
    </div>
</div>
{% endblock %}
"""

TABLES_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col">
    <div class="border-b border-gray-800 p-6 flex justify-between items-center bg-gray-900">
        <div>
            <div class="text-xs font-bold text-gray-500 uppercase tracking-widest mb-1">Database Scope</div>
            <h2 class="text-2xl font-bold text-white">{{ db_name }}</h2>
        </div>
        <form action="{{ url_for('drop_database_route', db_name=db_name) }}" method="POST" onsubmit="return confirm('CRITICAL: Irreversible deletion of database {{ db_name }}. Confirm?');">
            <button class="text-xs font-bold text-red-500 hover:text-red-400 hover:bg-red-500/10 px-4 py-2 rounded-md transition-colors">
                DELETE DATABASE
            </button>
        </form>
    </div>

    <div class="flex-grow overflow-y-auto p-6 scrollbar-thin">
        {% if tables %}
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {% for table in tables %}
            <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="group bg-gray-800/50 border border-gray-800 hover:border-brand-500/50 hover:bg-gray-800 p-5 rounded-xl transition-all relative overflow-hidden">
                <div class="absolute top-0 right-0 p-4 opacity-0 group-hover:opacity-100 transition-opacity text-brand-500">
                    <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14 5l7 7m0 0l-7 7m7-7H3" /></svg>
                </div>
                <div class="flex items-center gap-3 mb-3">
                    <div class="p-2 bg-gray-900 rounded-lg text-gray-400 group-hover:text-brand-500 transition-colors">
                        <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>
                    </div>
                </div>
                <h3 class="font-bold text-gray-200 truncate pr-6">{{ table }}</h3>
                <p class="text-xs text-gray-500 font-mono mt-1">Table / Collection</p>
            </a>
            {% endfor %}
        </div>
        {% else %}
        <div class="h-64 flex flex-col items-center justify-center border-2 border-dashed border-gray-800 rounded-xl">
            <p class="text-gray-600 font-medium">No Tables Found</p>
        </div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

ROWS_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col bg-gray-950" x-data="{ rawMode: false, currentRaw: '' }">
    <div class="border-b border-gray-800 p-4 bg-gray-900 flex flex-col md:flex-row gap-4 justify-between items-center shrink-0">
        <div class="flex items-center gap-3 w-full md:w-auto overflow-hidden">
            <a href="{{ url_for('list_tables', db_name=db_name) }}" class="text-gray-500 hover:text-white transition-colors">
                <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
            </a>
            <h2 class="font-bold text-lg text-white truncate"><span class="text-gray-500">{{ db_name }} /</span> {{ table }}</h2>
        </div>
        
        <div class="flex gap-2 w-full md:w-auto">
            <form method="GET" class="flex w-full md:w-96 relative">
                <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <svg class="h-4 w-4 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" /></svg>
                </div>
                <input type="text" name="q" value="{{ request.args.get('q', '') }}" placeholder="Deep Search (ID or Content)..." 
                       class="w-full bg-gray-950 border border-gray-700 text-gray-200 pl-10 pr-4 py-2 rounded-l-md focus:ring-1 focus:ring-brand-500 focus:border-brand-500 outline-none text-sm font-mono">
                <button type="submit" class="px-4 bg-gray-800 border border-l-0 border-gray-700 rounded-r-md hover:bg-gray-700 text-gray-300 text-xs font-bold uppercase">Search</button>
            </form>
            
            <button @click="rawMode = !rawMode" class="px-3 py-2 bg-gray-800 border border-gray-700 rounded-md text-gray-300 hover:text-white text-xs font-bold uppercase transition-colors" x-text="rawMode ? 'View Table' : 'View Raw'"></button>
            
            <a href="{{ url_for('edit_row', db_name=db_name, table=table, id='new') }}" class="px-4 py-2 bg-brand-600 hover:bg-brand-500 text-white rounded-md text-sm font-bold shadow-lg shadow-brand-500/20 transition-colors flex items-center gap-1">
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" /></svg> New
            </a>
        </div>
    </div>

    <div class="flex-grow overflow-auto relative bg-gray-950 scrollbar-thin">
        
        <div x-show="!rawMode" class="min-w-full inline-block align-middle">
            <table class="min-w-full divide-y divide-gray-800">
                <thead class="bg-gray-900 sticky top-0 z-10">
                    <tr>
                        <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-24">Action</th>
                        <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-48 cursor-pointer hover:text-white" onclick="updateSort('id')">
                            ID {{ '↓' if sort_col == 'id' and sort_dir == 'desc' else '↑' if sort_col == 'id' else '' }}
                        </th>
                        <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Document Data</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-gray-800 bg-gray-950">
                    {% for row in rows %}
                    <tr class="hover:bg-gray-900/50 group transition-colors">
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                            <div class="flex items-center gap-3 opacity-60 group-hover:opacity-100 transition-opacity">
                                <a href="{{ url_for('edit_row', db_name=db_name, table=table, id=row['__id']) }}" class="text-brand-500 hover:text-brand-400">
                                    <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" /></svg>
                                </a>
                                <form method="POST" action="{{ url_for('delete_row', db_name=db_name, table=table, id=row['__id']) }}" onsubmit="return confirm('Delete row?');">
                                    <button class="text-red-500 hover:text-red-400">
                                        <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                                    </button>
                                </form>
                                <button @click="copyToClipboard('{{ row['__raw'] | replace("'", "\\'") }}')" class="text-gray-500 hover:text-white" title="Copy JSON">
                                    <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>
                                </button>
                            </div>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-400">
                            {{ row['__id'] | highlight(request.args.get('q')) }}
                        </td>
                        <td class="px-6 py-4 text-sm text-gray-400 font-mono break-all leading-relaxed">
                            <div class="max-h-20 overflow-hidden relative group-hover:max-h-full transition-all duration-500">
                                {{ row | to_json | highlight(request.args.get('q')) }}
                            </div>
                        </td>
                    </tr>
                    {% else %}
                    <tr><td colspan="3" class="px-6 py-12 text-center text-gray-500 font-medium">No Records Found matching your criteria.</td></tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div x-show="rawMode" class="p-6 space-y-4">
            {% for row in rows %}
            <div class="bg-gray-900 border border-gray-800 rounded-lg p-4 font-mono text-xs text-gray-300 overflow-x-auto relative group">
                <button @click="copyToClipboard('{{ row['__raw'] | replace("'", "\\'") }}')" class="absolute top-2 right-2 p-2 bg-gray-800 rounded text-gray-400 hover:text-white opacity-0 group-hover:opacity-100 transition-opacity">Copy</button>
                <pre>{{ row['__raw'] }}</pre>
            </div>
            {% endfor %}
        </div>

    </div>

    <div class="p-4 border-t border-gray-800 bg-gray-900 flex justify-between items-center text-sm shrink-0">
        <div class="text-gray-500">Showing <span class="font-bold text-white">{{ rows|length }}</span> of <span class="font-bold text-white">{{ total }}</span></div>
        <div class="flex gap-2">
            {% if page > 1 %}
            <a href="?page={{ page - 1 }}&q={{ request.args.get('q','') }}&sort={{ sort_col }}&dir={{ sort_dir }}" class="px-3 py-1 bg-gray-800 rounded border border-gray-700 hover:border-gray-500 text-gray-300">Previous</a>
            {% endif %}
            <span class="px-3 py-1 text-gray-500 font-mono">Page {{ page }}</span>
            {% if total > page * 50 %}
            <a href="?page={{ page + 1 }}&q={{ request.args.get('q','') }}&sort={{ sort_col }}&dir={{ sort_dir }}" class="px-3 py-1 bg-gray-800 rounded border border-gray-700 hover:border-gray-500 text-gray-300">Next</a>
            {% endif %}
        </div>
    </div>
</div>

<script>
function updateSort(col) {
    const url = new URL(window.location);
    url.searchParams.set('sort', col);
    url.searchParams.set('dir', url.searchParams.get('dir') === 'asc' ? 'desc' : 'asc');
    window.location = url;
}
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        // Simple toast could go here, but for now we trust the action
        alert("Copied to clipboard!"); 
    });
}
</script>
{% endblock %}
"""

EDITOR_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col bg-gray-950">
    <div class="p-4 border-b border-gray-800 bg-gray-900 flex justify-between items-center shrink-0">
        <div>
            <div class="text-xs font-bold text-brand-500 uppercase tracking-wide">JSON Editor</div>
            <h2 class="font-bold text-xl text-white">{{ 'Create Record' if id == 'new' else 'Edit Record' }}</h2>
        </div>
        <div class="flex gap-3">
            <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="px-4 py-2 text-sm font-bold text-gray-400 hover:text-white transition-colors">Cancel</a>
            <button onclick="document.getElementById('saveForm').submit()" class="px-6 py-2 bg-brand-600 hover:bg-brand-500 text-white rounded-md text-sm font-bold shadow-lg shadow-brand-500/20 transition-all">
                Save Changes
            </button>
        </div>
    </div>

    <div class="flex-grow relative">
        <form id="saveForm" method="POST" class="h-full">
            <textarea id="json-editor" name="json_data">{{ data }}</textarea>
        </form>
    </div>
</div>

<script>
    var editor = CodeMirror.fromTextArea(document.getElementById("json-editor"), {
        lineNumbers: true,
        mode: "application/json",
        theme: "material-darker",
        matchBrackets: true,
        autoCloseBrackets: true,
        lint: true,
        smartIndent: true,
        tabSize: 2,
        extraKeys: {"Ctrl-S": function(cm){ document.getElementById('saveForm').submit(); }}
    });
</script>
{% endblock %}
"""

template_dict = {
    'base.html': BASE_LAYOUT,
    'login.html': LOGIN_TEMPLATE,
    'tables.html': TABLES_TEMPLATE,
    'rows.html': ROWS_TEMPLATE,
    'editor.html': EDITOR_TEMPLATE
}
app.jinja_loader = DictLoader(template_dict)

@app.template_filter('to_json')
def to_json_filter_global(value): return json_util.dumps(value, indent=None)

# ==========================================
# ROUTES
# ==========================================
@app.context_processor
def inject_dbs():
    if session.get('db_uri'):
        try:
            adp = get_adapter()
            if adp: return {'dbs': adp.list_databases()}
        except: pass
    return {'dbs': []}

@app.route('/')
def index():
    if session.get('db_uri'): 
        return redirect(url_for('list_tables', db_name=session.get('current_db_name', 'postgres')))
    return render_template('login.html')

@app.route('/connect', methods=['POST'])
def connect_db():
    uri = request.form.get('db_uri', '').strip()
    session['db_uri'] = uri
    adp = get_adapter()
    if adp:
        dbs = adp.list_databases()
        default_db = dbs[0] if dbs else 'default'
        return redirect(url_for('list_tables', db_name=default_db))
    session.pop('db_uri', None)
    flash('Connection Failed. Check your URL.', 'error')
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/compass/<db_name>')
def list_tables(db_name):
    session['current_db_name'] = db_name
    adp = get_adapter(db_name)
    if not adp: return redirect(url_for('logout'))
    try:
        tables = adp.list_tables(db_name)
        return render_template('tables.html', db_name=db_name, tables=tables)
    except Exception as e:
        flash(str(e), 'error')
        return redirect(url_for('index'))

@app.route('/compass/<db_name>/delete', methods=['POST'])
def drop_database_route(db_name):
    adp = get_adapter()
    try:
        adp.drop_database(db_name)
        flash(f"Database {db_name} deleted.", 'success')
        return redirect(url_for('index'))
    except Exception as e:
        flash(f"Delete failed: {str(e)}", 'error')
        return redirect(url_for('list_tables', db_name=db_name))

@app.route('/compass/<db_name>/<table>')
def view_rows(db_name, table):
    adp = get_adapter(db_name)
    page = int(request.args.get('page', 1))
    search = request.args.get('q', None)
    sort_col = request.args.get('sort', None)
    sort_dir = request.args.get('dir', 'desc')
    try:
        rows, total = adp.get_rows(db_name, table, page, search, sort_col, sort_dir)
        return render_template('rows.html', db_name=db_name, table=table, rows=rows, total=total, page=page, sort_col=sort_col, sort_dir=sort_dir)
    except Exception as e:
        flash(str(e), 'error')
        return redirect(url_for('list_tables', db_name=db_name))

@app.route('/compass/<db_name>/<table>/<id>/edit', methods=['GET', 'POST'])
def edit_row(db_name, table, id):
    adp = get_adapter(db_name)
    if request.method == 'POST':
        try:
            data = json.loads(request.form['json_data'])
            adp.save_row(db_name, table, id, data, is_new=(id=='new'))
            flash('Record Saved', 'success')
            return redirect(url_for('view_rows', db_name=db_name, table=table))
        except Exception as e:
            flash(f"Save Error: {str(e)}", 'error')

    data_str = "{\n\n}"
    if id != 'new':
        row = adp.get_row(db_name, table, id)
        if row: data_str = json_util.dumps(row, indent=2)
    return render_template('editor.html', db_name=db_name, table=table, id=id, data=data_str)

@app.route('/compass/<db_name>/<table>/<id>/delete', methods=['POST'])
def delete_row(db_name, table, id):
    adp = get_adapter(db_name)
    try:
        adp.delete_row(db_name, table, id)
        flash('Record Deleted', 'success')
    except Exception as e:
        flash(str(e), 'error')
    return redirect(url_for('view_rows', db_name=db_name, table=table))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
