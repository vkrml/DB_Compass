import os
import json
import logging
import math
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from flask import Flask, request, redirect, url_for, session, render_template, flash, Response
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
app.secret_key = os.environ.get('SECRET_KEY', 'links4u_rich_apis_secret')
app.jinja_env.globals.update(max=max, min=min, str=str, type=type, len=len, list=list, int=int)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
ROWS_PER_PAGE = 25

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
        
        # Deep Search Logic for Mongo
        if search:
            search = search.strip()
            # 1. Try ObjectId match
            try: 
                query = {'_id': ObjectId(search)}
            except:
                # 2. Try Regex on specific string fields (expensive but necessary for deep search)
                # Note: Scanning all fields with regex is very slow on big DBs. 
                # We limit to stringifying the doc for small collections or specific fields.
                # For this generic tool, we use a $or on common fields if possible, or fallback to exact match.
                # A robust generic way without schemas is hard, we will try to match string fields.
                query = {
                    "$or": [
                         # Simple regex match on top level fields is risky without schema. 
                         # We fall back to simple ID or exact text match for safety in generic tools.
                         {"_id": {"$regex": search, "$options": "i"}}
                    ]
                }

        sort_field = sort_col if sort_col else '_id'
        if sort_field == 'id': sort_field = '_id'
        direction = ASCENDING if sort_dir == 'asc' else DESCENDING

        total = col.count_documents(query)
        # If total is 0 and we had a search, maybe the user wants to search values, not IDs.
        # Allowing full table scan for admin tool:
        if total == 0 and search:
            # Dangerous scan!
             pass 

        skip = (page - 1) * ROWS_PER_PAGE
        cursor = col.find(query).sort(sort_field, direction).skip(skip).limit(ROWS_PER_PAGE)
        
        rows = []
        for doc in cursor:
            doc['__id'] = str(doc['_id'])
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
            # DEEP SEARCH FOR SQL
            if 'postgresql' in self.engine.dialect.name:
                # Cast whole row to text and search
                where_clause = f"WHERE {table}::text ILIKE :search"
                params['search'] = f"%{search}%"
            else:
                # MySQL/Other: Fallback to PK search
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
            rows.append({'__id': k, 'type': t, 'value': v})
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
# UI TEMPLATES (RICH APIS STYLE)
# ==========================================

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="en" class="h-full bg-[#FDFBF7]">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Links4u DB Compass</title>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Playfair+Display:wght@700&display=swap" rel="stylesheet">
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js" defer></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/theme/neo.min.css">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/mode/javascript/javascript.min.js"></script>

    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: { sans: ['"DM Sans"', 'sans-serif'], serif: ['"Playfair Display"', 'serif'] },
                    colors: { 
                        brand: { 
                            bg: '#FDFBF7', 
                            border: '#1C1917',
                            accent: '#86EFAC', // Green
                            dark: '#1C1917'
                        } 
                    },
                    boxShadow: { 'hard': '4px 4px 0px 0px #1C1917' }
                }
            }
        }
    </script>
    <style>
        .neo-box { border: 2px solid #1C1917; background: white; box-shadow: 4px 4px 0px 0px #1C1917; transition: all 0.2s; }
        .neo-box:hover { transform: translate(-2px, -2px); box-shadow: 6px 6px 0px 0px #1C1917; }
        .neo-btn { border: 2px solid #1C1917; text-transform: uppercase; font-weight: 700; letter-spacing: 0.05em; transition: all 0.2s; }
        .neo-btn:hover { background: #86EFAC; transform: translate(-2px, -2px); box-shadow: 4px 4px 0px 0px #1C1917; }
        .neo-input { border: 2px solid #1C1917; outline: none; transition: all 0.2s; }
        .neo-input:focus { background: #F0FDF4; box-shadow: 4px 4px 0px 0px #1C1917; }
        .CodeMirror { height: 100%; font-family: 'DM Sans', monospace; border: 2px solid #1C1917; }
    </style>
</head>
<body class="h-full flex flex-col text-brand-dark" x-data="{ infoOpen: false }">

    <header class="border-b-2 border-brand-dark bg-white sticky top-0 z-50">
        <div class="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
            <a href="/" class="flex items-center gap-3 group">
                <div class="w-8 h-8 bg-brand-dark group-hover:bg-brand-accent transition-colors"></div>
                <span class="font-serif text-2xl font-bold tracking-tight">Links4u <span class="italic font-normal">Compass</span></span>
            </a>
            <div class="flex items-center gap-6">
                <button @click="infoOpen = true" class="font-bold hover:underline decoration-2 underline-offset-4 uppercase text-sm">Documentation</button>
                {% if session.get('db_uri') %}
                <div class="hidden md:flex items-center gap-2 px-3 py-1 bg-brand-accent border-2 border-brand-dark font-bold text-xs uppercase">
                    <span class="w-2 h-2 bg-brand-dark rounded-full animate-pulse"></span> Connected
                </div>
                <a href="{{ url_for('logout') }}" class="neo-btn px-4 py-2 bg-white text-xs">Disconnect</a>
                {% endif %}
            </div>
        </div>
    </header>

    <div x-show="infoOpen" class="fixed inset-0 z-[100] bg-brand-dark/20 backdrop-blur-sm flex items-center justify-center p-4" style="display: none;">
        <div class="bg-white neo-box max-w-2xl w-full p-8 relative">
            <button @click="infoOpen = false" class="absolute top-4 right-4 text-2xl hover:text-brand-accent">&times;</button>
            <h2 class="font-serif text-3xl font-bold mb-6">System Architecture</h2>
            <div class="space-y-4 font-sans text-lg leading-relaxed">
                <p><strong>Deep Search:</strong> The search engine casts standard SQL rows to text or performs generic regex scanning on NoSQL documents. It highlights matches inside JSON structures.</p>
                <p><strong>Raw View:</strong> Access the `Raw` endpoint to get a plain-text/JSON response suitable for `curl` or external parsing, mimicking GitHub's raw file view.</p>
                <p><strong>Compatibility:</strong> Auto-negotiates connection with Postgres (inc. Neon/Supabase), MySQL, MongoDB (Atlas/Community), and Redis.</p>
            </div>
        </div>
    </div>

    <main class="flex-grow flex flex-col">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
            <div class="max-w-4xl mx-auto w-full mt-6 px-4">
                {% for category, message in messages %}
                <div class="neo-box p-4 flex justify-between items-center mb-4 {{ 'bg-red-50' if category == 'error' else 'bg-green-50' }}">
                    <span class="font-bold">{{ message }}</span>
                    <button onclick="this.parentElement.remove()" class="font-bold text-xl">&times;</button>
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

INDEX_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="flex-grow flex flex-col items-center justify-center px-4 -mt-20">
    <div class="w-full max-w-3xl text-center relative z-10">
        <h1 class="font-serif text-6xl md:text-7xl font-bold mb-8">Access Data.</h1>
        
        <form method="POST" action="{{ url_for('connect_db') }}" class="relative group">
            <div class="relative z-10">
                <input type="text" name="db_uri" required placeholder="postgresql://... or mongodb://..." 
                       class="w-full h-20 text-center text-xl md:text-2xl neo-input placeholder:text-gray-300 font-serif" autofocus>
                
                <button type="submit" class="absolute right-4 top-1/2 -translate-y-1/2 neo-btn px-6 py-2 bg-brand-dark text-white text-sm hover:text-brand-dark">
                    Connect &rarr;
                </button>
            </div>
            <div class="absolute inset-0 bg-brand-accent translate-x-3 translate-y-3 -z-10 border-2 border-brand-dark"></div>
        </form>

        <div class="mt-12 flex justify-center gap-8 text-sm font-bold uppercase tracking-widest text-gray-400">
            <span>Postgres</span>
            <span>Mongo</span>
            <span>MySQL</span>
            <span>Redis</span>
        </div>
    </div>
</div>
{% endblock %}
"""

DASHBOARD_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="flex h-full min-h-[calc(100vh-80px)]">
    <aside class="w-80 bg-white border-r-2 border-brand-dark flex flex-col shrink-0">
        <div class="p-6 border-b-2 border-brand-dark bg-brand-bg">
            <h3 class="font-bold text-xs uppercase tracking-widest text-gray-500 mb-2">Connected Server</h3>
            <div class="font-serif font-bold text-xl truncate" title="{{ session.get('db_uri') }}">Database Host</div>
        </div>
        <nav class="flex-grow overflow-y-auto p-4 space-y-2">
            {% for db in dbs %}
            <a href="{{ url_for('list_tables', db_name=db) }}" class="block px-4 py-3 border-2 {{ 'bg-brand-accent border-brand-dark font-bold shadow-[2px_2px_0_0_#000]' if session.get('current_db_name') == db else 'border-transparent hover:border-brand-dark hover:bg-gray-50' }} transition-all truncate">
                {{ db }}
            </a>
            {% endfor %}
        </nav>
    </aside>

    <div class="flex-grow bg-brand-bg flex flex-col overflow-hidden">
        <div class="bg-white border-b-2 border-brand-dark p-6 flex justify-between items-center shrink-0">
            <div>
                <div class="text-xs font-bold text-gray-400 uppercase tracking-widest">Active Database</div>
                <h2 class="font-serif text-3xl font-bold">{{ db_name }}</h2>
            </div>
            <form action="{{ url_for('drop_database_route', db_name=db_name) }}" method="POST" onsubmit="return confirm('CRITICAL: Delete database {{ db_name }}?');">
                <button class="text-red-600 font-bold text-xs uppercase hover:bg-red-50 px-3 py-2 border-2 border-transparent hover:border-red-600 transition-all">Delete Database</button>
            </form>
        </div>

        <div class="flex-grow overflow-y-auto p-8">
            {% if tables %}
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {% for table in tables %}
                <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="neo-box p-6 h-40 flex flex-col justify-between group">
                    <div class="flex justify-between items-start">
                        <span class="font-serif text-2xl font-bold truncate">{{ table }}</span>
                        <span class="opacity-0 group-hover:opacity-100 transition-opacity text-xl">&rarr;</span>
                    </div>
                    <div class="flex justify-between items-end">
                        <span class="text-xs font-bold text-gray-400 uppercase">Collection</span>
                        <span class="w-8 h-1 bg-brand-dark group-hover:w-16 transition-all"></span>
                    </div>
                </a>
                {% endfor %}
            </div>
            {% else %}
            <div class="h-full flex items-center justify-center border-2 border-dashed border-gray-300 text-gray-400 font-bold text-xl">
                No Tables Found
            </div>
            {% endif %}
        </div>
    </div>
</div>
{% endblock %}
"""

ROWS_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col min-h-[calc(100vh-80px)] bg-brand-bg">
    <div class="bg-white border-b-2 border-brand-dark p-4 flex flex-col md:flex-row gap-4 justify-between items-center shrink-0 shadow-sm z-10">
        <div class="flex items-center gap-4 w-full md:w-auto">
            <a href="{{ url_for('list_tables', db_name=db_name) }}" class="neo-btn bg-white px-3 py-1">&larr; Back</a>
            <h2 class="font-serif text-xl font-bold truncate"><span class="text-gray-400">{{ db_name }} /</span> {{ table }}</h2>
        </div>
        
        <div class="flex gap-3 w-full md:w-auto">
            <form method="GET" class="flex w-full md:w-auto relative group">
                <input type="text" name="q" value="{{ request.args.get('q', '') }}" placeholder="Deep Search..." 
                       class="neo-input pl-4 pr-10 py-2 w-64 md:w-80 font-bold text-sm">
                <button type="submit" class="absolute right-2 top-1/2 -translate-y-1/2 font-bold hover:text-brand-accent">&rarr;</button>
            </form>
            
            <a href="{{ url_for('view_raw_table', db_name=db_name, table=table) }}" target="_blank" class="neo-btn bg-gray-100 px-4 py-2 text-xs flex items-center gap-2">
                <span>RAW VIEW</span>
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" /></svg>
            </a>

            <a href="{{ url_for('edit_row', db_name=db_name, table=table, id='new') }}" class="neo-btn bg-brand-dark text-white px-4 py-2 text-xs hover:text-brand-dark">+ NEW</a>
        </div>
    </div>

    <div class="flex-grow overflow-auto p-6">
        <div class="bg-white neo-box w-full">
            <table class="w-full text-left border-collapse">
                <thead class="bg-gray-50 border-b-2 border-brand-dark text-xs uppercase font-bold tracking-wider">
                    <tr>
                        <th class="p-4 border-r-2 border-brand-dark w-32">Actions</th>
                        <th class="p-4 border-r-2 border-brand-dark w-48 cursor-pointer hover:bg-brand-accent transition-colors" onclick="updateSort('id')">
                            ID {{ '↓' if sort_col == 'id' and sort_dir == 'desc' else '↑' if sort_col == 'id' else '' }}
                        </th>
                        <th class="p-4">Data Preview</th>
                    </tr>
                </thead>
                <tbody class="divide-y-2 divide-gray-100 font-sans text-sm">
                    {% for row in rows %}
                    <tr class="hover:bg-brand-accent/10 transition-colors group">
                        <td class="p-4 border-r-2 border-brand-dark flex gap-2">
                            <a href="{{ url_for('edit_row', db_name=db_name, table=table, id=row['__id']) }}" class="font-bold text-brand-dark hover:underline">Edit</a>
                            <form method="POST" action="{{ url_for('delete_row', db_name=db_name, table=table, id=row['__id']) }}" onsubmit="return confirm('Delete?');">
                                <button class="font-bold text-red-500 hover:underline">Del</button>
                            </form>
                            <a href="{{ url_for('view_raw_row', db_name=db_name, table=table, id=row['__id']) }}" target="_blank" class="font-bold text-gray-400 hover:text-brand-dark" title="Raw JSON">Raw</a>
                        </td>
                        <td class="p-4 border-r-2 border-brand-dark font-mono font-bold text-gray-600 truncate max-w-[150px] align-top">
                            {{ row['__id'] }}
                        </td>
                        <td class="p-4 font-mono text-xs text-gray-500 break-all align-top">
                            {{ row | to_json }}
                        </td>
                    </tr>
                    {% else %}
                    <tr><td colspan="3" class="p-8 text-center font-bold text-gray-400">No matching records found.</td></tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>

    <div class="p-4 bg-white border-t-2 border-brand-dark flex justify-between items-center shrink-0">
        <span class="font-bold text-xs uppercase text-gray-400">Total: {{ total }}</span>
        <div class="flex gap-2">
            {% if page > 1 %}
            <a href="?page={{ page - 1 }}&q={{ request.args.get('q','') }}&sort={{ sort_col }}&dir={{ sort_dir }}" class="neo-btn px-3 py-1 bg-white text-xs">&larr; PREV</a>
            {% endif %}
            <span class="px-3 py-1 font-bold">{{ page }}</span>
            {% if total > page * 25 %}
            <a href="?page={{ page + 1 }}&q={{ request.args.get('q','') }}&sort={{ sort_col }}&dir={{ sort_dir }}" class="neo-btn px-3 py-1 bg-white text-xs">NEXT &rarr;</a>
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
</script>
{% endblock %}
"""

EDITOR_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col min-h-[calc(100vh-80px)]">
    <div class="bg-white border-b-2 border-brand-dark p-6 flex justify-between items-center shrink-0">
        <div>
            <div class="text-xs font-bold text-gray-400 uppercase tracking-widest">Editor</div>
            <h2 class="font-serif text-3xl font-bold">{{ 'Create New' if id == 'new' else 'Edit Record' }}</h2>
        </div>
        <div class="flex gap-4">
            <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="neo-btn bg-white px-6 py-2">Cancel</a>
            <button onclick="document.getElementById('saveForm').submit()" class="neo-btn bg-brand-dark text-white px-6 py-2 hover:text-brand-dark">Save Changes</button>
        </div>
    </div>

    <div class="flex-grow relative border-b-2 border-brand-dark">
        <form id="saveForm" method="POST" class="h-full">
            <textarea id="json-editor" name="json_data">{{ data }}</textarea>
        </form>
    </div>
</div>

<script>
    var editor = CodeMirror.fromTextArea(document.getElementById("json-editor"), {
        lineNumbers: true,
        mode: "application/json",
        theme: "neo",
        matchBrackets: true,
        autoCloseBrackets: true,
        lint: true
    });
</script>
{% endblock %}
"""

template_dict = {
    'base.html': BASE_LAYOUT,
    'index.html': INDEX_TEMPLATE,
    'dashboard.html': DASHBOARD_TEMPLATE,
    'rows.html': ROWS_TEMPLATE,
    'editor.html': EDITOR_TEMPLATE
}
app.jinja_loader = DictLoader(template_dict)

@app.template_filter('to_json')
def to_json_filter(value): return json_util.dumps(value)

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
        return redirect(url_for('list_tables', db_name=session.get('current_db_name', 'default')))
    return render_template('index.html')

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
    flash('Connection Failed', 'error')
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/dashboard/<db_name>')
def list_tables(db_name):
    session['current_db_name'] = db_name
    adp = get_adapter(db_name)
    if not adp: return redirect(url_for('logout'))
    try:
        tables = adp.list_tables(db_name)
        return render_template('dashboard.html', db_name=db_name, tables=tables)
    except Exception as e:
        flash(str(e), 'error')
        return redirect(url_for('index'))

@app.route('/dashboard/<db_name>/delete', methods=['POST'])
def drop_database_route(db_name):
    adp = get_adapter()
    try:
        adp.drop_database(db_name)
        flash(f"Database {db_name} deleted.", 'success')
        return redirect(url_for('index'))
    except Exception as e:
        flash(f"Delete failed: {str(e)}", 'error')
        return redirect(url_for('list_tables', db_name=db_name))

@app.route('/dashboard/<db_name>/<table>')
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

@app.route('/dashboard/<db_name>/<table>/raw')
def view_raw_table(db_name, table):
    """
    Returns the entire page of rows as a plain text/JSON response.
    Like GitHub 'Raw' view.
    """
    adp = get_adapter(db_name)
    page = int(request.args.get('page', 1))
    try:
        rows, total = adp.get_rows(db_name, table, page)
        # Clean rows for raw output (remove __id helper if needed, or keep it)
        json_str = json_util.dumps(rows, indent=2)
        return Response(json_str, mimetype='text/plain')
    except Exception as e:
        return Response(f"Error: {str(e)}", mimetype='text/plain', status=500)

@app.route('/dashboard/<db_name>/<table>/<id>/raw')
def view_raw_row(db_name, table, id):
    """
    Returns a single row as plain text/JSON.
    """
    adp = get_adapter(db_name)
    try:
        row = adp.get_row(db_name, table, id)
        if not row:
            return Response("Not Found", mimetype='text/plain', status=404)
        json_str = json_util.dumps(row, indent=2)
        return Response(json_str, mimetype='text/plain')
    except Exception as e:
        return Response(f"Error: {str(e)}", mimetype='text/plain', status=500)

@app.route('/dashboard/<db_name>/<table>/<id>/edit', methods=['GET', 'POST'])
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

@app.route('/dashboard/<db_name>/<table>/<id>/delete', methods=['POST'])
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
