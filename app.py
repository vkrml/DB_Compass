import os
import json
import logging
import math
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from flask import Flask, request, redirect, url_for, session, render_template, flash
from jinja2 import DictLoader
from bson import json_util, ObjectId

# Drivers
from pymongo import MongoClient, ASCENDING, DESCENDING
from sqlalchemy import create_engine, inspect, text
import redis

# ==========================================
# CONFIGURATION
# ==========================================
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'links4u_secret_key_change_me')
app.jinja_env.globals.update(max=max, min=min, str=str, type=type, len=len, list=list, int=int)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
ROWS_PER_PAGE = 25

# ==========================================
# ADVANCED DATABASE ADAPTERS
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
        
        # Search Logic (Try to parse JSON, else search ID)
        if search:
            search = search.strip()
            if search.startswith('{'):
                try: query = json.loads(search)
                except: pass
            else:
                try: query = {'_id': ObjectId(search)}
                except: query = {'_id': search}

        # Sort Logic
        sort_field = sort_col if sort_col else '_id'
        if sort_field == 'id': sort_field = '_id'
        direction = ASCENDING if sort_dir == 'asc' else DESCENDING

        total = col.count_documents(query)
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
        if is_new:
            col.insert_one(data)
        else:
            try: oid = ObjectId(id)
            except: oid = id
            if '_id' in data: del data['_id']
            if '__id' in data: del data['__id']
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

        self.base_uri = uri
        
        if db_name:
            u = urlparse(uri)
            new_path = f"/{db_name}"
            uri = urlunparse(u._replace(path=new_path))

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
                else:
                    return [urlparse(str(self.engine.url)).database or 'main']
        except:
            return [urlparse(str(self.engine.url)).database or 'default']

    def drop_database(self, db_name):
        # Dangerous and difficult in SQL without dropping connections. 
        # For safety/complexity, strictly implemented for specific drivers or skipped.
        # Implemented for Postgres/MySQL via raw SQL, requires appropriate user permissions.
        with self.engine.connect() as conn:
            conn.execute(text("COMMIT")) # Close transaction
            conn.execute(text(f"DROP DATABASE {db_name}"))

    def list_tables(self, db_name):
        return sorted(inspect(self.engine).get_table_names())

    def drop_table(self, db_name, table):
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE {table}"))

    def get_pk(self, table):
        insp = inspect(self.engine)
        pk = insp.get_pk_constraint(table)
        return pk['constrained_columns'][0] if pk['constrained_columns'] else 'id'

    def get_rows(self, db_name, table, page, search=None, sort_col=None, sort_dir='desc'):
        pk = self.get_pk(table)
        sort_field = sort_col if sort_col else pk
        
        # Validation to prevent injection on Sort Field
        insp = inspect(self.engine)
        cols = [c['name'] for c in insp.get_columns(table)]
        if sort_field not in cols: sort_field = pk

        offset = (page - 1) * ROWS_PER_PAGE
        
        where_clause = ""
        params = {}
        
        if search:
            # Simple Search: Try to match PK or assume generic text search on PK
            where_clause = f"WHERE {pk}::text LIKE :search" if 'postgres' in self.engine.dialect.name else f"WHERE {pk} LIKE :search"
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
                db_int = int(db_name.replace("DB", "").strip())
                pool_args = self.r.connection_pool.connection_kwargs.copy()
                pool_args['db'] = db_int
                self.r = redis.Redis(**pool_args, decode_responses=True)
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
# LINKS4U DB COMPASS UI TEMPLATES
# ==========================================

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="en" class="h-full bg-stone-50">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Links4u DB Compass</title>
    <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js" defer></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/theme/nord.min.css">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/mode/javascript/javascript.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/mode/sql/sql.min.js"></script>
    
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: { sans: ['"Space Grotesk"', 'sans-serif'], mono: ['"JetBrains Mono"', 'monospace'] },
                    colors: { 
                        brand: { 
                            bg: '#F5F5F4', 
                            surface: '#FFFFFF',
                            primary: '#A3E635', // Lime 400
                            dark: '#1C1917', // Stone 900
                            danger: '#EF4444' 
                        } 
                    },
                    boxShadow: { 'neo': '4px 4px 0px 0px #1C1917' }
                }
            }
        }
    </script>
    <style>
        .neo-border { border: 2px solid #1C1917; }
        .neo-shadow { box-shadow: 4px 4px 0px 0px #1C1917; }
        .neo-shadow-hover:hover { box-shadow: 6px 6px 0px 0px #1C1917; transform: translate(-2px, -2px); }
        .neo-shadow-active:active { box-shadow: 2px 2px 0px 0px #1C1917; transform: translate(2px, 2px); }
        .scrollbar-hide::-webkit-scrollbar { display: none; }
        .CodeMirror { height: 100%; font-family: 'JetBrains Mono'; font-size: 14px; }
    </style>
</head>
<body class="h-full flex flex-col md:flex-row overflow-hidden text-stone-900" x-data="{ sidebarOpen: false }">

    <header class="md:hidden flex items-center justify-between p-4 border-b-2 border-brand-dark bg-white z-20">
        <div class="flex items-center gap-2">
            <div class="w-6 h-6 bg-brand-primary border-2 border-brand-dark"></div>
            <span class="font-bold text-lg tracking-tight">Links4u <span class="text-stone-500">DB</span></span>
        </div>
        <button @click="sidebarOpen = !sidebarOpen" class="p-2 neo-border bg-brand-primary">
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"></path></svg>
        </button>
    </header>

    <aside :class="sidebarOpen ? 'translate-x-0' : '-translate-x-full'" class="fixed inset-y-0 left-0 z-30 w-72 bg-white border-r-2 border-brand-dark transition-transform duration-300 md:relative md:translate-x-0 flex flex-col">
        <div class="p-6 border-b-2 border-brand-dark hidden md:flex items-center gap-3">
            <div class="w-8 h-8 bg-brand-primary border-2 border-brand-dark shadow-[2px_2px_0_0_#000]"></div>
            <h1 class="font-bold text-xl">Links4u DB</h1>
        </div>

        {% if session.get('db_uri') %}
        <div class="p-4 bg-stone-100 border-b-2 border-brand-dark">
            <div class="text-xs font-bold uppercase text-stone-500 mb-1">Status</div>
            <div class="flex items-center gap-2">
                <span class="w-3 h-3 rounded-full bg-green-500 border border-black"></span>
                <span class="font-mono text-sm truncate w-full" title="{{ session.get('db_uri') }}">Connected</span>
            </div>
            <a href="{{ url_for('logout') }}" class="mt-3 block text-center text-xs font-bold text-red-600 hover:underline">DISCONNECT SERVER</a>
        </div>
        {% endif %}

        <nav class="flex-grow overflow-y-auto p-4 space-y-6">
            {% if session.get('db_uri') %}
                {% if dbs %}
                <div>
                    <h3 class="font-bold text-xs uppercase tracking-wider text-stone-400 mb-3">Databases</h3>
                    <ul class="space-y-2">
                        {% for db in dbs %}
                        <li>
                            <a href="{{ url_for('list_tables', db_name=db) }}" class="flex items-center gap-2 px-3 py-2 border-2 border-transparent hover:border-brand-dark hover:bg-brand-primary/20 rounded transition-all {{ 'bg-brand-primary border-brand-dark font-bold' if session.get('current_db_name') == db else 'text-stone-600' }}">
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"></path></svg>
                                <span class="truncate">{{ db }}</span>
                            </a>
                        </li>
                        {% endfor %}
                    </ul>
                </div>
                {% endif %}
            {% else %}
                <div class="text-center p-4 text-stone-500 text-sm">No connection active.</div>
            {% endif %}
        </nav>
    </aside>

    <div x-show="sidebarOpen" @click="sidebarOpen = false" class="fixed inset-0 bg-black/50 z-20 md:hidden"></div>

    <main class="flex-grow flex flex-col h-full overflow-hidden relative bg-stone-50">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
            <div class="absolute top-4 right-4 z-50 w-full max-w-sm space-y-2">
                {% for category, message in messages %}
                <div class="neo-border neo-shadow p-3 flex justify-between items-center {{ 'bg-red-100 text-red-800' if category == 'error' else 'bg-green-100 text-green-800' }}">
                    <span class="font-bold text-sm">{{ message }}</span>
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

LOGIN_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex items-center justify-center p-4 bg-stone-50">
    <div class="w-full max-w-lg bg-white neo-border neo-shadow p-8">
        <h1 class="text-4xl font-bold mb-2">Connect Database</h1>
        <p class="text-stone-500 mb-8">Links4u DB Compass Gateway</p>
        
        <form method="POST" action="{{ url_for('connect_db') }}" class="space-y-6">
            <div>
                <label class="block font-bold text-sm mb-2 uppercase">Connection String (URI)</label>
                <input type="text" name="db_uri" placeholder="postgresql://user:pass@host/db" required
                       class="w-full p-4 neo-border bg-stone-50 focus:bg-white focus:outline-none focus:ring-2 focus:ring-brand-primary font-mono text-sm">
            </div>
            
            <div class="flex gap-4 text-xs font-mono text-stone-400">
                <span>MONGODB</span> &bull; <span>POSTGRES</span> &bull; <span>MYSQL</span> &bull; <span>REDIS</span>
            </div>

            <button type="submit" class="w-full py-4 bg-brand-dark text-white font-bold tracking-widest hover:bg-brand-primary hover:text-brand-dark neo-border transition-colors">
                INITIALIZE CONNECTION &rarr;
            </button>
        </form>
    </div>
</div>
{% endblock %}
"""

TABLES_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col p-6 overflow-hidden">
    <div class="flex justify-between items-end mb-6 border-b-2 border-stone-200 pb-4 shrink-0">
        <div>
            <div class="text-xs font-bold text-stone-400 uppercase tracking-widest mb-1">Database</div>
            <h2 class="text-3xl font-bold">{{ db_name }}</h2>
        </div>
        <form action="{{ url_for('drop_database_route', db_name=db_name) }}" method="POST" onsubmit="return confirm('CRITICAL WARNING: This will permanently DELETE the entire database {{ db_name }}. Are you sure?');">
            <button class="text-red-500 font-bold text-xs hover:bg-red-50 px-3 py-1 neo-border border-transparent hover:border-red-500">DELETE DATABASE</button>
        </form>
    </div>

    <div class="flex-grow overflow-y-auto pr-2">
        {% if tables %}
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
            {% for table in tables %}
            <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="block bg-white neo-border p-6 hover:bg-brand-primary/10 transition-transform hover:-translate-y-1 hover:shadow-[4px_4px_0_0_#000] group relative">
                <div class="flex justify-between items-start">
                    <div class="w-10 h-10 bg-brand-primary neo-border flex items-center justify-center mb-4 group-hover:bg-white transition-colors">
                        <span class="font-bold text-lg">{{ table[0] | upper }}</span>
                    </div>
                    <span class="opacity-0 group-hover:opacity-100 transition-opacity text-xl">&rarr;</span>
                </div>
                <h3 class="font-bold text-lg truncate">{{ table }}</h3>
                <p class="text-xs text-stone-400 font-mono mt-1">Collection / Table</p>
            </a>
            {% endfor %}
        </div>
        {% else %}
        <div class="h-64 flex flex-col items-center justify-center neo-border border-dashed bg-stone-100">
            <p class="text-stone-400 font-bold text-lg">No Tables Found</p>
        </div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

ROWS_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col bg-white">
    <div class="border-b-2 border-brand-dark p-4 bg-stone-50 flex flex-col md:flex-row gap-4 justify-between items-center shrink-0">
        <div class="flex items-center gap-4 w-full md:w-auto">
            <h2 class="font-bold text-xl flex items-center gap-2">
                <span class="text-stone-400">{{ db_name }} /</span> {{ table }}
            </h2>
        </div>
        
        <div class="flex gap-2 w-full md:w-auto">
            <form method="GET" class="flex w-full md:w-80">
                <input type="text" name="q" value="{{ request.args.get('q', '') }}" placeholder="Search ID or Value..." class="w-full px-3 py-2 neo-border border-r-0 focus:outline-none font-mono text-sm">
                <button type="submit" class="px-4 bg-brand-dark text-white neo-border hover:bg-brand-primary hover:text-black">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                </button>
            </form>
            
            <a href="{{ url_for('edit_row', db_name=db_name, table=table, id='new') }}" class="px-4 py-2 bg-brand-primary neo-border font-bold hover:bg-brand-primary/80 whitespace-nowrap">+ NEW</a>
            
            <form action="{{ url_for('drop_table_route', db_name=db_name, table=table) }}" method="POST" onsubmit="return confirm('Delete table {{ table }}?');">
                <button class="h-full px-3 bg-red-100 text-red-600 neo-border hover:bg-red-500 hover:text-white" title="Delete Table">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
                </button>
            </form>
        </div>
    </div>

    <div class="flex-grow overflow-auto bg-stone-100 p-4">
        <div class="bg-white neo-border min-w-[800px]">
            <table class="w-full text-left border-collapse">
                <thead class="bg-stone-50 text-xs uppercase font-bold sticky top-0 z-10">
                    <tr>
                        <th class="p-3 border-b-2 border-brand-dark w-32">Controls</th>
                        <th class="p-3 border-b-2 border-brand-dark w-48 hover:bg-stone-200 cursor-pointer" onclick="updateSort('id')">
                            ID {{ '↓' if sort_col == 'id' and sort_dir == 'desc' else '↑' if sort_col == 'id' else '' }}
                        </th>
                        <th class="p-3 border-b-2 border-brand-dark">
                            Document / Row Data
                        </th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-stone-200 font-mono text-sm">
                    {% for row in rows %}
                    <tr class="hover:bg-brand-primary/5 group">
                        <td class="p-3 border-r border-stone-200 bg-white group-hover:bg-transparent">
                            <div class="flex gap-2">
                                <a href="{{ url_for('edit_row', db_name=db_name, table=table, id=row['__id']) }}" class="px-2 py-1 neo-border text-xs font-bold hover:bg-black hover:text-white transition-colors">EDIT</a>
                                <form method="POST" action="{{ url_for('delete_row', db_name=db_name, table=table, id=row['__id']) }}" onsubmit="return confirm('Delete row?');">
                                    <button class="px-2 py-1 neo-border border-red-200 text-red-500 text-xs font-bold hover:bg-red-500 hover:text-white hover:border-red-500 transition-colors">DEL</button>
                                </form>
                            </div>
                        </td>
                        <td class="p-3 border-r border-stone-200 align-top font-bold text-stone-600 truncate max-w-[150px]" title="{{ row['__id'] }}">
                            {{ row['__id'] }}
                        </td>
                        <td class="p-3 align-top text-stone-500 break-all">
                            {{ row | to_json_preview | truncate(300) }}
                        </td>
                    </tr>
                    {% else %}
                    <tr><td colspan="3" class="p-8 text-center text-stone-400 font-bold">No Records Found</td></tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>

    <div class="p-4 border-t-2 border-brand-dark bg-white flex justify-between items-center shrink-0">
        <div class="text-xs font-bold uppercase text-stone-500">Total: {{ total }} records</div>
        <div class="flex gap-2">
            {% if page > 1 %}
            <a href="?page={{ page - 1 }}&q={{ request.args.get('q','') }}&sort={{ sort_col }}&dir={{ sort_dir }}" class="px-3 py-1 neo-border text-sm font-bold hover:bg-stone-100">&larr; PREV</a>
            {% endif %}
            <span class="px-3 py-1 neo-border bg-brand-dark text-white text-sm font-bold">{{ page }}</span>
            {% if total > page * 25 %}
            <a href="?page={{ page + 1 }}&q={{ request.args.get('q','') }}&sort={{ sort_col }}&dir={{ sort_dir }}" class="px-3 py-1 neo-border text-sm font-bold hover:bg-stone-100">NEXT &rarr;</a>
            {% endif %}
        </div>
    </div>
</div>

<script>
function updateSort(col) {
    const currentUrl = new URL(window.location.href);
    const currentDir = currentUrl.searchParams.get('dir');
    const newDir = (currentDir === 'asc') ? 'desc' : 'asc';
    currentUrl.searchParams.set('sort', col);
    currentUrl.searchParams.set('dir', newDir);
    window.location.href = currentUrl.toString();
}
</script>
{% endblock %}
"""

EDITOR_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="h-full flex flex-col">
    <div class="p-4 border-b-2 border-brand-dark bg-white flex justify-between items-center shrink-0">
        <div>
            <div class="text-xs font-bold text-stone-400 uppercase">MODE: JSON EDITOR</div>
            <h2 class="font-bold text-2xl">{{ 'New Record' if id == 'new' else 'Edit Record' }}</h2>
        </div>
        <div class="flex gap-3">
            <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="px-4 py-2 font-bold text-stone-500 hover:text-black">CANCEL</a>
            <button onclick="document.getElementById('saveForm').submit()" class="px-6 py-2 bg-brand-primary neo-border font-bold hover:shadow-[4px_4px_0_0_#000] hover:-translate-y-1 transition-all">
                SAVE CHANGES
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
        theme: "nord",
        matchBrackets: true,
        autoCloseBrackets: true,
        lint: true,
        smartIndent: true,
        tabSize: 2
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

@app.template_filter('to_json_preview')
def to_json_preview(value): return json_util.dumps(value)

# ==========================================
# ROUTING CONTROLLER
# ==========================================

@app.context_processor
def inject_dbs():
    # Inject database list into sidebar for every authenticated request
    if session.get('db_uri'):
        try:
            adp = get_adapter()
            if adp: return {'dbs': adp.list_databases()}
        except: pass
    return {'dbs': []}

@app.route('/')
def index():
    if session.get('db_uri'): 
        # Redirect to first available database if none selected, or dashboard
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
    flash('Connection Failed. Check URL.', 'error')
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

@app.route('/compass/<db_name>/<table>/drop', methods=['POST'])
def drop_table_route(db_name, table):
    adp = get_adapter(db_name)
    try:
        adp.drop_table(db_name, table)
        flash(f"Table {table} deleted.", 'success')
        return redirect(url_for('list_tables', db_name=db_name))
    except Exception as e:
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('view_rows', db_name=db_name, table=table))

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
