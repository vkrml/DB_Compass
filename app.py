import os
import json
import logging
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from flask import Flask, request, redirect, url_for, session, render_template, flash
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
app.secret_key = os.environ.get('SECRET_KEY', 'change_this_secret_key_in_production')
app.jinja_env.globals.update(max=max, min=min, str=str, type=type, len=len, list=list)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

ROWS_PER_PAGE = 50

# ==========================================
# ADAPTERS (The Brains)
# ==========================================
class DatabaseAdapter:
    def connect(self, uri, db_name=None): raise NotImplementedError
    def list_databases(self): raise NotImplementedError
    def list_tables(self, db_name): raise NotImplementedError
    def get_rows(self, db_name, table, page): raise NotImplementedError
    def get_row(self, db_name, table, id): raise NotImplementedError
    def save_row(self, db_name, table, id, data, is_new): raise NotImplementedError
    def delete_row(self, db_name, table, id): raise NotImplementedError

class MongoAdapter(DatabaseAdapter):
    def __init__(self): self.client = None
    
    def connect(self, uri, db_name=None):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.client.server_info() # Test connection
        return True

    def list_databases(self):
        return self.client.list_database_names()

    def list_tables(self, db_name):
        return self.client[db_name].list_collection_names()

    def get_rows(self, db_name, table, page):
        col = self.client[db_name][table]
        skip = (page - 1) * ROWS_PER_PAGE
        total = col.count_documents({})
        cursor = col.find().sort('_id', DESCENDING).skip(skip).limit(ROWS_PER_PAGE)
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
        if is_new:
            self.client[db_name][table].insert_one(data)
        else:
            try: oid = ObjectId(id)
            except: oid = id
            if '_id' in data: del data['_id']
            if '__id' in data: del data['__id']
            self.client[db_name][table].replace_one({'_id': oid}, data)
        return True

    def delete_row(self, db_name, table, id):
        try: oid = ObjectId(id)
        except: oid = id
        self.client[db_name][table].delete_one({'_id': oid})

class SQLAdapter(DatabaseAdapter):
    def __init__(self): 
        self.engine = None
        self.base_uri = ""
    
    def connect(self, uri, db_name=None):
        # Normalize Postgres URI
        if uri.startswith("postgres://"): uri = uri.replace("postgres://", "postgresql://", 1)
        
        # Clean NeonDB/Azure params
        try:
            parsed = urlparse(uri)
            qs = parse_qs(parsed.query)
            if 'channel_binding' in qs: del qs['channel_binding']
            uri = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
        except: pass

        self.base_uri = uri
        
        # If switching DB, modify path
        if db_name:
            u = urlparse(uri)
            # Reconstruct URI with new path (database name)
            # This handles postgresql://user:pass@host/old_db -> .../new_db
            new_path = f"/{db_name}"
            new_uri = u._replace(path=new_path)
            self.engine = create_engine(urlunparse(new_uri))
        else:
            self.engine = create_engine(uri)
            
        with self.engine.connect() as conn: pass # Test
        return True

    def list_databases(self):
        # Query system catalogs to find ALL databases
        dialect = self.engine.dialect.name
        dbs = []
        try:
            with self.engine.connect() as conn:
                if dialect == 'postgresql':
                    res = conn.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false;"))
                    dbs = [row[0] for row in res]
                elif dialect == 'mysql':
                    res = conn.execute(text("SHOW DATABASES;"))
                    dbs = [row[0] for row in res]
                else:
                    # SQLite or others usually only have one DB
                    dbs = [urlparse(str(self.engine.url)).database or 'main']
        except Exception as e:
            # Fallback if permission denied
            dbs = [urlparse(str(self.engine.url)).database or 'default']
        return sorted(dbs)

    def list_tables(self, db_name):
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def get_pk(self, table):
        insp = inspect(self.engine)
        pk = insp.get_pk_constraint(table)
        return pk['constrained_columns'][0] if pk['constrained_columns'] else 'id'

    def get_rows(self, db_name, table, page):
        pk = self.get_pk(table)
        offset = (page - 1) * ROWS_PER_PAGE
        with self.engine.connect() as conn:
            # Count
            try:
                total = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            except:
                total = 0
                
            # Select
            sql = text(f"SELECT * FROM {table} ORDER BY {pk} DESC LIMIT {ROWS_PER_PAGE} OFFSET {offset}")
            result = conn.execute(sql)
            rows = []
            for r in result:
                d = dict(r._mapping)
                # Serialize dates/binary
                for k,v in d.items():
                    if hasattr(v, 'isoformat'): d[k] = v.isoformat()
                    if isinstance(v, bytes): d[k] = "<binary>"
                d['__id'] = str(d.get(pk))
                rows.append(d)
            return rows, total

    def get_row(self, db_name, table, id):
        pk = self.get_pk(table)
        with self.engine.connect() as conn:
            sql = text(f"SELECT * FROM {table} WHERE {pk} = :id")
            res = conn.execute(sql, {"id": id}).mappings().first()
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
        # Handle switching DBs (Redis 0-15)
        if db_name:
            try:
                db_int = int(db_name.replace("DB ", ""))
                # Re-connect to specific DB index
                pool_args = self.r.connection_pool.connection_kwargs.copy()
                pool_args['db'] = db_int
                self.r = redis.Redis(**pool_args, decode_responses=True)
            except: pass
        self.r.ping()
        return True

    def list_databases(self):
        # Redis databases are fixed 0-15
        return [f"DB {i}" for i in range(16)]

    def list_tables(self, db_name):
        return ["Keys"]

    def get_rows(self, db_name, table, page):
        keys = sorted(self.r.keys('*'))
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
        
        # Simple heuristic for types
        if isinstance(val, dict): self.r.hset(key, mapping=val)
        elif isinstance(val, list): 
            self.r.delete(key)
            self.r.rpush(key, *val)
        else: self.r.set(key, str(val))

    def delete_row(self, db_name, table, id):
        self.r.delete(id)

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
# UI TEMPLATES (CODEMIRROR ENABLED)
# ==========================================

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Universal Data Manager</title>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Playfair+Display:wght@700&display=swap" rel="stylesheet">
    <script src="https://cdn.tailwindcss.com"></script>
    
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/theme/neo.min.css">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/codemirror.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.5/mode/javascript/javascript.min.js"></script>
    
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: { sans: ['"DM Sans"'], serif: ['"Playfair Display"'] },
                    colors: { brand: { light: '#F3F4F6', accent: '#86EFAC', dark: '#111827' } },
                    boxShadow: { 'hard': '4px 4px 0px 0px #111827', 'hard-hover': '6px 6px 0px 0px #111827' }
                }
            }
        }
    </script>
    <style>
        body { background-color: #FDFBF7; color: #111827; }
        .neo-box { background: white; border: 2px solid #111827; box-shadow: 4px 4px 0px 0px #111827; transition: all 0.2s; }
        .neo-box:hover { transform: translate(-2px, -2px); box-shadow: 6px 6px 0px 0px #111827; }
        .neo-btn { background-color: #111827; color: white; padding: 0.5rem 1rem; border: 2px solid #111827; font-weight: 700; text-transform: uppercase; transition: all 0.2s; }
        .neo-btn:hover { background-color: #86EFAC; color: #111827; box-shadow: 4px 4px 0px 0px #111827; transform: translate(-2px, -2px); }
        .CodeMirror { border: 2px solid #111827; height: 500px; font-family: 'DM Sans', monospace; font-size: 14px; }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #f1f1f1; border-left: 2px solid #111827; }
        ::-webkit-scrollbar-thumb { background: #111827; }
    </style>
</head>
<body class="min-h-screen flex flex-col">
    <nav class="border-b-2 border-brand-dark bg-white sticky top-0 z-50">
        <div class="max-w-7xl mx-auto px-4 h-16 flex items-center justify-between">
            <a href="/" class="flex items-center gap-2 group">
                <div class="w-8 h-8 bg-brand-dark group-hover:bg-brand-accent transition-colors"></div>
                <span class="font-serif text-xl font-bold">Data<span class="italic">Nexus</span></span>
            </a>
            {% if session.get('db_uri') %}
            <div class="flex items-center gap-4">
                <span class="text-sm font-bold bg-gray-100 px-2 py-1 border border-brand-dark hidden md:inline-block">
                    {{ session.get('current_db_name', 'Connected') }}
                </span>
                <a href="{{ url_for('logout') }}" class="text-red-600 font-bold hover:underline">LOGOUT</a>
            </div>
            {% endif %}
        </div>
    </nav>
    <main class="flex-grow">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
            <div class="max-w-4xl mx-auto mt-4 px-4">
                {% for category, message in messages %}
                <div class="p-3 mb-2 border-2 border-brand-dark font-bold text-sm {{ 'bg-red-100' if category == 'error' else 'bg-green-100' }}">
                    {{ message }}
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
<div class="min-h-[80vh] flex flex-col items-center justify-center px-4">
    <div class="w-full max-w-2xl text-center">
        <h1 class="font-serif text-6xl font-bold mb-4">Connect.</h1>
        <p class="text-gray-600 mb-8">Access Postgres, Mongo, MySQL, or Redis.</p>
        <form method="POST" action="{{ url_for('connect_db') }}" class="relative">
            <input type="text" name="db_uri" required placeholder="postgresql://... or mongodb://..." 
                   class="w-full border-2 border-brand-dark p-4 text-center text-xl shadow-hard focus:outline-none focus:bg-green-50">
            <button type="submit" class="mt-4 neo-btn w-full py-4 text-xl">Enter System &rarr;</button>
        </form>
    </div>
</div>
{% endblock %}
"""

DASHBOARD_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-7xl mx-auto px-4 py-8">
    <h2 class="font-serif text-3xl font-bold mb-6 border-b-2 border-brand-dark pb-2">Select Database</h2>
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {% for db in dbs %}
        <a href="{{ url_for('list_tables', db_name=db) }}" class="neo-box p-6 flex items-center justify-between group">
            <span class="font-bold text-lg truncate">{{ db }}</span>
            <span class="opacity-0 group-hover:opacity-100 transition-opacity">&rarr;</span>
        </a>
        {% endfor %}
    </div>
</div>
{% endblock %}
"""

TABLES_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-7xl mx-auto px-4 py-8">
    <div class="flex items-center gap-4 mb-6">
        <a href="{{ url_for('dashboard') }}" class="neo-btn bg-white text-brand-dark">&larr; Databases</a>
        <h2 class="font-serif text-3xl font-bold">Tables in <span class="text-gray-500">{{ db_name }}</span></h2>
    </div>
    {% if tables %}
    <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
        {% for table in tables %}
        <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="neo-box p-6 group hover:bg-brand-light">
            <h3 class="font-bold text-xl mb-2">{{ table }}</h3>
            <p class="text-xs text-gray-500 uppercase font-bold tracking-wider">Collection / Table</p>
        </a>
        {% endfor %}
    </div>
    {% else %}
    <div class="p-12 border-2 border-dashed border-gray-300 text-center text-gray-400 font-bold text-xl">Empty Database</div>
    {% endif %}
</div>
{% endblock %}
"""

ROWS_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-[95%] mx-auto px-4 py-8">
    <div class="flex justify-between items-center mb-6">
        <div class="flex items-center gap-4">
            <a href="{{ url_for('list_tables', db_name=db_name) }}" class="neo-btn bg-white text-brand-dark">&larr; Tables</a>
            <h2 class="font-serif text-2xl font-bold">{{ table }}</h2>
        </div>
        <a href="{{ url_for('edit_row', db_name=db_name, table=table, id='new') }}" class="neo-btn bg-brand-accent text-brand-dark">+ New Record</a>
    </div>

    <div class="bg-white border-2 border-brand-dark shadow-hard overflow-x-auto">
        <table class="w-full text-left border-collapse">
            <thead>
                <tr class="bg-gray-100 border-b-2 border-brand-dark text-xs uppercase">
                    <th class="p-3 border-r border-gray-200 w-24">Actions</th>
                    <th class="p-3 border-r border-gray-200">ID</th>
                    <th class="p-3">Data Preview</th>
                </tr>
            </thead>
            <tbody>
                {% for row in rows %}
                <tr class="hover:bg-blue-50 border-b border-gray-100">
                    <td class="p-3 border-r border-gray-200 flex gap-3">
                        <a href="{{ url_for('edit_row', db_name=db_name, table=table, id=row['__id']) }}" class="text-blue-600 font-bold hover:underline">Edit</a>
                        <form method="POST" action="{{ url_for('delete_row', db_name=db_name, table=table, id=row['__id']) }}" onsubmit="return confirm('Delete?');">
                            <button class="text-red-600 font-bold hover:underline">Del</button>
                        </form>
                    </td>
                    <td class="p-3 border-r border-gray-200 font-mono text-xs">{{ row['__id'] }}</td>
                    <td class="p-3 font-mono text-xs text-gray-600 truncate max-w-xl">
                        {{ row | to_pretty_json | truncate(150) }}
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% if total > 50 %}
        <div class="p-4 bg-gray-50 border-t-2 border-brand-dark flex justify-between">
            <span class="font-bold text-sm">Page {{ page }}</span>
            <div class="flex gap-2">
                {% if page > 1 %}<a href="?page={{ page-1 }}" class="underline font-bold">Prev</a>{% endif %}
                <a href="?page={{ page+1 }}" class="underline font-bold">Next</a>
            </div>
        </div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

EDITOR_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-5xl mx-auto px-4 py-8">
    <div class="mb-4 flex items-center justify-between">
        <a href="{{ url_for('view_rows', db_name=db_name, table=table) }}" class="text-gray-500 font-bold hover:text-black">&larr; Cancel</a>
        <h2 class="font-serif text-3xl font-bold">{{ 'Create New' if id == 'new' else 'Edit Record' }}</h2>
    </div>

    <form method="POST" class="bg-white border-2 border-brand-dark shadow-hard p-1">
        <textarea id="json-editor" name="json_data">{{ data }}</textarea>
        
        <div class="p-4 bg-gray-100 border-t-2 border-brand-dark flex justify-between items-center">
            <span class="text-xs font-bold text-gray-500 uppercase">JSON Mode</span>
            <button type="submit" class="neo-btn bg-brand-accent text-brand-dark px-8">Save Changes</button>
        </div>
    </form>
    
    <script>
        var editor = CodeMirror.fromTextArea(document.getElementById("json-editor"), {
            lineNumbers: true,
            mode: "application/json",
            theme: "neo",
            matchBrackets: true,
            autoCloseBrackets: true,
            gutters: ["CodeMirror-lint-markers"],
            lint: true
        });
    </script>
</div>
{% endblock %}
"""

# Register Templates
template_dict = {
    'base.html': BASE_LAYOUT,
    'index.html': INDEX_TEMPLATE,
    'dashboard.html': DASHBOARD_TEMPLATE,
    'tables.html': TABLES_TEMPLATE,
    'rows.html': ROWS_TEMPLATE,
    'editor.html': EDITOR_TEMPLATE
}
app.jinja_loader = DictLoader(template_dict)

@app.template_filter('to_pretty_json')
def to_pretty_json(value): return json_util.dumps(value, indent=None) # Compact for preview

# ==========================================
# ROUTES
# ==========================================

@app.route('/')
def index():
    if session.get('db_uri'): return redirect(url_for('dashboard'))
    return render_template('index.html')

@app.route('/connect', methods=['POST'])
def connect_db():
    uri = request.form.get('db_uri', '').strip()
    session['db_uri'] = uri
    adp = get_adapter()
    if adp:
        return redirect(url_for('dashboard'))
    session.pop('db_uri', None)
    flash('Connection Failed', 'error')
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/dbs')
def dashboard():
    adp = get_adapter()
    if not adp: return redirect(url_for('index'))
    dbs = adp.list_databases()
    return render_template('dashboard.html', dbs=dbs)

@app.route('/dbs/<db_name>')
def list_tables(db_name):
    session['current_db_name'] = db_name
    adp = get_adapter(db_name)
    if not adp: return redirect(url_for('index'))
    try:
        tables = adp.list_tables(db_name)
        return render_template('tables.html', db_name=db_name, tables=tables)
    except Exception as e:
        flash(str(e), 'error')
        return redirect(url_for('dashboard'))

# FIXED: Removed space in <table >
@app.route('/dbs/<db_name>/<table>')
def view_rows(db_name, table):
    adp = get_adapter(db_name)
    page = int(request.args.get('page', 1))
    try:
        rows, total = adp.get_rows(db_name, table, page)
        return render_template('rows.html', db_name=db_name, table=table, rows=rows, total=total, page=page)
    except Exception as e:
        flash(str(e), 'error')
        return redirect(url_for('list_tables', db_name=db_name))

@app.route('/dbs/<db_name>/<table>/<id>/edit', methods=['GET', 'POST'])
def edit_row(db_name, table, id):
    adp = get_adapter(db_name)
    if request.method == 'POST':
        try:
            data = json.loads(request.form['json_data'])
            adp.save_row(db_name, table, id, data, is_new=(id=='new'))
            flash('Saved!', 'success')
            return redirect(url_for('view_rows', db_name=db_name, table=table))
        except Exception as e:
            flash(str(e), 'error')

    data_str = "{\n\n}"
    if id != 'new':
        row = adp.get_row(db_name, table, id)
        if row: data_str = json_util.dumps(row, indent=2)
    
    return render_template('editor.html', db_name=db_name, table=table, id=id, data=data_str)

@app.route('/dbs/<db_name>/<table>/<id>/delete', methods=['POST'])
def delete_row(db_name, table, id):
    adp = get_adapter(db_name)
    try:
        adp.delete_row(db_name, table, id)
        flash('Deleted', 'success')
    except Exception as e:
        flash(str(e), 'error')
    return redirect(url_for('view_rows', db_name=db_name, table=table))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
