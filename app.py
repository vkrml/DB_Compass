import os
import json
import math
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
# CONFIGURATION & LOGGING
# ==========================================
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'change_this_secret_key_in_production')
app.jinja_env.globals.update(max=max, min=min, str=str, type=type, len=len)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

ROWS_PER_PAGE = 25

# ==========================================
# DATABASE ADAPTER LAYER
# ==========================================
class DatabaseAdapter:
    """Abstract interface for DB operations."""
    def connect(self, uri): raise NotImplementedError
    def list_groups(self): raise NotImplementedError # Databases/Schemas
    def list_items(self, group): raise NotImplementedError # Collections/Tables/Keys
    def get_rows(self, group, item, page, sort_field, sort_order): raise NotImplementedError
    def get_row(self, group, item, id): raise NotImplementedError
    def update_row(self, group, item, id, data): raise NotImplementedError
    def delete_row(self, group, item, id): raise NotImplementedError
    def add_row(self, group, item, data): raise NotImplementedError

class MongoAdapter(DatabaseAdapter):
    def __init__(self):
        self.client = None

    def connect(self, uri):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.client.server_info()
        return True

    def list_groups(self):
        return self.client.list_database_names()

    def list_items(self, group):
        return self.client[group].list_collection_names()

    def get_rows(self, group, item, page, sort_field, sort_order):
        col = self.client[group][item]
        sort_dir = ASCENDING if sort_order == 'asc' else DESCENDING
        skip = (page - 1) * ROWS_PER_PAGE
        
        real_sort_field = '_id' if sort_field == 'id' else sort_field
        
        total = col.count_documents({})
        cursor = col.find().sort(real_sort_field, sort_dir).skip(skip).limit(ROWS_PER_PAGE)
        
        rows = []
        for doc in cursor:
            doc['__id'] = str(doc['_id'])
            rows.append(doc)
        return rows, total

    def get_row(self, group, item, id):
        try: oid = ObjectId(id)
        except: oid = id
        return self.client[group][item].find_one({'_id': oid})

    def update_row(self, group, item, id, data):
        try: oid = ObjectId(id)
        except: oid = id
        if '_id' in data: del data['_id']
        if '__id' in data: del data['__id']
        self.client[group][item].replace_one({'_id': oid}, data)
        return True

    def add_row(self, group, item, data):
        res = self.client[group][item].insert_one(data)
        return str(res.inserted_id)

    def delete_row(self, group, item, id):
        try: oid = ObjectId(id)
        except: oid = id
        self.client[group][item].delete_one({'_id': oid})
        return True

class SQLAdapter(DatabaseAdapter):
    def __init__(self):
        self.engine = None
        self.inspector = None

    def connect(self, uri):
        # 1. Fix dialect string
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql://", 1)
        
        # 2. Fix NeonDB/Azure specific issues with channel_binding
        # Some docker envs don't support it, causing crashes. We attempt to strip it if present.
        try:
            parsed = urlparse(uri)
            qs = parse_qs(parsed.query)
            if 'channel_binding' in qs:
                del qs['channel_binding']
                new_query = urlencode(qs, doseq=True)
                uri = urlunparse(parsed._replace(query=new_query))
        except Exception:
            pass # Use original if parsing fails

        self.engine = create_engine(uri)
        with self.engine.connect() as conn:
            pass
        self.inspector = inspect(self.engine)
        return True

    def list_groups(self):
        return [urlparse(str(self.engine.url)).database or 'default']

    def list_items(self, group):
        return self.inspector.get_table_names()

    def get_pk(self, table):
        pk = self.inspector.get_pk_constraint(table)
        return pk['constrained_columns'][0] if pk['constrained_columns'] else 'id'

    def get_rows(self, group, item, page, sort_field, sort_order):
        pk = self.get_pk(item)
        sf = pk if sort_field == 'id' else sort_field
        offset = (page - 1) * ROWS_PER_PAGE
        
        with self.engine.connect() as conn:
            total = conn.execute(text(f"SELECT COUNT(*) FROM {item}")).scalar()
            
            # Basic validation to prevent major injection, though SQLAlch handles params
            if not re.match(r'^[a-zA-Z0-9_]+$', sf): sf = pk
            if sort_order not in ['asc', 'desc']: sort_order = 'asc'

            sql = f"SELECT * FROM {item} ORDER BY {sf} {sort_order.upper()} LIMIT {ROWS_PER_PAGE} OFFSET {offset}"
            result = conn.execute(text(sql))
            
            rows = []
            for row in result:
                d = dict(row._mapping)
                # Convert non-serializable objects (like datetime) to string
                for k, v in d.items():
                    if hasattr(v, 'isoformat'): d[k] = v.isoformat()
                d['__id'] = str(d.get(pk))
                rows.append(d)
            return rows, total

    def get_row(self, group, item, id):
        pk = self.get_pk(item)
        with self.engine.connect() as conn:
            sql = text(f"SELECT * FROM {item} WHERE {pk} = :id")
            result = conn.execute(sql, {"id": id}).mappings().first()
            if result:
                d = dict(result)
                for k, v in d.items():
                    if hasattr(v, 'isoformat'): d[k] = v.isoformat()
                return d
            return None

    def update_row(self, group, item, id, data):
        pk = self.get_pk(item)
        if '__id' in data: del data['__id']
        
        set_clause = ", ".join([f"{k} = :{k}" for k in data.keys()])
        with self.engine.begin() as conn:
            sql = text(f"UPDATE {item} SET {set_clause} WHERE {pk} = :pk_val")
            data['pk_val'] = id
            conn.execute(sql, data)
        return True

    def add_row(self, group, item, data):
        cols = ", ".join(data.keys())
        vals = ", ".join([f":{k}" for k in data.keys()])
        with self.engine.begin() as conn:
            sql = text(f"INSERT INTO {item} ({cols}) VALUES ({vals})")
            conn.execute(sql, data)
        return "New Row"

    def delete_row(self, group, item, id):
        pk = self.get_pk(item)
        with self.engine.begin() as conn:
            sql = text(f"DELETE FROM {item} WHERE {pk} = :id")
            conn.execute(sql, {"id": id})
        return True

class RedisAdapter(DatabaseAdapter):
    def __init__(self):
        self.r = None
    
    def connect(self, uri):
        self.r = redis.from_url(uri, decode_responses=True)
        self.r.ping()
        return True

    def list_groups(self):
        # Redis has 16 DBs (0-15), but from_url usually connects to one.
        # We'll just show the current DB index.
        info = self.r.connection_pool.connection_kwargs
        db = info.get('db', 0)
        return [f"DB {db}"]

    def list_items(self, group):
        # For Redis, we don't have tables. We'll return a virtual item called "Keys"
        # Or we could scan for key prefixes. For simplicity:
        return ["All Keys"]

    def get_rows(self, group, item, page, sort_field, sort_order):
        # Redis SCAN is better for production than KEYS
        cursor = 0
        all_keys = []
        # We need to scan enough to fill a page. This is tricky in Redis.
        # Simplification: Get first 1000 keys and paginate inside memory
        # In a real heavy prod app, you'd use SCAN cursors for pagination.
        keys = self.r.keys('*')
        keys.sort(reverse=(sort_order == 'desc'))
        
        total = len(keys)
        start = (page - 1) * ROWS_PER_PAGE
        end = start + ROWS_PER_PAGE
        page_keys = keys[start:end]
        
        rows = []
        for key in page_keys:
            type_ = self.r.type(key)
            val = "(complex type)"
            if type_ == 'string':
                val = self.r.get(key)
            elif type_ == 'hash':
                val = self.r.hgetall(key)
            elif type_ == 'list':
                val = self.r.lrange(key, 0, -1)
            elif type_ == 'set':
                val = list(self.r.smembers(key))
            
            rows.append({
                '__id': key,
                'type': type_,
                'value': val
            })
        return rows, total

    def get_row(self, group, item, id):
        # id is the Key
        type_ = self.r.type(id)
        if type_ == 'none': return None
        
        val = None
        if type_ == 'string': val = self.r.get(id)
        elif type_ == 'hash': val = self.r.hgetall(id)
        elif type_ == 'list': val = self.r.lrange(id, 0, -1)
        elif type_ == 'set': val = list(self.r.smembers(id))
        
        return {'key': id, 'type': type_, 'value': val}

    def update_row(self, group, item, id, data):
        # Redis update is tricky via JSON. 
        # We expect data to be {'value': ...}
        val = data.get('value')
        
        # If the user provides a raw JSON object, we assume they want to store a Hash or String
        if isinstance(val, dict):
            self.r.delete(id) # clear old type
            self.r.hset(id, mapping=val)
        elif isinstance(val, list):
            self.r.delete(id)
            self.r.rpush(id, *val)
        else:
            self.r.set(id, str(val))
        return True

    def add_row(self, group, item, data):
        key = data.get('key')
        val = data.get('value')
        if not key: raise Exception("Key is required")
        
        if isinstance(val, dict):
            self.r.hset(key, mapping=val)
        elif isinstance(val, list):
            self.r.rpush(key, *val)
        else:
            self.r.set(key, str(val))
        return key

    def delete_row(self, group, item, id):
        self.r.delete(id)
        return True

# Factory
def get_adapter_instance():
    uri = session.get('db_uri', '')
    if not uri: return None
    
    try:
        if uri.startswith('mongodb'):
            adapter = MongoAdapter()
        elif uri.startswith('redis') or uri.startswith('rediss'):
            adapter = RedisAdapter()
        else:
            adapter = SQLAdapter()
        
        adapter.connect(uri)
        return adapter
    except Exception as e:
        logging.error(f"Connection Error: {e}")
        return None

# ==========================================
# UI TEMPLATES
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
        .neo-input { width: 100%; border: 2px solid #111827; padding: 1rem; outline: none; transition: all 0.2s; }
        .neo-input:focus { background-color: #F0FDF4; box-shadow: 4px 4px 0px 0px #111827; }
        .neo-btn { background-color: #111827; color: white; padding: 0.75rem 1.5rem; border: 2px solid #111827; font-weight: 700; text-transform: uppercase; transition: all 0.2s; }
        .neo-btn:hover { background-color: #86EFAC; color: #111827; box-shadow: 4px 4px 0px 0px #111827; transform: translate(-2px, -2px); }
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #f1f1f1; border-left: 2px solid #111827; }
        ::-webkit-scrollbar-thumb { background: #111827; }
    </style>
</head>
<body class="min-h-screen flex flex-col">
    <nav class="border-b-2 border-brand-dark bg-white sticky top-0 z-50">
        <div class="max-w-7xl mx-auto px-4 h-20 flex items-center justify-between">
            <a href="/" class="flex items-center gap-2 group">
                <div class="w-8 h-8 bg-brand-dark group-hover:bg-brand-accent transition-colors"></div>
                <span class="font-serif text-2xl font-bold">Data<span class="italic">Nexus</span></span>
            </a>
            <div class="flex items-center gap-6">
                {% if session.get('db_uri') %}
                <a href="{{ url_for('logout') }}" class="font-bold text-red-600 hover:bg-red-50 px-3 py-1 border-2 border-transparent hover:border-red-600 transition-all">DISCONNECT</a>
                {% endif %}
            </div>
        </div>
    </nav>
    
    {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
            <div class="max-w-2xl mx-auto mt-6 px-4">
                {% for category, message in messages %}
                    <div class="border-2 border-brand-dark p-4 mb-4 flex justify-between items-center shadow-hard {{ 'bg-red-100' if category == 'error' else 'bg-green-100' }}">
                        <p class="font-bold">{{ message }}</p>
                        <button onclick="this.parentElement.remove()" class="text-xl font-bold">&times;</button>
                    </div>
                {% endfor %}
            </div>
        {% endif %}
    {% endwith %}

    <main class="flex-grow">
        {% block content %}{% endblock %}
    </main>
</body>
</html>
"""

INDEX_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="min-h-[80vh] flex flex-col items-center justify-center px-4">
    <div class="w-full max-w-3xl text-center">
        <h1 class="font-serif text-5xl md:text-7xl font-bold mb-6">Access Your Data.</h1>
        <p class="text-xl text-gray-600 mb-12">Connect to Postgres, Mongo, MySQL, or Redis instantly.</p>

        <form method="POST" action="{{ url_for('connect_db') }}" class="relative group">
            <div class="relative z-10">
                <input type="text" name="db_uri" required placeholder="postgresql://... or redis://..." class="neo-input text-center h-20 text-xl md:text-2xl placeholder:text-gray-300" autofocus>
                <button type="submit" class="absolute right-3 top-3 bottom-3 bg-brand-dark text-white font-bold uppercase px-6 hover:bg-brand-accent hover:text-brand-dark">Connect &rarr;</button>
            </div>
            <div class="absolute inset-0 bg-brand-dark translate-x-2 translate-y-2 -z-10"></div>
        </form>
        
        <div class="mt-8 flex justify-center gap-4 text-sm font-bold text-gray-400 uppercase">
            <span>PgSQL</span> &bull; <span>Mongo</span> &bull; <span>MySQL</span> &bull; <span>Redis</span>
        </div>
    </div>
</div>
{% endblock %}
"""

DASHBOARD_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-7xl mx-auto px-4 py-12">
    <div class="flex items-center justify-between mb-12 border-b-2 border-gray-200 pb-6">
        <div>
            <p class="text-gray-500 font-bold uppercase text-sm">Connected Database</p>
            <h2 class="font-serif text-4xl font-bold">{{ groups[0] }}</h2>
        </div>
        <span class="bg-brand-accent px-4 py-2 border-2 border-brand-dark font-bold shadow-hard">Active</span>
    </div>

    {% if items %}
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
        {% for item in items %}
        <a href="{{ url_for('view_data', group=groups[0], item=item) }}" class="neo-box p-8 flex flex-col justify-between min-h-[160px] group">
            <h3 class="font-serif text-2xl font-bold">{{ item }}</h3>
            <div class="flex items-center gap-2 text-sm font-bold text-gray-500 group-hover:text-brand-dark">
                <span>EXPLORE</span> <span>&rarr;</span>
            </div>
        </a>
        {% endfor %}
    </div>
    {% else %}
    <div class="text-center py-20 border-2 border-dashed border-gray-300">
        <h3 class="text-2xl font-bold text-gray-400">No tables/collections found.</h3>
    </div>
    {% endif %}
</div>
{% endblock %}
"""

DATA_VIEW_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-[95%] mx-auto px-4 py-8">
    <div class="flex justify-between items-center mb-8">
        <div class="flex items-center gap-4">
            <a href="{{ url_for('dashboard') }}" class="neo-btn bg-white text-brand-dark hover:bg-gray-100">&larr; Back</a>
            <h1 class="font-serif text-3xl font-bold">{{ item }}</h1>
        </div>
        <a href="{{ url_for('add_row', group=group, item=item) }}" class="neo-btn bg-brand-accent text-brand-dark">+ Add New</a>
    </div>

    <div class="bg-white border-2 border-brand-dark shadow-hard overflow-hidden">
        <div class="overflow-x-auto">
            <table class="w-full text-left border-collapse">
                <thead>
                    <tr class="bg-gray-50 border-b-2 border-brand-dark text-xs uppercase">
                        <th class="p-4 border-r border-gray-200 w-24">Actions</th>
                        <th class="p-4 border-r border-gray-200">ID / Key</th>
                        <th class="p-4">Data</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-gray-200">
                    {% for row in rows %}
                    <tr class="hover:bg-gray-50">
                        <td class="p-4 border-r border-gray-200 flex gap-2">
                            <a href="{{ url_for('edit_row', group=group, item=item, id=row['__id']) }}" class="text-blue-600 font-bold hover:underline">Edit</a>
                            <form method="POST" action="{{ url_for('delete_row', group=group, item=item, id=row['__id']) }}" onsubmit="return confirm('Delete?');">
                                <button type="submit" class="text-red-600 font-bold hover:underline">Del</button>
                            </form>
                        </td>
                        <td class="p-4 font-mono text-sm border-r border-gray-200 max-w-xs truncate" title="{{ row['__id'] }}">{{ row['__id'] }}</td>
                        <td class="p-4 font-mono text-xs text-gray-600 whitespace-pre truncate max-w-3xl">{{ row | to_pretty_json | truncate(200) }}</td>
                    </tr>
                    {% else %}
                    <tr><td colspan="3" class="p-12 text-center text-gray-500">No records found.</td></tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        <div class="p-4 bg-gray-50 border-t-2 border-brand-dark flex justify-between items-center">
            <span class="font-bold text-sm">Total: {{ total }}</span>
            <div class="flex gap-2">
                {% if page > 1 %}
                <a href="?page={{ page - 1 }}" class="px-3 py-1 border-2 border-brand-dark bg-white font-bold text-sm">Prev</a>
                {% endif %}
                <span class="px-3 py-1 font-bold">{{ page }}</span>
                {% if page * 25 < total %}
                <a href="?page={{ page + 1 }}" class="px-3 py-1 border-2 border-brand-dark bg-white font-bold text-sm">Next</a>
                {% endif %}
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

EDITOR_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-4xl mx-auto px-4 py-12">
    <div class="mb-8">
        <a href="{{ url_for('view_data', group=group, item=item) }}" class="text-gray-500 font-bold hover:text-brand-dark">&larr; Cancel</a>
        <h2 class="font-serif text-4xl font-bold mt-2">{{ 'Edit' if is_edit else 'Create' }} Record</h2>
    </div>

    <form method="POST" class="bg-white border-2 border-brand-dark shadow-hard">
        <div class="p-2 bg-gray-100 border-b-2 border-brand-dark text-xs font-mono text-gray-500">JSON EDITOR</div>
        <textarea name="json_data" class="w-full h-[500px] p-6 font-mono text-sm outline-none resize-none" spellcheck="false">{{ data }}</textarea>
        <div class="border-t-2 border-brand-dark p-6 bg-gray-50 flex justify-end">
            <button type="submit" class="neo-btn bg-brand-accent text-brand-dark">Save Changes</button>
        </div>
    </form>
    {% if not is_edit and 'Keys' in item %}
    <p class="mt-4 text-sm text-gray-500">For Redis: format as <code>{"key": "mykey", "value": "myvalue"}</code></p>
    {% endif %}
</div>
{% endblock %}
"""

template_dict = {
    'base.html': BASE_LAYOUT,
    'index.html': INDEX_TEMPLATE,
    'dashboard.html': DASHBOARD_TEMPLATE,
    'view.html': DATA_VIEW_TEMPLATE,
    'editor.html': EDITOR_TEMPLATE
}
app.jinja_loader = DictLoader(template_dict)

@app.template_filter('to_pretty_json')
def to_pretty_json(value):
    return json_util.dumps(value, indent=2)

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
    adapter = get_adapter_instance()
    if adapter:
        flash("Successfully connected.", "success")
        return redirect(url_for('dashboard'))
    session.pop('db_uri', None)
    flash("Connection failed. Check URL or Firewall.", "error")
    return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/dashboard')
def dashboard():
    adapter = get_adapter_instance()
    if not adapter: return redirect(url_for('index'))
    try:
        groups = adapter.list_groups()
        items = adapter.list_items(groups[0])
        return render_template('dashboard.html', groups=groups, items=items)
    except Exception as e:
        flash(f"Error: {str(e)}", "error")
        return redirect(url_for('logout'))

@app.route('/data/<group>/<item>')
def view_data(group, item):
    adapter = get_adapter_instance()
    if not adapter: return redirect(url_for('index'))
    page = int(request.args.get('page', 1))
    sort = request.args.get('sort', 'id')
    try:
        rows, total = adapter.get_rows(group, item, page, sort, 'desc')
        return render_template('view.html', group=group, item=item, rows=rows, total=total, page=page)
    except Exception as e:
        flash(f"Read Error: {str(e)}", "error")
        return redirect(url_for('dashboard'))

@app.route('/data/<group>/<item>/add', methods=['GET', 'POST'])
def add_row(group, item):
    adapter = get_adapter_instance()
    if not adapter: return redirect(url_for('index'))
    if request.method == 'POST':
        try:
            data = json.loads(request.form['json_data'])
            adapter.add_row(group, item, data)
            flash("Created successfully", "success")
            return redirect(url_for('view_data', group=group, item=item))
        except Exception as e:
            flash(f"Create Error: {str(e)}", "error")
    
    default_data = "{\n  \"key\": \"new_key\",\n  \"value\": \"value\"\n}" if "Key" in item else "{\n\n}"
    return render_template('editor.html', group=group, item=item, data=default_data, is_edit=False)

@app.route('/data/<group>/<item>/<id>/edit', methods=['GET', 'POST'])
def edit_row(group, item, id):
    adapter = get_adapter_instance()
    if not adapter: return redirect(url_for('index'))
    if request.method == 'POST':
        try:
            data = json.loads(request.form['json_data'])
            adapter.update_row(group, item, id, data)
            flash("Updated successfully", "success")
            return redirect(url_for('view_data', group=group, item=item))
        except Exception as e:
            flash(f"Update Error: {str(e)}", "error")
    
    row = adapter.get_row(group, item, id)
    if not row: return redirect(url_for('view_data', group=group, item=item))
    return render_template('editor.html', group=group, item=item, data=json_util.dumps(row, indent=2), is_edit=True)

@app.route('/data/<group>/<item>/<id>/delete', methods=['POST'])
def delete_row(group, item, id):
    adapter = get_adapter_instance()
    if adapter:
        try:
            adapter.delete_row(group, item, id)
            flash("Deleted", "success")
        except Exception as e:
            flash(f"Delete Error: {str(e)}", "error")
    return redirect(url_for('view_data', group=group, item=item))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
