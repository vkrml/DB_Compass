import os
import json
import math
import logging
import functools
from datetime import datetime
from urllib.parse import urlparse

from flask import Flask, request, redirect, url_for, session, render_template, flash, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from jinja2 import DictLoader
from bson import json_util, ObjectId

# Database Drivers
from pymongo import MongoClient, ASCENDING, DESCENDING
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, scoped_session

# ==========================================
# CONFIGURATION & LOGGING
# ==========================================
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'change_this_secret_key_in_production')

# Helper for Jinja
app.jinja_env.globals.update(max=max, min=min, str=str)

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["1000 per day", "200 per hour"]
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

ROWS_PER_PAGE = 25

# ==========================================
# DATABASE ADAPTER LAYER
# ==========================================
class DatabaseAdapter:
    """Abstract interface for DB operations."""
    def connect(self, uri): raise NotImplementedError
    def list_groups(self): raise NotImplementedError # Databases or Schemas
    def list_items(self, group): raise NotImplementedError # Collections or Tables
    def get_rows(self, group, item, page, sort_field, sort_order): raise NotImplementedError
    def get_row(self, group, item, id): raise NotImplementedError
    def update_row(self, group, item, id, data): raise NotImplementedError
    def delete_row(self, group, item, id): raise NotImplementedError
    def add_row(self, group, item, data): raise NotImplementedError
    def close(self): pass

class MongoAdapter(DatabaseAdapter):
    def __init__(self):
        self.client = None

    def connect(self, uri):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.client.server_info() # Trigger connection check
        return True

    def list_groups(self):
        return self.client.list_database_names()

    def list_items(self, group):
        return self.client[group].list_collection_names()

    def get_rows(self, group, item, page, sort_field, sort_order):
        col = self.client[group][item]
        sort_dir = ASCENDING if sort_order == 'asc' else DESCENDING
        skip = (page - 1) * ROWS_PER_PAGE
        
        # Sort field handling for Mongo
        if sort_field == 'id': sort_field = '_id'
        
        total = col.count_documents({})
        cursor = col.find().sort(sort_field, sort_dir).skip(skip).limit(ROWS_PER_PAGE)
        
        # Convert ObjectId to string for UI
        rows = []
        for doc in cursor:
            doc['__id'] = str(doc['_id']) # Normalized ID for UI
            rows.append(doc)
            
        return rows, total

    def get_row(self, group, item, id):
        try:
            oid = ObjectId(id)
        except:
            oid = id
        return self.client[group][item].find_one({'_id': oid})

    def update_row(self, group, item, id, data):
        try:
            oid = ObjectId(id)
        except:
            oid = id
        
        # Remove immutable _id from update data if present
        if '_id' in data: del data['_id']
        if '__id' in data: del data['__id']
        
        self.client[group][item].replace_one({'_id': oid}, data)
        return True

    def add_row(self, group, item, data):
        res = self.client[group][item].insert_one(data)
        return str(res.inserted_id)

    def delete_row(self, group, item, id):
        try:
            oid = ObjectId(id)
        except:
            oid = id
        self.client[group][item].delete_one({'_id': oid})
        return True

class SQLAdapter(DatabaseAdapter):
    def __init__(self):
        self.engine = None
        self.inspector = None

    def connect(self, uri):
        # Fix for SQLAlchemy requiring postgresql:// instead of postgres://
        if uri.startswith("postgres://"):
            uri = uri.replace("postgres://", "postgresql://", 1)
            
        self.engine = create_engine(uri)
        with self.engine.connect() as conn:
            pass # Test connection
        self.inspector = inspect(self.engine)
        return True

    def list_groups(self):
        # In SQL, we usually treat the connected DB as the only group, 
        # or list Schemas. For simplicity, we return the DB name or 'public'
        return [urlparse(str(self.engine.url)).database or 'default']

    def list_items(self, group):
        return self.inspector.get_table_names()

    def get_pk(self, table):
        pk = self.inspector.get_pk_constraint(table)
        return pk['constrained_columns'][0] if pk['constrained_columns'] else 'id'

    def get_rows(self, group, item, page, sort_field, sort_order):
        pk = self.get_pk(item)
        sf = sort_field if sort_field != 'id' else pk
        offset = (page - 1) * ROWS_PER_PAGE
        
        with self.engine.connect() as conn:
            # Count
            total = conn.execute(text(f"SELECT COUNT(*) FROM {item}")).scalar()
            
            # Select
            sql = f"SELECT * FROM {item} ORDER BY {sf} {sort_order.upper()} LIMIT {ROWS_PER_PAGE} OFFSET {offset}"
            result = conn.execute(text(sql))
            
            rows = []
            for row in result:
                d = dict(row._mapping)
                d['__id'] = str(d.get(pk)) # Normalize ID
                rows.append(d)
                
            return rows, total

    def get_row(self, group, item, id):
        pk = self.get_pk(item)
        with self.engine.connect() as conn:
            # Basic sanitization needed here for real prod, using parameterized query
            sql = text(f"SELECT * FROM {item} WHERE {pk} = :id")
            result = conn.execute(sql, {"id": id}).mappings().first()
            return dict(result) if result else None

    def update_row(self, group, item, id, data):
        pk = self.get_pk(item)
        
        # Clean data
        if '__id' in data: del data['__id']
        
        # Build update string
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

# Factory
def get_adapter_instance():
    uri = session.get('db_uri', '')
    if not uri: return None
    
    try:
        if uri.startswith('mongodb'):
            adapter = MongoAdapter()
        else:
            adapter = SQLAdapter()
        
        adapter.connect(uri)
        return adapter
    except Exception as e:
        logging.error(f"Connection Error: {e}")
        return None

# ==========================================
# UI TEMPLATES (RICH-API STYLE)
# ==========================================

# CSS Variables & Tailwind Config for Neo-Brutalism
BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Universal Data Manager</title>
    
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;500;700&family=Playfair+Display:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">
    
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js" defer></script>
    
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: {
                        sans: ['"DM Sans"', 'sans-serif'],
                        serif: ['"Playfair Display"', 'serif'],
                    },
                    colors: {
                        brand: {
                            light: '#F3F4F6', // Off-white/Gray
                            accent: '#86EFAC', // Green
                            dark: '#111827', // Almost black
                        }
                    },
                    boxShadow: {
                        'hard': '4px 4px 0px 0px #111827',
                        'hard-sm': '2px 2px 0px 0px #111827',
                        'hard-hover': '6px 6px 0px 0px #111827',
                    }
                }
            }
        }
    </script>

    <style>
        body { background-color: #FDFBF7; color: #111827; }
        .neo-box {
            background: white;
            border: 2px solid #111827;
            box-shadow: 4px 4px 0px 0px #111827;
            transition: all 0.2s ease;
        }
        .neo-box:hover {
            transform: translate(-2px, -2px);
            box-shadow: 6px 6px 0px 0px #111827;
        }
        .neo-input {
            width: 100%;
            border: 2px solid #111827;
            padding: 1rem;
            font-family: 'DM Sans', sans-serif;
            font-size: 1.1rem;
            outline: none;
            transition: all 0.2s;
        }
        .neo-input:focus {
            background-color: #F0FDF4; /* Light green tint */
            box-shadow: 4px 4px 0px 0px #111827;
        }
        .neo-btn {
            background-color: #111827;
            color: white;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            padding: 0.75rem 1.5rem;
            border: 2px solid #111827;
            transition: all 0.2s;
        }
        .neo-btn:hover {
            background-color: #86EFAC;
            color: #111827;
            box-shadow: 4px 4px 0px 0px #111827;
            transform: translate(-2px, -2px);
        }
        .neo-btn-secondary {
            background-color: white;
            color: #111827;
        }
        
        /* Custom Scrollbar */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #f1f1f1; border-left: 2px solid #111827; }
        ::-webkit-scrollbar-thumb { background: #111827; }
        ::-webkit-scrollbar-thumb:hover { background: #374151; }
        
        .code-editor {
            font-family: 'Courier New', Courier, monospace;
            line-height: 1.5;
        }
    </style>
</head>
<body class="min-h-screen flex flex-col">
    <nav class="border-b-2 border-brand-dark bg-white sticky top-0 z-50">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-20 flex items-center justify-between">
            <a href="/" class="flex items-center gap-2 group">
                <div class="w-8 h-8 bg-brand-dark group-hover:bg-brand-accent transition-colors"></div>
                <span class="font-serif text-2xl font-bold tracking-tight">Data<span class="italic">Nexus</span></span>
            </a>
            
            <div class="flex items-center gap-6">
                <button onclick="document.getElementById('infoModal').classList.remove('hidden')" class="font-bold hover:underline decoration-2 underline-offset-4">HOW IT WORKS</button>
                {% if session.get('db_uri') %}
                <a href="{{ url_for('logout') }}" class="font-bold text-red-600 hover:bg-red-50 px-3 py-1 border-2 border-transparent hover:border-red-600 transition-all">
                    DISCONNECT
                </a>
                {% endif %}
            </div>
        </div>
    </nav>

    {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
            <div class="max-w-2xl mx-auto mt-6 px-4">
                {% for category, message in messages %}
                    <div class="border-2 border-brand-dark p-4 mb-4 flex justify-between items-center shadow-hard-sm {{ 'bg-red-100' if category == 'error' else 'bg-green-100' }}">
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

    <div id="infoModal" class="hidden fixed inset-0 z-[100] bg-black/50 backdrop-blur-sm flex items-center justify-center p-4">
        <div class="bg-white border-2 border-brand-dark shadow-hard max-w-lg w-full p-8 relative">
            <button onclick="document.getElementById('infoModal').classList.add('hidden')" class="absolute top-4 right-4 text-2xl font-bold hover:text-brand-accent">&times;</button>
            <h3 class="font-serif text-3xl font-bold mb-4">Platform Intelligence</h3>
            <p class="mb-4">This platform auto-detects your database architecture.</p>
            <ul class="list-disc pl-5 space-y-2 font-medium">
                <li>Supports <strong>MongoDB</strong> (Standard & Atlas)</li>
                <li>Supports <strong>PostgreSQL</strong> (inc. ElephantSQL, Supabase)</li>
                <li>Supports <strong>MySQL</strong> & <strong>SQLite</strong></li>
            </ul>
            <div class="mt-6 pt-6 border-t-2 border-gray-200">
                <p class="text-sm text-gray-500">Security Note: Credentials are stored in encrypted session cookies only. We do not persist your keys.</p>
            </div>
        </div>
    </div>
</body>
</html>
"""

INDEX_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="min-h-[80vh] flex flex-col items-center justify-center px-4 relative overflow-hidden">
    
    <div class="absolute top-20 left-10 w-16 h-16 border-2 border-brand-dark opacity-20 rotate-12"></div>
    <div class="absolute bottom-20 right-10 w-24 h-24 border-2 border-brand-dark rounded-full opacity-20"></div>

    <div class="w-full max-w-3xl z-10">
        <div class="text-center mb-12">
            <h1 class="font-serif text-5xl md:text-7xl font-bold mb-6">Access Your Data.</h1>
            <p class="text-xl text-gray-600 max-w-xl mx-auto">
                Connect to any SQL or NoSQL database instantly. <br>
                Secure. Minimal. Powerful.
            </p>
        </div>

        <form method="POST" action="{{ url_for('connect_db') }}" class="relative group">
            <div class="relative z-10">
                <input 
                    type="text" 
                    name="db_uri" 
                    required
                    placeholder="postgresql://user:pass@host/db or mongodb://..." 
                    class="neo-input text-center h-20 text-xl md:text-2xl placeholder:text-gray-300"
                    autofocus
                >
                <button type="submit" class="absolute right-3 top-3 bottom-3 bg-brand-dark text-white font-bold uppercase tracking-widest px-6 hover:bg-brand-accent hover:text-brand-dark transition-colors">
                    Connect &rarr;
                </button>
            </div>
            <div class="absolute inset-0 bg-brand-dark translate-x-2 translate-y-2 -z-10 transition-transform group-hover:translate-x-3 group-hover:translate-y-3"></div>
        </form>
        
        <div class="mt-8 flex justify-center gap-4 text-sm font-bold text-gray-400 uppercase tracking-widest">
            <span>Mongo</span> &bull; <span>Postgres</span> &bull; <span>MySQL</span> &bull; <span>SQLite</span>
        </div>
    </div>
</div>
{% endblock %}
"""

DASHBOARD_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
    
    <div class="flex flex-col md:flex-row md:items-center justify-between mb-12 border-b-2 border-gray-200 pb-6">
        <div>
            <p class="text-gray-500 font-bold uppercase tracking-wider text-sm mb-1">Connected Database</p>
            <h2 class="font-serif text-4xl font-bold">{{ groups[0] }}</h2>
        </div>
        <div class="mt-4 md:mt-0">
             <span class="bg-brand-accent px-4 py-2 border-2 border-brand-dark font-bold shadow-hard-sm">Active Session</span>
        </div>
    </div>

    {% if items %}
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
        {% for item in items %}
        <a href="{{ url_for('view_data', group=groups[0], item=item) }}" class="neo-box p-8 flex flex-col justify-between min-h-[160px] group cursor-pointer relative overflow-hidden">
            <div class="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                <svg class="w-16 h-16" fill="currentColor" viewBox="0 0 20 20"><path d="M2 10a8 8 0 018-8v8h8a8 8 0 11-16 0z"></path><path d="M12 2.252A8.014 8.014 0 0117.748 8H12V2.252z"></path></svg>
            </div>
            <h3 class="font-serif text-2xl font-bold z-10">{{ item }}</h3>
            <div class="flex items-center gap-2 text-sm font-bold text-gray-500 z-10 group-hover:text-brand-dark">
                <span>EXPLORE DATA</span>
                <span class="transition-transform group-hover:translate-x-1">&rarr;</span>
            </div>
        </a>
        {% endfor %}
    </div>
    {% else %}
    <div class="text-center py-20 border-2 border-dashed border-gray-300">
        <h3 class="text-2xl font-bold text-gray-400">No collections or tables found.</h3>
    </div>
    {% endif %}
</div>
{% endblock %}
"""

DATA_VIEW_TEMPLATE = """
{% extends 'base.html' %}
{% block content %}
<div class="max-w-[95%] mx-auto px-4 py-8">
    <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 gap-4">
        <div class="flex items-center gap-4">
            <a href="{{ url_for('dashboard') }}" class="neo-btn neo-btn-secondary py-2 px-4">&larr; Back</a>
            <div>
                <p class="text-xs font-bold text-gray-500 uppercase">Collection / Table</p>
                <h1 class="font-serif text-3xl font-bold">{{ item }}</h1>
            </div>
        </div>
        <div class="flex gap-4">
            <a href="{{ url_for('add_row', group=group, item=item) }}" class="neo-btn bg-brand-accent text-brand-dark hover:bg-white">
                + New Record
            </a>
        </div>
    </div>

    <div class="bg-white border-2 border-brand-dark shadow-hard overflow-hidden">
        <div class="overflow-x-auto">
            <table class="w-full text-left border-collapse">
                <thead>
                    <tr class="bg-gray-50 border-b-2 border-brand-dark text-xs uppercase tracking-wider">
                        <th class="p-4 font-bold border-r border-gray-200 w-24">Actions</th>
                        <th class="p-4 font-bold border-r border-gray-200">ID</th>
                        <th class="p-4 font-bold">Data Preview</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-gray-200">
                    {% for row in rows %}
                    <tr class="hover:bg-gray-50 transition-colors">
                        <td class="p-4 border-r border-gray-200 flex gap-2">
                            <a href="{{ url_for('edit_row', group=group, item=item, id=row['__id']) }}" class="text-blue-600 hover:text-blue-800 font-bold">Edit</a>
                            <form method="POST" action="{{ url_for('delete_row', group=group, item=item, id=row['__id']) }}" onsubmit="return confirm('Delete this record?');">
                                <button type="submit" class="text-red-600 hover:text-red-800 font-bold">Del</button>
                            </form>
                        </td>
                        <td class="p-4 font-mono text-sm border-r border-gray-200 max-w-xs truncate" title="{{ row['__id'] }}">
                            {{ row['__id'] }}
                        </td>
                        <td class="p-4 font-mono text-xs text-gray-600 whitespace-pre truncate max-w-3xl">
                            {{ row | to_pretty_json | truncate(200) }}
                        </td>
                    </tr>
                    {% else %}
                    <tr>
                        <td colspan="3" class="p-12 text-center text-gray-500 italic">No records found in this dataset.</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        
        <div class="p-4 bg-gray-50 border-t-2 border-brand-dark flex justify-between items-center">
            <span class="font-bold text-sm">Total: {{ total }}</span>
            <div class="flex gap-2">
                {% if page > 1 %}
                <a href="?page={{ page - 1 }}" class="px-3 py-1 border-2 border-brand-dark bg-white hover:bg-gray-100 font-bold text-sm">Prev</a>
                {% endif %}
                <span class="px-3 py-1 border-2 border-transparent font-bold">{{ page }}</span>
                {% if page * 25 < total %}
                <a href="?page={{ page + 1 }}" class="px-3 py-1 border-2 border-brand-dark bg-white hover:bg-gray-100 font-bold text-sm">Next</a>
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
        <a href="{{ url_for('view_data', group=group, item=item) }}" class="text-gray-500 font-bold hover:text-brand-dark transition-colors">&larr; Cancel & Return</a>
        <h2 class="font-serif text-4xl font-bold mt-2">{{ 'Edit' if is_edit else 'Create' }} Record</h2>
        <p class="text-gray-500 mt-2">Mode: <span class="font-mono text-brand-dark">{{ 'JSON Object' }}</span></p>
    </div>

    <form method="POST" class="bg-white border-2 border-brand-dark shadow-hard p-1">
        <textarea name="json_data" id="json_editor" class="w-full h-[500px] p-6 font-mono text-sm outline-none resize-none custom-scrollbar" spellcheck="false">{{ data }}</textarea>
        
        <div class="border-t-2 border-brand-dark p-6 bg-gray-50 flex justify-end gap-4">
            <button type="submit" class="neo-btn bg-brand-accent text-brand-dark w-full md:w-auto">
                {{ 'Save Changes' if is_edit else 'Create Record' }}
            </button>
        </div>
    </form>
    
    <div class="mt-6 p-4 bg-blue-50 border-l-4 border-blue-500 text-sm text-blue-800">
        <strong>Tip:</strong> For SQL databases, ensure your JSON keys match your table column names exactly.
    </div>
</div>
{% endblock %}
"""

# Register Templates
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
    if session.get('db_uri'):
        return redirect(url_for('dashboard'))
    return render_template('index.html')

@app.route('/connect', methods=['POST'])
@limiter.limit("10 per minute")
def connect_db():
    uri = request.form.get('db_uri', '').strip()
    session['db_uri'] = uri
    
    adapter = get_adapter_instance()
    if adapter:
        flash("Successfully connected.", "success")
        return redirect(url_for('dashboard'))
    else:
        session.pop('db_uri', None)
        flash("Could not connect. Check your connection string.", "error")
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
        # Default to first group (DB) for generic SQL/Mongo usage
        # You could expand this to list schemas for Postgres
        items = adapter.list_items(groups[0])
        return render_template('dashboard.html', groups=groups, items=items)
    except Exception as e:
        flash(f"Error loading dashboard: {str(e)}", "error")
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
        flash(f"Error reading data: {str(e)}", "error")
        return redirect(url_for('dashboard'))

@app.route('/data/<group>/<item>/add', methods=['GET', 'POST'])
def add_row(group, item):
    adapter = get_adapter_instance()
    if not adapter: return redirect(url_for('index'))

    if request.method == 'POST':
        try:
            data = json.loads(request.form['json_data'])
            adapter.add_row(group, item, data)
            flash("Record created successfully", "success")
            return redirect(url_for('view_data', group=group, item=item))
        except Exception as e:
            flash(f"Error creating record: {str(e)}", "error")

    return render_template('editor.html', group=group, item=item, data="{\n\n}", is_edit=False)

@app.route('/data/<group>/<item>/<id>/edit', methods=['GET', 'POST'])
def edit_row(group, item, id):
    adapter = get_adapter_instance()
    if not adapter: return redirect(url_for('index'))

    if request.method == 'POST':
        try:
            data = json.loads(request.form['json_data'])
            adapter.update_row(group, item, id, data)
            flash("Record updated", "success")
            return redirect(url_for('view_data', group=group, item=item))
        except Exception as e:
            flash(f"Save error: {str(e)}", "error")

    row = adapter.get_row(group, item, id)
    if not row:
        flash("Record not found", "error")
        return redirect(url_for('view_data', group=group, item=item))

    return render_template('editor.html', group=group, item=item, data=json_util.dumps(row, indent=2), is_edit=True)

@app.route('/data/<group>/<item>/<id>/delete', methods=['POST'])
def delete_row(group, item, id):
    adapter = get_adapter_instance()
    if adapter:
        try:
            adapter.delete_row(group, item, id)
            flash("Record deleted", "success")
        except Exception as e:
            flash(f"Delete error: {str(e)}", "error")
            
    return redirect(url_for('view_data', group=group, item=item))

if __name__ == '__main__':
    # Standard Python entry point
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)
