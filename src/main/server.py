import os
# Patch gevent para compatibilidade com o driver Cassandra
os.environ['CASS_DRIVER_GEVENT_PATCH'] = 'true'

import asyncio
import json
import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from cassandra.cluster import Cluster
from cassandra.protocol import SyntaxException

app = FastAPI()

# --- Configuração do Cassandra ---
cassandra_session = None

def connect_to_cassandra():
    """Conecta ao cluster Cassandra e retorna a sessão."""
    global cassandra_session
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS btc
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)
        session.set_keyspace('btc')
        print("Conexão com Cassandra estabelecida com sucesso.")
        return session
    except Exception as e:
        print(f"Erro fatal ao conectar com Cassandra: {e}")
        return None

# --- Gerenciamento de Conexões WebSocket ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast_json(self, data: dict):
        if not data or not data.get("points"):
            return
        message = json.dumps(data)
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# --- Lógica de Streaming de Dados ---
async def data_streamer():
    """Verifica novos dados no Cassandra e transmite via WebSocket."""
    global cassandra_session
    if not cassandra_session:
        print("Sessão do Cassandra não está disponível. Tentando reconectar...")
        cassandra_session = connect_to_cassandra()
        if not cassandra_session:
            await asyncio.sleep(5) # Espera antes de tentar novamente
            return

    last_id = 0
    while True:
        try:
            now = datetime.datetime.now()
            table_name = f"btc_{now.strftime('%Y_%m_%d')}"
            day_partition = now.strftime('%Y-%m-%d')

            # Garante que a tabela do dia exista
            cassandra_session.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                day_partition TEXT,
                sequential_id INT,
                id UUID,
                dia_tempo TIMESTAMP,
                valor DECIMAL,
                volume_buy DECIMAL,
                volume_sell DECIMAL,
                PRIMARY KEY (day_partition, sequential_id)
            ) WITH CLUSTERING ORDER BY (sequential_id ASC);""")

            # Busca novos dados
            query = f"SELECT sequential_id, valor FROM {table_name} WHERE day_partition = %s AND sequential_id > %s"
            rows = cassandra_session.execute(query, (day_partition, last_id))
            
            new_data = []
            max_id_in_batch = last_id

            for row in rows:
                new_data.append({"id": row.sequential_id, "value": float(row.valor)})
                if row.sequential_id > max_id_in_batch:
                    max_id_in_batch = row.sequential_id
            
            if new_data:
                print(f"Enviando {len(new_data)} novos pontos de dados da tabela {table_name}.")
                await manager.broadcast_json({"table": table_name, "points": new_data})
                last_id = max_id_in_batch

        except SyntaxException as e:
            print(f"Erro de sintaxe na query do Cassandra (a tabela pode não existir ainda): {e}")
        except Exception as e:
            print(f"Erro durante o streaming de dados: {e}")
            # Em caso de erro, reseta a conexão para tentar novamente
            cassandra_session = None 

        await asyncio.sleep(1) # Verifica a cada 1 segundo

# --- Endpoints da API ---
@app.on_event("startup")
async def startup_event():
    """Inicia a conexão com o BD e a tarefa de streaming."""
    global cassandra_session
    cassandra_session = connect_to_cassandra()
    asyncio.create_task(data_streamer())

@app.get("/api/tables")
async def get_tables():
    """Lista todas as tabelas de dados de bitcoin do keyspace 'btc'."""
    if not cassandra_session:
        return {"error": "Cassandra connection not available"}, 500
    
    try:
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'btc'"
        rows = cassandra_session.execute(query)
        table_names = sorted([row.table_name for row in rows if row.table_name.startswith('btc_')], reverse=True)
        return {"tables": table_names}
    except Exception as e:
        print(f"Erro ao buscar tabelas: {e}")
        return {"error": "Could not fetch tables from Cassandra"}, 500

@app.get("/api/data/{table_name}")
async def get_table_data(table_name: str):
    """Retorna todos os dados de uma tabela específica."""
    if not cassandra_session:
        return {"error": "Cassandra connection not available"}, 500
    
    if not table_name.startswith("btc_") or not all(c.isalnum() or c == '_' for c in table_name):
        return {"error": "Invalid table name"}, 400

    try:
        day_partition = table_name.replace("btc_", "").replace("_", "-")
        
        query = f"SELECT sequential_id, valor FROM {table_name} WHERE day_partition = %s"
        rows = cassandra_session.execute(query, (day_partition,))
        
        data = [{"id": row.sequential_id, "value": float(row.valor)} for row in rows]
        return {"data": data}
    except SyntaxException:
        return {"error": f"Table '{table_name}' not found or query is invalid."}, 404
    except Exception as e:
        print(f"Erro ao buscar dados da tabela {table_name}: {e}")
        return {"error": f"Could not fetch data for table {table_name}"}, 500

@app.get("/")
async def get_index():
    """Serve o arquivo HTML principal (menu de seleção)."""
    try:
        with open("src/web/html/index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html não encontrado.</h1>", status_code=404)

@app.get("/plotly")
async def get_plotly():
    """Serve a página do gráfico Plotly."""
    try:
        with open("src/web/html/plotly.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>plotly.html não encontrado.</h1>", status_code=404)

@app.get("/chartjs")
async def get_chartjs():
    """Serve a página do gráfico Chart.js."""
    try:
        with open("src/web/html/chartjs.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>chartjs.html não encontrado.</h1>", status_code=404)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Endpoint WebSocket para clientes se conectarem."""
    await manager.connect(websocket)
    print("Novo cliente conectado via WebSocket.")
    try:
        while True:
            # Mantém a conexão viva
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Cliente desconectado.")

# Para executar: uvicorn server:app --reload