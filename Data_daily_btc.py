import sys
import os
import requests
import json
import csv
import pandas as pd
import datetime
import ssl
import uuid
from cassandra.cluster import Cluster
from cassandra.protocol import SyntaxException, InvalidRequest

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

class BitcoinRelator(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("Bitcoin Relator - Binance")
        self.geometry("1000x700")

        self.historico_precos = []
        self.nome_arquivo_csv = ""
        self.intervalo_atualizacao = 30 * 60  # 30 minutos
        self.tempo_restante = self.intervalo_atualizacao
        self.atualizando = False
        self.diretorio_relatorios = os.path.expanduser("~")  # Diretório padrão inicial
        self.cassandra_session = None
        self.cassandra_cluster = None
        self.timer_id = None
        self.sequential_id = 1 # This will be updated by load_last_sequential_id

        self.init_cassandra()
        self.load_last_sequential_id()
        self.init_ui()
        self.atualizar_preco()
        self.iniciar_contagem_regressiva()
        self.protocol("WM_DELETE_WINDOW", self.close_app)

    def init_cassandra(self):
        try:
            self.cassandra_cluster = Cluster(['127.0.0.1'])
            self.cassandra_session = self.cassandra_cluster.connect()
            self.cassandra_session.execute("""
                CREATE KEYSPACE IF NOT EXISTS btc
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """)
            self.cassandra_session.set_keyspace('btc')
        except Exception as e:
            messagebox.showerror("Erro de Conexão com Cassandra", f"Não foi possível conectar ou criar o keyspace no Cassandra: {e}")
            self.cassandra_session = None

    def load_last_sequential_id(self):
        if not self.cassandra_session:
            return
        try:
            now = datetime.datetime.now()
            today_str = now.strftime('%Y_%m_%d')
            table_name = f"btc_{today_str}"
            day_partition = now.strftime('%Y-%m-%d')

            # Garante que a tabela do dia exista antes de consultá-la
            query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                day_partition TEXT,
                sequential_id INT,
                id UUID,
                dia_tempo TIMESTAMP,
                valor DECIMAL,
                volume_buy DECIMAL,
                volume_sell DECIMAL,
                PRIMARY KEY (day_partition, sequential_id)
            ) WITH CLUSTERING ORDER BY (sequential_id ASC);"""
            self.cassandra_session.execute(query)

            # Busca o maior ID sequencial na tabela do dia
            query = f"SELECT MAX(sequential_id) as max_id FROM {table_name} WHERE day_partition = %s"
            result = self.cassandra_session.execute(query, (day_partition,))
            row = result.one()
            if row and row.max_id is not None:
                self.sequential_id = row.max_id + 1
            else:
                self.sequential_id = 1
            print(f"ID sequencial inicializado em: {self.sequential_id}")
        except Exception as e:
            print(f"Não foi possível carregar o último ID sequencial, iniciando em 1. Erro: {e}")
            self.sequential_id = 1

    def close_app(self):
        if self.timer_id:
            self.after_cancel(self.timer_id)
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
        self.destroy()

    def init_ui(self):
        # Variáveis de String para os Labels
        self.preco_var = tk.StringVar(value="Bitcoin Price: Waiting...")
        self.variacao_var = tk.StringVar(value="Change: Waiting...")
        self.tempo_restante_var = tk.StringVar(value=self.formatar_tempo(self.tempo_restante))
        self.volume_compras_var = tk.StringVar(value="Buy Volume (USD est.): Waiting...")
        self.volume_vendas_var = tk.StringVar(value="Sell Volume (USD est.): Waiting...")
        self.diretorio_var = tk.StringVar(value=self.diretorio_relatorios)

        # Menu Bar
        menubar = tk.Menu(self)
        self.config(menu=menubar)
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="New", command=self.novo_historico)
        file_menu.add_command(label="Save", command=self.salvar_historico)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.close_app)

        # Abas
        self.tabs = ttk.Notebook(self)
        self.tab_historico = ttk.Frame(self.tabs)
        self.tab_relatorio = ttk.Frame(self.tabs)
        self.tabs.add(self.tab_historico, text="History")
        self.tabs.add(self.tab_relatorio, text="Report")
        self.tabs.pack(expand=True, fill='both', padx=10, pady=10)

        # --- Layout da aba Histórico ---
        hist_frame = ttk.Frame(self.tab_historico)
        hist_frame.pack(expand=True, fill='both', padx=5, pady=5)

        ttk.Label(hist_frame, textvariable=self.preco_var).pack(anchor='w')
        ttk.Label(hist_frame, textvariable=self.variacao_var).pack(anchor='w')
        ttk.Label(hist_frame, textvariable=self.volume_compras_var).pack(anchor='w')
        ttk.Label(hist_frame, textvariable=self.volume_vendas_var).pack(anchor='w')

        # Tree Widget
        self.tree_historico = ttk.Treeview(hist_frame, columns=("ID", "DateTime", "Price", "Change", "Buy Volume", "Sell Volume"), show='headings')
        self.tree_historico.heading("ID", text="ID")
        self.tree_historico.heading("DateTime", text="DateTime")
        self.tree_historico.heading("Price", text="Price")
        self.tree_historico.heading("Change", text="Change (%)")
        self.tree_historico.heading("Buy Volume", text="Buy Volume (USD)")
        self.tree_historico.heading("Sell Volume", text="Sell Volume (USD)")
        self.tree_historico.pack(expand=True, fill='both', pady=5)

        # Layout do tempo
        tempo_frame = ttk.Frame(hist_frame)
        tempo_frame.pack(fill='x', pady=5)
        ttk.Label(tempo_frame, text="Time Remaining: ").pack(side='left')
        ttk.Label(tempo_frame, textvariable=self.tempo_restante_var).pack(side='left')

        # Layout dos intervalos
        interval_frame = ttk.Frame(hist_frame)
        interval_frame.pack(fill='x', pady=5)
        ttk.Button(interval_frame, text="10 Seconds", command=lambda: self.definir_intervalo(10)).pack(side='left', padx=2)
        ttk.Button(interval_frame, text="1 Minute", command=lambda: self.definir_intervalo(1 * 60)).pack(side='left', padx=2)
        ttk.Button(interval_frame, text="10 Minutes", command=lambda: self.definir_intervalo(10 * 60)).pack(side='left', padx=2)
        ttk.Button(interval_frame, text="1 Hour", command=lambda: self.definir_intervalo(60 * 60)).pack(side='left', padx=2)

        ttk.Button(hist_frame, text="Update Now", command=self.atualizar_preco).pack(pady=5)

        # --- Layout da aba Relatório ---
        relatorio_frame = ttk.Frame(self.tab_relatorio)
        relatorio_frame.pack(expand=True, fill='both', padx=5, pady=5)

        diretorio_frame = ttk.Frame(relatorio_frame)
        diretorio_frame.pack(fill='x', pady=5)
        ttk.Label(diretorio_frame, text="Reports Directory:").pack(side='left')
        ttk.Entry(diretorio_frame, textvariable=self.diretorio_var, width=50).pack(side='left', expand=True, fill='x', padx=5)
        ttk.Button(diretorio_frame, text="Select Directory", command=self.selecionar_diretorio).pack(side='left')

        ttk.Button(relatorio_frame, text="Generate Report", command=self.gerar_relatorio).pack(pady=5)
        self.text_relatorio = tk.Text(relatorio_frame, wrap='word')
        self.text_relatorio.pack(expand=True, fill='both', pady=5)
        ttk.Button(relatorio_frame, text="Save Report to CSV", command=self.salvar_relatorio_csv).pack(pady=5)
        ttk.Button(relatorio_frame, text="Registrar no Cassandra", command=self.salvar_no_cassandra).pack(pady=5)

    def obter_preco_e_volume_bitcoin(self):
        try:
            preco_url = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
            preco_response = requests.get(preco_url)
            preco_response.raise_for_status()
            preco_data = preco_response.json()
            preco = float(preco_data['price'])

            orderbook_url = 'https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000'
            orderbook_response = requests.get(orderbook_url)
            orderbook_response.raise_for_status()
            orderbook_data = orderbook_response.json()

            volume_compras = sum([float(price) * float(quantity) for price, quantity in orderbook_data['bids']])
            volume_vendas = sum([float(price) * float(quantity) for price, quantity in orderbook_data['asks']])

            return preco, volume_compras, volume_vendas
        except requests.exceptions.RequestException as e:
            print(f"Request error: {type(e).__name__} - {e}")
            return None, None, None
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Error processing API response: {type(e).__name__} - {e}")
            return None, None, None
        except ssl.SSLError as e:
            print(f"SSL Error: {type(e).__name__} - {e}")
            return None, None, None
        except Exception as e:
            print(f"Unexpected error: {type(e).__name__} - {e}")
            return None, None, None

    def atualizar_preco(self):
        if self.atualizando:
            return
        self.atualizando = True

        preco, volume_compras, volume_vendas = self.obter_preco_e_volume_bitcoin()

        if preco is not None and volume_compras is not None and volume_vendas is not None:
            agora = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            variacao = self.calcular_variacao(preco)

            self.preco_var.set(f"Bitcoin Price: ${preco:.2f}")
            self.variacao_var.set(f"Change: {variacao:.2f}%")
            self.volume_compras_var.set(f"Buy Volume (USD est.): ${volume_compras:.2f}")
            self.volume_vendas_var.set(f"Sell Volume (USD est.): ${volume_vendas:.2f}")

            self.historico_precos.append((agora, preco, variacao, volume_compras, volume_vendas))
            self.atualizar_historico()

            if self.cassandra_session:
                self.salvar_dados_cassandra_auto(preco, volume_compras, volume_vendas)

            if self.nome_arquivo_csv:
                self.salvar_historico()
        else:
            self.preco_var.set("Error getting Bitcoin price.")
            self.variacao_var.set("")
            self.volume_compras_var.set("")
            self.volume_vendas_var.set("")
        self.atualizando = False

    def salvar_dados_cassandra_auto(self, preco, volume_compras, volume_vendas):
        if not self.cassandra_session:
            return
        try:
            now = datetime.datetime.now()
            today_str = now.strftime('%Y_%m_%d')
            table_name = f"btc_{today_str}"
            day_partition = now.strftime('%Y-%m-%d')
            query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                day_partition TEXT,
                sequential_id INT,
                id UUID,
                dia_tempo TIMESTAMP,
                valor DECIMAL,
                volume_buy DECIMAL,
                volume_sell DECIMAL,
                PRIMARY KEY (day_partition, sequential_id)
            ) WITH CLUSTERING ORDER BY (sequential_id ASC);"""
            self.cassandra_session.execute(query)

            insert_now = now.replace(microsecond=0)
            query = f"INSERT INTO {table_name} (day_partition, sequential_id, id, dia_tempo, valor, volume_buy, volume_sell) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            self.cassandra_session.execute(query, (day_partition, self.sequential_id, uuid.uuid4(), insert_now, preco, volume_compras, volume_vendas))
            self.sequential_id += 1
        except (SyntaxException, InvalidRequest) as e:
            messagebox.showwarning("Erro de Query Cassandra", f"Erro ao salvar dados automaticamente no Cassandra: {e}")
        except Exception as e:
            messagebox.showwarning("Erro Inesperado", f"Ocorreu um erro inesperado ao salvar no Cassandra: {e}")

    def calcular_variacao(self, preco_atual):
        if not self.historico_precos:
            return 0
        _, preco_anterior, _, _, _ = self.historico_precos[-1]
        if preco_anterior == 0:
            return 0
        return ((preco_atual - preco_anterior) / preco_anterior) * 100

    def atualizar_historico(self):
        for i in self.tree_historico.get_children():
            self.tree_historico.delete(i)
        for i, (data_str, preco, variacao, volume_compras, volume_vendas) in enumerate(self.historico_precos, 1):
            try:
                data_obj = datetime.datetime.strptime(data_str, '%Y-%m-%d %H:%M:%S')
                data_formatada = data_obj.strftime('%m-%d %H:%M:%S')
            except ValueError:
                data_formatada = data_str
            self.tree_historico.insert('', 'end', values=(i, data_formatada, f"${preco:.2f}", f"{variacao:.2f}%", f"${volume_compras:.2f}", f"${volume_vendas:.2f}"))

    def gerar_relatorio(self):
        if not self.historico_precos:
            self.text_relatorio.delete('1.0', tk.END)
            self.text_relatorio.insert(tk.END, "No data available to generate a report.")
            return

        df = pd.DataFrame(self.historico_precos, columns=['DateTime', 'Price', 'Change', 'Buy Volume (USD)', 'Sell Volume (USD)'])
        df['DateTime'] = pd.to_datetime(df['DateTime']).dt.strftime('%m-%d %H:%M:%S')
        media = df['Price'].mean()
        maximo = df['Price'].max()
        minimo = df['Price'].min()
        desvio_padrao = df['Price'].std()
        volume_medio_compras = df['Buy Volume (USD)'].mean()
        volume_medio_vendas = df['Sell Volume (USD)'].mean()

        relatorio = f"Bitcoin Price Report:\n\n"
        relatorio += f"Average Price: ${media:.2f}\n"
        relatorio += f"Maximum Price: ${maximo:.2f}\n"
        relatorio += f"Minimum Price: ${minimo:.2f}\n"
        relatorio += f"Standard Deviation: ${desvio_padrao:.2f}\n"
        relatorio += f"Average Buy Volume (USD est.): ${volume_medio_compras:.2f}\n"
        relatorio += f"Average Sell Volume (USD est.): ${volume_medio_vendas:.2f}\n\n"
        relatorio += "Detailed History:\n"
        relatorio += df.to_string()

        self.text_relatorio.delete('1.0', tk.END)
        self.text_relatorio.insert(tk.END, relatorio)

    def salvar_no_cassandra(self):
        if not self.cassandra_session:
            messagebox.showerror("Erro de Conexão", "Não há conexão com o Cassandra.")
            return
        if not self.historico_precos:
            messagebox.showwarning("Sem Dados", "Não há dados para registrar no Cassandra.")
            return
        try:
            now = datetime.datetime.now()
            today_str = now.strftime('%Y_%m_%d')
            table_name = f'btc_{today_str}'
            day_partition = now.strftime('%Y-%m-%d')
            query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                day_partition TEXT,
                sequential_id INT,
                id UUID,
                dia_tempo TIMESTAMP,
                valor DECIMAL,
                volume_buy DECIMAL,
                volume_sell DECIMAL,
                PRIMARY KEY (day_partition, sequential_id)
            ) WITH CLUSTERING ORDER BY (sequential_id ASC);"""
            self.cassandra_session.execute(query)
            _, preco, _, volume_compras, volume_vendas = self.historico_precos[-1]
            insert_now = now.replace(microsecond=0)
            query = f"INSERT INTO {table_name} (day_partition, sequential_id, id, dia_tempo, valor, volume_buy, volume_sell) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            self.cassandra_session.execute(query, (day_partition, self.sequential_id, uuid.uuid4(), insert_now, preco, volume_compras, volume_vendas))
            self.sequential_id += 1
            messagebox.showinfo("Sucesso", f"Dados registrados na tabela '{table_name}' do Cassandra.")
        except (SyntaxException, InvalidRequest) as e:
            messagebox.showerror("Erro de Query", f"Erro ao criar tabela/inserir dados no Cassandra: {e}")
        except Exception as e:
            messagebox.showerror("Erro Inesperado", f"Ocorreu um erro inesperado: {e}")

    def selecionar_diretorio(self):
        diretorio = filedialog.askdirectory(title="Select Reports Directory")
        if diretorio:
            self.diretorio_relatorios = diretorio
            self.diretorio_var.set(diretorio)

    def salvar_relatorio_csv(self):
        now = datetime.datetime.now()
        date_time_str = now.strftime("%Y%m%d_%H%M%S")
        filename = f"btc_data_{date_time_str}.csv"
        report_dir = self.diretorio_relatorios
        os.makedirs(report_dir, exist_ok=True)
        filepath = os.path.join(report_dir, filename)
        df = pd.DataFrame(self.historico_precos, columns=['DateTime', 'Price', 'Change', 'Buy Volume (USD)', 'Sell Volume (USD)'])
        try:
            df.to_csv(filepath, index=False)
            print(f"Report saved to {filepath}")
        except Exception as e:
            print(f"Error saving report: {e}")

    def novo_historico(self):
        self.historico_precos = []
        self.nome_arquivo_csv = ""
        self.atualizar_historico()
        self.text_relatorio.delete('1.0', tk.END)
        print("New history created.")

    def salvar_historico(self):
        if not self.nome_arquivo_csv:
            file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")], title="Save CSV File")
            if file_path:
                self.nome_arquivo_csv = file_path
            else:
                return
        try:
            with open(self.nome_arquivo_csv, 'w', newline='') as arquivo_csv:
                escritor_csv = csv.writer(arquivo_csv)
                escritor_csv.writerow(["DateTime", "Price", "Change (%)", "Buy Volume (USD)", "Sell Volume (USD)"])
                for row in self.historico_precos:
                    escritor_csv.writerow(row)
            print(f"History saved to {self.nome_arquivo_csv}")
        except Exception as e:
            print(f"Error saving file: {e}")

    def definir_intervalo(self, intervalo_segundos):
        self.intervalo_atualizacao = intervalo_segundos
        if self.timer_id:
            self.after_cancel(self.timer_id)
        self.iniciar_contagem_regressiva()
        print(f"Update interval set to {intervalo_segundos} seconds.")

    def iniciar_contagem_regressiva(self):
        self.tempo_restante = self.intervalo_atualizacao
        self.contagem_regressiva()

    def contagem_regressiva(self):
        self.tempo_restante_var.set(self.formatar_tempo(self.tempo_restante))
        if self.tempo_restante <= 0:
            self.atualizar_preco()
            self.tempo_restante = self.intervalo_atualizacao
        else:
            self.tempo_restante -= 1
        self.timer_id = self.after(1000, self.contagem_regressiva)

    def formatar_tempo(self, segundos):
        minutos, segundos = divmod(segundos, 60)
        return f"{minutos:02}:{segundos:02}"

if __name__ == '__main__':
    app = BitcoinRelator()
    app.mainloop()