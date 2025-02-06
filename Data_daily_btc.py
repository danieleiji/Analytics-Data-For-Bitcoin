import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QLabel, QTreeWidget,
                             QTreeWidgetItem, QVBoxLayout, QWidget, QPushButton,
                             QFileDialog, QMenuBar, QMenu, QHBoxLayout, QAction,
                             QTextEdit, QTabWidget, QLineEdit)
from PyQt5.QtCore import QTimer, QDateTime, Qt
import requests
import json
import csv
import pandas as pd
import datetime
import ssl  # Importe o módulo ssl

class BitcoinRelator(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Bitcoin Relator - Binance")
        self.setGeometry(100, 100, 1000, 700)

        self.historico_precos = []
        self.nome_arquivo_csv = ""
        self.intervalo_atualizacao = 30 * 60  # 30 minutos
        self.tempo_restante = self.intervalo_atualizacao
        self.atualizando = False
        self.diretorio_relatorios = os.path.expanduser("~") #Diretório padrão inicial

        self.init_ui()
        self.atualizar_preco()
        self.iniciar_contagem_regressiva()

    def init_ui(self):
        # Labels
        self.label_preco = QLabel("Bitcoin Price: Waiting...")
        self.label_variacao = QLabel("Change: Waiting...")
        self.label_tempo_restante = QLabel("Time Remaining: ")
        self.label_tempo_restante_valor = QLabel(self.formatar_tempo(self.tempo_restante))
        self.label_volume_compras = QLabel("Buy Volume (USD est.): Waiting...")
        self.label_volume_vendas = QLabel("Sell Volume (USD est.): Waiting...")

        # Tree Widget
        self.tree_historico = QTreeWidget()
        self.tree_historico.setHeaderLabels(["DateTime", "Price", "Change (%)", "Buy Volume (USD)", "Sell Volume (USD)"])

        # Text Edit para relatório
        self.text_relatorio = QTextEdit()

        # Botão para gerar relatório
        self.btn_gerar_relatorio = QPushButton("Generate Report")
        self.btn_gerar_relatorio.clicked.connect(self.gerar_relatorio)

        # Botão para salvar o relatório
        self.btn_salvar_relatorio = QPushButton("Save Report to CSV")  # Renamed button
        self.btn_salvar_relatorio.clicked.connect(self.salvar_relatorio_csv)  # Changed connection

        # Input de diretório
        self.label_diretorio = QLabel("Reports Directory:")
        self.input_diretorio = QLineEdit(self.diretorio_relatorios)
        self.btn_selecionar_diretorio = QPushButton("Select Directory")
        self.btn_selecionar_diretorio.clicked.connect(self.selecionar_diretorio)

        # Abas
        self.tabs = QTabWidget()
        self.tab_historico = QWidget()
        self.tab_relatorio = QWidget()

        self.tabs.addTab(self.tab_historico, "History")
        self.tabs.addTab(self.tab_relatorio, "Report")

        # Layout da aba Histórico
        historico_layout = QVBoxLayout()
        historico_layout.addWidget(self.label_preco)
        historico_layout.addWidget(self.label_variacao)
        historico_layout.addWidget(self.label_volume_compras)
        historico_layout.addWidget(self.label_volume_vendas)
        historico_layout.addWidget(self.tree_historico)

        tempo_layout = QHBoxLayout()
        tempo_layout.addWidget(self.label_tempo_restante)
        tempo_layout.addWidget(self.label_tempo_restante_valor)
        historico_layout.addLayout(tempo_layout)

        interval_layout = QHBoxLayout()
        btn_1min = QPushButton("1 Minute")
        btn_10min = QPushButton("10 Minutes")
        btn_30min = QPushButton("30 Minutes")
        btn_1hour = QPushButton("1 Hour")

        btn_1min.clicked.connect(lambda: self.definir_intervalo(1 * 60))
        btn_10min.clicked.connect(lambda: self.definir_intervalo(10 * 60))
        btn_30min.clicked.connect(lambda: self.definir_intervalo(30 * 60))
        btn_1hour.clicked.connect(lambda: self.definir_intervalo(60 * 60))

        interval_layout.addWidget(btn_1min)
        interval_layout.addWidget(btn_10min)
        interval_layout.addWidget(btn_30min)
        interval_layout.addWidget(btn_1hour)
        historico_layout.addLayout(interval_layout)

        self.btn_atualizar = QPushButton("Update Now")
        self.btn_atualizar.clicked.connect(self.atualizar_preco)
        historico_layout.addWidget(self.btn_atualizar)

        self.tab_historico.setLayout(historico_layout)

        # Layout da aba Relatório
        relatorio_layout = QVBoxLayout()

        # Adiciona o input de diretório e botão de seleção
        diretorio_layout = QHBoxLayout()
        diretorio_layout.addWidget(self.label_diretorio)
        diretorio_layout.addWidget(self.input_diretorio)
        diretorio_layout.addWidget(self.btn_selecionar_diretorio)
        relatorio_layout.addLayout(diretorio_layout)

        relatorio_layout.addWidget(self.btn_gerar_relatorio)
        relatorio_layout.addWidget(self.text_relatorio)
        relatorio_layout.addWidget(self.btn_salvar_relatorio)
        self.tab_relatorio.setLayout(relatorio_layout)

        # Menu Bar
        menubar = self.menuBar()
        file_menu = menubar.addMenu("File")

        new_action = QAction("New", self)
        save_action = QAction("Save", self)
        exit_action = QAction("Exit", self)

        new_action.triggered.connect(self.novo_historico)
        save_action.triggered.connect(self.salvar_historico)
        exit_action.triggered.connect(self.close)

        file_menu.addAction(new_action)
        file_menu.addAction(save_action)
        file_menu.addSeparator()
        file_menu.addAction(exit_action)

        # Central Widget
        central_widget = QWidget()
        main_layout = QVBoxLayout()
        main_layout.addWidget(self.tabs)  # Adiciona as abas ao layout principal
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

    def obter_preco_e_volume_bitcoin(self):
        try:
            # Obter preço
            preco_url = 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
            preco_response = requests.get(preco_url)
            preco_response.raise_for_status()
            preco_data = preco_response.json()
            preco = float(preco_data['price'])

            # Obter book de ordens
            orderbook_url = 'https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=50'  # Limite de 50 ordens
            orderbook_response = requests.get(orderbook_url)
            orderbook_response.raise_for_status()
            orderbook_data = orderbook_response.json()

            # Calcular volumes estimados
            volume_compras = sum([float(price) * float(quantity) for price, quantity in orderbook_data['bids']])  # bids = ordens de compra
            volume_vendas = sum([float(price) * float(quantity) for price, quantity in orderbook_data['asks']])  # asks = ordens de venda

            return preco, volume_compras, volume_vendas

        except requests.exceptions.RequestException as e:
            print(f"Request error: {type(e).__name__} - {e}")  # Imprime o tipo da exceção
            return None, None, None
        except (KeyError, json.JSONDecodeError) as e:
            print(f"Error processing API response: {type(e).__name__} - {e}") # Imprime o tipo da exceção
            return None, None, None
        except ssl.SSLError as e:  # Captura erros SSL específicos
            print(f"SSL Error: {type(e).__name__} - {e}")
            return None, None, None
        except Exception as e:
            print(f"Unexpected error: {type(e).__name__} - {e}") # Captura qualquer outro erro
            return None, None, None

    def atualizar_preco(self):
        if self.atualizando:
            return
        self.atualizando = True

        preco, volume_compras, volume_vendas = self.obter_preco_e_volume_bitcoin()

        if preco is not None and volume_compras is not None and volume_vendas is not None:
            agora = QDateTime.currentDateTime().toString(Qt.ISODate)
            variacao = self.calcular_variacao(preco)

            self.label_preco.setText(f"Bitcoin Price: ${preco:.2f}")
            self.label_variacao.setText(f"Change: {variacao:.2f}%")
            self.label_volume_compras.setText(f"Buy Volume (USD est.): ${volume_compras:.2f}")
            self.label_volume_vendas.setText(f"Sell Volume (USD est.): ${volume_vendas:.2f}")

            self.historico_precos.append((agora, preco, variacao, volume_compras, volume_vendas))
            self.atualizar_historico()

            if self.nome_arquivo_csv:
                self.salvar_historico()
        else:
            self.label_preco.setText("Error getting Bitcoin price.")
            self.label_variacao.setText("")
            self.label_volume_compras.setText("")
            self.label_volume_vendas.setText("")
        self.atualizando = False

    def calcular_variacao(self, preco_atual):
        if not self.historico_precos:
            return 0

        _, preco_anterior, _, _, _ = self.historico_precos[-1] if self.historico_precos else (None, 0, None, None, None)
        if preco_anterior == 0:
            return 0

        variacao = ((preco_atual - preco_anterior) / preco_anterior) * 100
        return variacao

    def atualizar_historico(self):
        self.tree_historico.clear()
        for data, preco, variacao, volume_compras, volume_vendas in self.historico_precos:
            item = QTreeWidgetItem([data, f"${preco:.2f}", f"{variacao:.2f}%", f"${volume_compras:.2f}", f"${volume_vendas:.2f}"])
            self.tree_historico.addTopLevelItem(item)

    def gerar_relatorio(self):
        if not self.historico_precos:
            self.text_relatorio.setText("No data available to generate a report.")
            return

        df = pd.DataFrame(self.historico_precos, columns=['DateTime', 'Price', 'Change', 'Buy Volume (USD)', 'Sell Volume (USD)'])

        # Calcular estatísticas
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

        self.text_relatorio.setText(relatorio)

    def selecionar_diretorio(self):
        diretorio = QFileDialog.getExistingDirectory(self, "Select Reports Directory")
        if diretorio:
            self.diretorio_relatorios = diretorio
            self.input_diretorio.setText(diretorio)

    def salvar_relatorio_csv(self):  # Changed function name
        # Get current date and time for filename
        now = datetime.datetime.now()
        date_time_str = now.strftime("%Y%m%d_%H%M%S")

        # Create the filename
        filename = f"btc_data_{date_time_str}.csv" #CSV Extension

        # Get the directory from the input
        report_dir = self.diretorio_relatorios

        os.makedirs(report_dir, exist_ok=True)  # Create the directory if it doesn't exist
        filepath = os.path.join(report_dir, filename)

        # Convert data to DataFrame
        df = pd.DataFrame(self.historico_precos, columns=['DateTime', 'Price', 'Change', 'Buy Volume (USD)', 'Sell Volume (USD)'])

        # Save to CSV
        try:
            df.to_csv(filepath, index=False) #No Index
            print(f"Report saved to {filepath}")
        except Exception as e:
            print(f"Error saving report: {e}")


    def novo_historico(self):
        self.historico_precos = []
        self.nome_arquivo_csv = ""
        self.atualizar_historico()
        self.text_relatorio.clear()
        print("New history created.")

    def salvar_historico(self):
        if not self.nome_arquivo_csv:
            file_path, _ = QFileDialog.getSaveFileName(self, "Save CSV File", "", "CSV Files (*.csv);;All Files (*.*)")
            if file_path:
                self.nome_arquivo_csv = file_path
            else:
                return

        try:
            with open(self.nome_arquivo_csv, 'w', newline='') as arquivo_csv:
                escritor_csv = csv.writer(arquivo_csv)
                escritor_csv.writerow(["DateTime", "Price", "Change (%)", "Buy Volume (USD)", "Sell Volume (USD)"])
                for data, preco, variacao, volume_compras, volume_vendas in self.historico_precos:
                    escritor_csv.writerow([data, preco, variacao, volume_compras, volume_vendas])
            print(f"History saved to {self.nome_arquivo_csv}")
        except Exception as e:
            print(f"Error saving file: {e}")

    def definir_intervalo(self, intervalo_segundos):
        self.intervalo_atualizacao = intervalo_segundos
        self.tempo_restante = self.intervalo_atualizacao
        self.timer.stop()
        self.iniciar_contagem_regressiva()
        print(f"Update interval set to {intervalo_segundos // 60} minutes.")

    def iniciar_contagem_regressiva(self):
        self.tempo_restante = self.intervalo_atualizacao
        self.label_tempo_restante_valor.setText(self.formatar_tempo(self.tempo_restante))
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.contagem_regressiva)
        self.timer.start(1000)

    def contagem_regressiva(self):
        self.tempo_restante -= 1
        self.label_tempo_restante_valor.setText(self.formatar_tempo(self.tempo_restante))

        if self.tempo_restante <= 0:
            self.timer.stop()
            self.atualizar_preco()
            self.iniciar_contagem_regressiva()

    def formatar_tempo(self, segundos):
        minutos, segundos = divmod(segundos, 60)
        return f"{minutos:02}:{segundos:02}"

if __name__ == '__main__':
    app = QApplication(sys.argv)
    relator = BitcoinRelator()
    relator.show()
    sys.exit(app.exec_())