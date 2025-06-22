import tkinter as tk
from tkinter import messagebox
import random
from datetime import datetime
import uuid
from cassandra.cluster import Cluster
from cassandra.protocol import SyntaxException, InvalidRequest

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Gerador de Dados Aleatórios para Cassandra")

        # Configuração da conexão com o Cassandra
        try:
            self.cluster = Cluster(['127.0.0.1'])  # Conecta ao Cassandra no localhost
            self.session = self.cluster.connect()
            # Define o keyspace a ser usado
            self.session.set_keyspace('btc')
        except Exception as e:
            messagebox.showerror("Erro de Conexão", f"Não foi possível conectar ao Cassandra: {e}")
            self.root.destroy()
            return

        # Interface gráfica
        self.label = tk.Label(root, text="Pressione o botão para gerar e salvar dados.")
        self.label.pack(pady=20)

        self.generate_button = tk.Button(root, text="Gerar e Salvar", command=self.generate_and_save_data)
        self.generate_button.pack(pady=10)

        # Garante que a conexão seja fechada ao fechar a janela
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def generate_and_save_data(self):
        """
        Gera um número aleatório e a data/hora atual e os salva no Cassandra.
        """
        try:
            # Gera os dados
            random_num = random.randint(1, 1000)
            current_time = datetime.now()
            unique_id = uuid.uuid4() # Gera um ID único para a chave primária

            # Prepara e executa a query de inserção
            query = "INSERT INTO random_data (id, random_number, event_time) VALUES (%s, %s, %s)"
            self.session.execute(query, (unique_id, random_num, current_time))

            # Mostra uma mensagem de sucesso
            messagebox.showinfo("Sucesso", f"Dados inseridos com sucesso!\nNúmero: {random_num}\nData: {current_time}")

        except (SyntaxException, InvalidRequest) as e:
            messagebox.showerror("Erro de Query", f"Erro ao inserir dados no Cassandra (verifique o keyspace e a tabela): {e}")
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro inesperado: {e}")

    def on_closing(self):
        """
        Fecha a conexão com o Cassandra antes de fechar a aplicação.
        """
        if self.cluster:
            self.cluster.shutdown()
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.geometry("800x600")
    root.mainloop()