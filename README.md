# Analytics-Data-For-Bitcon

Uma plataforma completa para coleta, análise e visualização de dados do Bitcoin, utilizando dados em tempo real e históricos da exchange Binance.

## Visão Geral

Este projeto oferece um ecossistema robusto para entusiastas e analistas de criptomoedas. Ele combina uma aplicação de desktop para monitoramento em tempo real, um servidor web para visualização de dados históricos e um conjunto de scripts para coleta e análise aprofundada de dados do Bitcoin (BTC).

<!-- Placeholder para GIF/Vídeo do projeto em ação -->

## ✨ Funcionalidades

- **Coleta de Dados**: Scripts para buscar dados históricos e em tempo real do par BTC/USDT diretamente da API da Binance.
- **Aplicação Desktop (GUI)**: Uma interface gráfica construída com Tkinter que exibe o preço atual do Bitcoin, variação, volumes de compra/venda e armazena um fluxo de dados contínuo no Cassandra.
- **Servidor Web**: Um backend FastAPI que serve os dados armazenados para uma interface web interativa.
- **Visualização Web**: Frontend com HTML, CSS e JavaScript puro, permitindo a visualização de séries temporais com as bibliotecas Plotly.js e Chart.js.
- **Armazenamento Híbrido**: 
  - **Cassandra**: Utilizado para armazenar o fluxo de dados de alta frequência da aplicação desktop.
  - **SQL Server**: Utilizado para análises mais complexas e consultas SQL sobre dados processados.
  - **Arquivos CSV**: Usado para cache de dados brutos e armazenamento de dados processados.
- **Análise de Dados**: Scripts em Python com Pandas para cálculos de médias móveis, volatilidade, retornos, e scripts SQL para análises avançadas.

## 🛠️ Tecnologias Utilizadas

- **Backend**: Python, FastAPI
- **Frontend**: HTML, CSS, JavaScript, Plotly.js, Chart.js
- **Desktop**: Python, Tkinter
- **Bancos de Dados**: Apache Cassandra, SQL Server
- **Análise de Dados**: Pandas, NumPy
- **Fonte de Dados**: Binance API

## 📂 Estrutura do Projeto
