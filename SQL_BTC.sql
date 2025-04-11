-- Tabela: [BTC].[dbo].[bitcoin_data_analise]

  -- Seleciona colunas específicas e TODAS as linhas da tabela

  SELECT * FROM [BTC].[dbo].[bitcoin_data_analise];

-- 1. Estatísticas Descritivas Básicas

-- Estatísticas gerais (Média, Desvio Padrão, Mínimo, Máximo)
SELECT
    AVG([close]) AS media_preco,
    STDEV([close]) AS desvio_padrao_preco,
    MIN([close]) AS preco_minimo,
    MAX([close]) AS preco_maximo
FROM [BTC].[dbo].[bitcoin_data_analise];

-- Mediana para o preço de fechamento (separado)
SELECT DISTINCT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [close]) OVER () AS mediana_preco
FROM [BTC].[dbo].[bitcoin_data_analise];

-- Estatísticas gerais para o volume (Média, Desvio Padrão, Mínimo, Máximo)
SELECT
    AVG([volume]) AS media_volume,
    STDEV([volume]) AS desvio_padrao_volume,
    MIN([volume]) AS volume_minimo,
    MAX([volume]) AS volume_maximo
FROM [BTC].[dbo].[bitcoin_data_analise];

-- Mediana para o volume (separado) 
SELECT DISTINCT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [volume]) OVER () AS mediana_volume
FROM [BTC].[dbo].[bitcoin_data_analise];

-- Estatísticas gerais para o retorno diário (Média, Desvio Padrão, Mínimo, Máximo)
SELECT
    AVG([retorno_diario]) AS media_retorno_diario,
    STDEV([retorno_diario]) AS desvio_padrao_retorno_diario,
    MIN([retorno_diario]) AS retorno_diario_minimo,
    MAX([retorno_diario]) AS retorno_diario_maximo
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [retorno_diario] IS NOT NULL;

-- Mediana para o retorno diário (separado) 
SELECT DISTINCT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [retorno_diario]) OVER () AS mediana_retorno_diario
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [retorno_diario] IS NOT NULL;

-- Estatísticas gerais para a volatilidade de 30 dias (Média, Desvio Padrão, Mínimo, Máximo)
SELECT
    AVG([volatilidade_30]) AS media_volatilidade,
    STDEV([volatilidade_30]) AS desvio_padrao_volatilidade,
    MIN([volatilidade_30]) AS volatilidade_minima,
    MAX([volatilidade_30]) AS volatilidade_maxima
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [volatilidade_30] IS NOT NULL;

-- Mediana para a volatilidade de 30 dias (separado) 
SELECT DISTINCT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY [volatilidade_30]) OVER () AS mediana_volatilidade
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [volatilidade_30] IS NOT NULL;


-- 2. Contagem de Dias Positivos e Negativos

SELECT
    SUM(CASE WHEN [retorno_diario] > 0 THEN 1 ELSE 0 END) AS dias_positivos,
    SUM(CASE WHEN [retorno_diario] < 0 THEN 1 ELSE 0 END) AS dias_negativos,
    SUM(CASE WHEN [retorno_diario] = 0 THEN 1 ELSE 0 END) AS dias_neutros,
    COUNT([retorno_diario]) AS total_dias_com_retorno,
    COUNT(*) AS total_registros
FROM [BTC].[dbo].[bitcoin_data_analise];


-- 3. Identificando os Dias de Maior e Menor Volume e Volatilidade

-- Dias de maior volume (TOP 5)
SELECT TOP 5 [timestamp], [volume]
FROM [BTC].[dbo].[bitcoin_data_analise]
ORDER BY [volume] DESC;

-- Dias de menor volume (TOP 5)
SELECT TOP 5 [timestamp], [volume]
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [volume] IS NOT NULL
ORDER BY [volume] ASC;

-- Dias de maior volatilidade (TOP 5)
SELECT TOP 5 [timestamp], [volatilidade_30]
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [volatilidade_30] IS NOT NULL
ORDER BY [volatilidade_30] DESC;

-- Dias de menor volatilidade (TOP 5)
SELECT TOP 5 [timestamp], [volatilidade_30]
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [volatilidade_30] IS NOT NULL
ORDER BY [volatilidade_30] ASC;


-- 4. Filtrando por Períodos de Alta/Baixa Volatilidade

-- Períodos de alta volatilidade (acima de um limite, por exemplo, 5 - ajuste conforme necessário)
SELECT [timestamp], [close], [volatilidade_30]
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [volatilidade_30] > 5
ORDER BY [timestamp];

-- Períodos de baixa volatilidade (abaixo de um limite, por exemplo, 1 - ajuste conforme necessário)
SELECT [timestamp], [close], [volatilidade_30]
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [volatilidade_30] < 1
ORDER BY [timestamp];


-- 5. Calculando o Retorno Acumulado em Janelas de Tempo (Rolling)

-- Retorno acumulado em janelas de 7 dias (rolling) - Requer SQL Server 2012+
SELECT
    [timestamp],
    [close],
    -- Calcula o retorno acumulado de 7 dias.
    CASE
        -- Verifica se algum fator de crescimento na janela é não positivo antes de calcular LOG
        WHEN MIN(1 + ([retorno_diario] / 100.0)) OVER (ORDER BY [timestamp] ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) <= 0 THEN NULL
        ELSE EXP(SUM(LOG(1 + ([retorno_diario] / 100.0))) OVER (ORDER BY [timestamp] ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) - 1
    END AS retorno_acumulado_7d
FROM [BTC].[dbo].[bitcoin_data_analise]
WHERE [retorno_diario] IS NOT NULL; -- Necessário para o cálculo LOG


-- 6. Cruzamento de Médias Móveis (Sinalização) - Requer SQL Server 2012+

-- Detectar cruzamentos de alta da MM7 sobre a MM30
SELECT
    t1.[timestamp],
    t1.[close]
FROM [BTC].[dbo].[bitcoin_data_analise] AS t1
INNER JOIN (
    SELECT
        [timestamp],
        LAG([mm_7]) OVER (ORDER BY [timestamp]) AS prev_mm_7,
        LAG([mm_30]) OVER (ORDER BY [timestamp]) AS prev_mm_30
    FROM [BTC].[dbo].[bitcoin_data_analise]
    WHERE [mm_7] IS NOT NULL AND [mm_30] IS NOT NULL
) AS t2 ON t1.[timestamp] = t2.[timestamp]
WHERE t2.prev_mm_7 IS NOT NULL -- Garante que o dia anterior existe
  AND t2.prev_mm_7 < t2.prev_mm_30 -- Condição 1: No dia anterior, a MM7 estava ABAIXO da MM30.
  AND t1.[mm_7] > t1.[mm_30];     -- Condição 2: No dia atual, a MM7 está ACIMA da MM30.


-- Detectar cruzamentos de baixa da MM7 sobre a MM30
SELECT
    t1.[timestamp],
    t1.[close]
FROM [BTC].[dbo].[bitcoin_data_analise] AS t1
INNER JOIN (
    SELECT
        [timestamp],
        LAG([mm_7]) OVER (ORDER BY [timestamp]) AS prev_mm_7,
        LAG([mm_30]) OVER (ORDER BY [timestamp]) AS prev_mm_30
    FROM [BTC].[dbo].[bitcoin_data_analise]
    WHERE [mm_7] IS NOT NULL AND [mm_30] IS NOT NULL
) AS t2 ON t1.[timestamp] = t2.[timestamp]
WHERE t2.prev_mm_7 IS NOT NULL -- Garante que o dia anterior existe
  AND t2.prev_mm_7 > t2.prev_mm_30  -- Dia anterior: MM7 ACIMA da MM30.
  AND t1.[mm_7] < t1.[mm_30];     -- Dia atual: MM7 ABAIXO da MM30.


-- 7. Agrupamento por Mês/Ano

-- Estatísticas mensais
SELECT
    YEAR([timestamp]) AS ano,
    MONTH([timestamp]) AS mes,
    AVG([close]) AS preco_medio,
    MAX([high]) AS maxima_mensal,
    MIN([low]) AS minima_mensal,
    SUM([volume]) AS volume_total,
    STDEV([close]) AS desvio_padrao_mensal,
    AVG([volatilidade_30]) AS volatilidade_media_mensal
FROM [BTC].[dbo].[bitcoin_data_analise]
GROUP BY YEAR([timestamp]), MONTH([timestamp])
ORDER BY ano, mes;


-- 8. Correlação (Fórmula explícita para SQL Server)

-- Calculando a correlação entre 'close' e 'volatilidade_30'
WITH Stats AS (
    SELECT
        AVG([close] * [volatilidade_30]) AS avg_xy,
        AVG([close]) AS avg_x,
        AVG([volatilidade_30]) AS avg_y,
        STDEV([close]) AS stdev_x,
        STDEV([volatilidade_30]) AS stdev_y,
        COUNT(*) AS n
    FROM [BTC].[dbo].[bitcoin_data_analise]
    WHERE [close] IS NOT NULL AND [volatilidade_30] IS NOT NULL
)
SELECT
    CASE
        WHEN stdev_x > 0 AND stdev_y > 0 AND n > 1 THEN (avg_xy - (avg_x * avg_y)) / (stdev_x * stdev_y)
        ELSE NULL
    END AS correlacao_close_volatilidade
FROM Stats;

-- Calculando a correlação entre 'retorno_diario' e 'volatilidade_30'
WITH StatsRetVol AS (
    SELECT
        AVG([retorno_diario] * [volatilidade_30]) AS avg_xy,
        AVG([retorno_diario]) AS avg_x,
        AVG([volatilidade_30]) AS avg_y,
        STDEV([retorno_diario]) AS stdev_x,
        STDEV([volatilidade_30]) AS stdev_y,
        COUNT(*) as n
    FROM [BTC].[dbo].[bitcoin_data_analise]
    WHERE [retorno_diario] IS NOT NULL AND [volatilidade_30] IS NOT NULL
)
SELECT
    CASE
        WHEN stdev_x > 0 AND stdev_y > 0 AND n > 1 THEN (avg_xy - (avg_x * avg_y)) / (stdev_x * stdev_y)
        ELSE NULL
    END AS correlacao_retorno_volatilidade
FROM StatsRetVol;