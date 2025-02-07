  CREATE TABLE IF NOT EXISTS bitcoin_data (
    timestamp timestamp,
    open decimal,
    high decimal,
    low decimal,
    close decimal,
    volume decimal,
    retorno_diario decimal,
    retorno_acumulado decimal,
    mm_7 decimal,
    mm_30 decimal,
    mm_200 decimal,
    volatilidade_30 decimal,
    PRIMARY KEY (timestamp)
);