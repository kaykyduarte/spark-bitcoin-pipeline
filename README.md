# Bitcoin Pipeline com PySpark

Projeto de estudo de engenharia de dados que transforma candles de Bitcoin de um minuto em candles horários, usando Apache Spark e arquivos Parquet.

## Camadas

- **Bronze:** leitura do CSV com schema explícito e gravação em Parquet.
- **Silver:** nomes de colunas em minúsculo e conversão do timestamp Unix em segundos para data e horário, usando UTC.
- **Gold:** agregação horária de abertura, máxima, mínima, fechamento, volume e quantidade de registros.

O projeto inclui verificações de nulos, duplicados, regras OHLCV, contagens e gaps temporais. Na Gold, horas internas devem ter 60 registros; a primeira e a última podem ser parciais.

## Ambiente

- Python 3.12
- Java 21
- PySpark 4.2.0
- Ubuntu via WSL

## Preparação

No terminal Ubuntu, dentro da pasta do projeto:

```bash
python3 -m venv ~/venvs/spark-bitcoin
source ~/venvs/spark-bitcoin/bin/activate
python -m pip install -r requirements-dev.txt

export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python"
```

Disponibilize o CSV de entrada em:

```text
data/raw/btcusd_1-min_data.csv
```

O arquivo deve conter as colunas `Timestamp`, `Open`, `High`, `Low`, `Close` e `Volume`, com o timestamp em segundos Unix. Os dados não estão incluídos no repositório.

## Execução

Com o ambiente virtual ativo, execute na raiz do projeto:

```bash
python -m src.bronze_ingestion
python -m src.validate_bronze
python -m src.silver_transformation
python -m src.validate_silver
python -m src.gold_transformation
```

Os resultados são gravados no sistema de arquivos do Ubuntu:

```text
~/spark-bitcoin-data/
├── bronze/bitcoin_bronze/
├── silver/bitcoin_silver/
└── gold/bitcoin_hourly/
```

Cada execução utiliza `overwrite`, substituindo o snapshot da camada correspondente.

## Testes e qualidade de código

```bash
python -m pytest -v
python -m ruff check src tests
python -m ruff format --check src tests
```

Os testes usam pequenos DataFrames para verificar as transformações e as regras de qualidade, sem depender do CSV completo.
