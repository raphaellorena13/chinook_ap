import typer
import sqlite3
import psycopg2
from typing import List

app = typer.Typer()

def obter_tabelas_sqlite(caminho_sqlite: str) -> List[str]:
    """
    Retorna a lista de tabelas presentes em um arquivo SQLite.
    """
    conn = sqlite3.connect(caminho_sqlite)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tabelas = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tabelas

def ler_dados_tabela_sqlite(caminho_sqlite: str, tabela: str):
    """
    Lê todas as linhas e nomes de colunas de uma tabela SQLite.
    """
    conn = sqlite3.connect(caminho_sqlite)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {tabela}")
    linhas = cursor.fetchall()
    colunas = [description[0] for description in cursor.description]
    conn.close()
    return colunas, linhas

def criar_tabela_postgres(pg_conn, tabela: str, colunas: List[str], schema: str = 'public'):
    """
    Cria uma tabela no Postgres com todas as colunas como TEXT.
    Se já existir, apaga e recria.
    """
    cursor = pg_conn.cursor()
    colunas_def = ', '.join([f'"{col}" TEXT' for col in colunas])
    print(f"\n--- Criando tabela '{schema}.{tabela}' no Postgres ---")
    print(f"Colunas: {colunas}")
    print(f"Definição: {colunas_def}")
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
    cursor.execute(f'DROP TABLE IF EXISTS "{schema}"."{tabela}";')
    cursor.execute(f'CREATE TABLE "{schema}"."{tabela}" ({colunas_def});')
    pg_conn.commit()
    cursor.close()

def inserir_dados_postgres(pg_conn, tabela: str, colunas: List[str], linhas: list, schema: str = 'public'):
    """
    Insere os dados lidos do SQLite na tabela Postgres criada.
    """
    cursor = pg_conn.cursor()
    cols = ', '.join([f'"{col}"' for col in colunas])
    placeholders = ', '.join(['%s'] * len(colunas))
    sql = f'INSERT INTO "{schema}"."{tabela}" ({cols}) VALUES ({placeholders})'
    print(f"Inserindo {len(linhas)} linhas na tabela {schema}.{tabela}...")
    cursor.executemany(sql, linhas)
    pg_conn.commit()
    print("Inserção concluída.")
    cursor.close()

@app.command()
def migrar(
    sqlite_path: str = typer.Option(..., help="Caminho para o arquivo SQLite"),
    pg_host: str = typer.Option(..., help="Host do Postgres"),
    pg_db: str = typer.Option(..., help="Nome do banco Postgres"),
    pg_user: str = typer.Option(..., help="Usuário do Postgres"),
    pg_password: str = typer.Option(..., help="Senha do Postgres"),
    tabela: str = typer.Option(..., help="Tabela a migrar"),
    pg_schema: str = typer.Option('public', help="Schema do Postgres a ser utilizado")
):
    """
    Migra uma tabela do SQLite para o Postgres.
    Cria a tabela no Postgres e insere todos os dados.
    """
    pg_conn = psycopg2.connect(host=pg_host, dbname=pg_db, user=pg_user, password=pg_password)
    typer.echo(f"Migrando tabela: {tabela}")
    colunas, linhas = ler_dados_tabela_sqlite(sqlite_path, tabela)
    criar_tabela_postgres(pg_conn, tabela, colunas, schema=pg_schema)
    inserir_dados_postgres(pg_conn, tabela, colunas, linhas, schema=pg_schema)
    typer.echo(f"Tabela {tabela} migrada com sucesso!")
    pg_conn.close()

if __name__ == "__main__":
    app()