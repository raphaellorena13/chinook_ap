from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import subprocess
from migracao import migrar

def executar_migracao(table):
    """
    Executa a migração de uma tabela do SQLite para o Postgres.
    Parâmetros de conexão definidos conforme ambiente Docker.
    """
    sqlite_path = '/opt/airflow/dags/chinook.db'  # Caminho do banco SQLite no container
    pg_host = 'postgres'                          # Host do serviço Postgres no docker-compose
    pg_db = 'postgres'                            # Nome do banco de destino
    pg_user = 'airflow'                           # Usuário do Postgres
    pg_password = 'airflow'                       # Senha do Postgres
    pg_schema = 'raw'                             # Schema de destino
    migrar(
        sqlite_path=sqlite_path,
        pg_host=pg_host,
        pg_db=pg_db,
        pg_user=pg_user,
        pg_password=pg_password,
        tabela=table,
        pg_schema=pg_schema
    )

def executar_dbt(tipo_execucao: str):
    """
    Executa comandos dbt ('run' ou 'test') no projeto analytics.
    Captura e exibe o output para logging.
    """
    result = subprocess.run(
        ['dbt', tipo_execucao, '--project-dir', '/opt/airflow/analytics/'],
        capture_output=True,
        check=True
    )
    print(result.stdout.decode())
    if result.returncode != 0:
        raise Exception(f"dbt {tipo_execucao} failed: {result.stderr.decode()}")


default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl_sqlite_to_analytics',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    description='Pipeline: SQLite -> raw -> dbt -> analytics',
    tags=['apostaGanha', 'SQLite', 'Postgres', 'raw', 'dbt', 'analytics'],
    max_active_runs=1,  # Garante execução serial
) as dag:
    # Operadores de início e fim para delimitar o pipeline
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Tabelas a migrar do SQLite para o Postgres
    tabelas = [
        "album",
        "artist",
        "customer",
        "genre",
        "invoice",
        "invoiceline",
        "track"
    ]

    # Grupo de tasks para migração das tabelas
    with TaskGroup(group_id='ingest_sqlite_to_raw') as tg_ingest:
        migrate_tasks = []
        for tabela in tabelas:
            task = PythonOperator(
                task_id=f'migracao_da_tabela_{tabela}',
                python_callable=executar_migracao,
                op_args=[tabela],
                doc=f"Migra a tabela {tabela} do SQLite para o Postgres (schema raw)."
            )
            migrate_tasks.append(task)
        # Encadeia as tasks de migração em série
        for i in range(len(migrate_tasks) - 1):
            migrate_tasks[i] >> migrate_tasks[i+1]

    # Grupo de tasks para execução do dbt (run e test)
    tipo_dbts = ['run', 'test']
    with TaskGroup(group_id='dbt') as tg_dbt:
        list_dbt_tasks = []
        for tipo in tipo_dbts:
            task = PythonOperator(
                task_id=f'dbt_{tipo}',
                python_callable=executar_dbt,
                op_args=[tipo],
                doc=f"Executa dbt {tipo} no projeto analytics."
            )
            list_dbt_tasks.append(task)
        # Encadeia as tasks dbt em série
        for i in range(len(list_dbt_tasks) - 1):
            list_dbt_tasks[i] >> list_dbt_tasks[i+1]

    # Orquestração final do pipeline
    start >> tg_ingest >> tg_dbt >> end