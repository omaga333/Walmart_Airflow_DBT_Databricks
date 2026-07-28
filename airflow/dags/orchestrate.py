from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag()
def orchestrate():

    @task
    def ingest_cdc():
        return "cdc data ingested"


    target_clean = BashOperator(
        task_id="target_clean",
        bash_command="""
        rm -rf /opt/airflow/walmart_project/target
        """
    )


    source_freshness = BashOperator(
    task_id="source_freshness",
    cwd="/opt/airflow/walmart_project",
    bash_command="""
    dbt source freshness --profiles-dir /opt/airflow/walmart_project
    """
)


    silver_technical_run = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="""
        dbt run --select silver_tech
        """
    )


    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd="/opt/airflow/walmart_project",
        bash_command="""
        dbt test --select silver_tech
        """
    )


    gold_ephemeral_run = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="""
        dbt run --select path:models/gold/ephemeral
        """
    )


    gold_dimension = BashOperator(
        task_id="gold_dimension",
        cwd="/opt/airflow/walmart_project",
        bash_command="""
        dbt snapshot
        """
    )


    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd="/opt/airflow/walmart_project",
        bash_command="""
        dbt run --select path:models/gold/fact
        """
    )


    ingest_cdc() >> target_clean >> source_freshness >> \
    silver_technical_run >> silver_technical_tests >> \
    gold_ephemeral_run >> gold_dimension >> gold_fact


orchestrate_dag = orchestrate()