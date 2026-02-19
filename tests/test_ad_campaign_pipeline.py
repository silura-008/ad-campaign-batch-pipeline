import os
import pytest
from airflow.models.dagbag import DagBag

@pytest.fixture(scope="module")
def dag_bag():
    """
    Loads all DAGs once for the entire module.
    This avoids re-parsing DAGs for each test.
    """
    return DagBag(
        dag_folder=os.path.join(os.getcwd(), "dags"),
        include_examples=False,
    )


def test_dags_import(dag_bag):
    """
    Ensure all DAGs load with zero import errors.
    """
    assert len(dag_bag.import_errors) == 0, \
        f"DAG import failures. Errors: {dag_bag.import_errors}"


def test_ad_campaign_dag_exists(dag_bag):
    """
    Ensure specific DAG exists.
    Prevents accidental rename or deletion.
    """
    assert "ad_campaign_pipeline" in dag_bag.dags