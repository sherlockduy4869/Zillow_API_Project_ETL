from etls.redshift_etl import create_namespace, create_workspace
from utils.constants import REDSHIFT_NAMESPACE_NAME, REDSHIFT_WORKSPACE_NAME


def redshift_pipeline(session):
    
    redshift_serverless = session.client("redshift-serverless")

    create_namespace(redshift_serverless, REDSHIFT_NAMESPACE_NAME)

    create_workspace(redshift_serverless, REDSHIFT_WORKSPACE_NAME, REDSHIFT_NAMESPACE_NAME)
