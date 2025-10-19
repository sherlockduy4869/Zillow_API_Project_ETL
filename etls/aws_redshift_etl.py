import logging
import time

from utils.constants import REDSHIFT_ROLE_ARN

def create_namespace(redshift_serverless, NAMESPACE_NAME):
    try:
        # Check if namespace exists
        namespaces = redshift_serverless.list_namespaces()
        for ns in namespaces.get('namespaces', []):
            if ns['namespaceName'] == NAMESPACE_NAME:
                logging.info("Namespace already exists!")
                return ns['namespaceName']

        # Create namespace
        response = redshift_serverless.create_namespace(
            namespaceName=NAMESPACE_NAME,
            dbName='dev',
            iamRoles=[REDSHIFT_ROLE_ARN]
        )
        logging.info("Namespace created:", response['namespace']['namespaceName'])
        return response['namespace']['namespaceName']

    except Exception as e:
        print("Error creating namespace:", e)

def create_workspace(redshift_serverless, WORKSPACE_NAME, NAMESPACE_NAME):
    try:
        # Check if workgroup exists
        workgroups = redshift_serverless.list_workgroups()
        for wg in workgroups.get('workgroups', []):
            if wg['workgroupName'] == WORKSPACE_NAME:
                logging.info("Workgroup already exists!")
                return wg['workgroupName']

        # Create workgroup
        response = redshift_serverless.create_workgroup(
            workgroupName = WORKSPACE_NAME,
            namespaceName = NAMESPACE_NAME,
            baseCapacity=32,   # 32 RAUs, adjust for testing
        )
        logging.info("Workgroup created:", response['workgroup']['workgroupName'])

        # Wait until available
        while True:
            wg = redshift_serverless.get_workgroup(workgroupName=WORKSPACE_NAME)
            status = wg['workgroup']['status']
            logging.info("Workgroup status:", status)
            if status == 'AVAILABLE':
                print("Workgroup is ready!")
                break
            time.sleep(20)

        return response['workgroup']['workgroupName']

    except Exception as e:
        logging.error("Error creating workgroup:", e)


