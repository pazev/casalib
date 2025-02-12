from dataclasses import dataclass, field
from typing import Dict

import boto3


@dataclass
class QueryExec:
    """ Classe para gerenciar a execução de queries usando boto3
    """
    query: str = field(repr=False)
    query_id: str
    workgroup: str
    schema_name: str
    data_catalog: str
    wait_seconds: int = 1

    def get_execution_info_(
        self, boto3_session: boto3.Session
    ) -> Dict:
        """ Captura dados da execução """
        athena = boto3_session.client('athena')
        query_exec = athena.get_query_execution(
            QueryExecutionId=self.query_id
        )
        return query_exec

    def get_status(self, boto3_session: boto3.Session) -> str:
        """ Captura status de uma query """
        res = self.get_execution_info_(boto3_session)
        return res ['QueryExecution'][ 'Status ']['State']

    def wait(self, boto3_session: boto3.Session) -> "QueryExec":
        """ Espera a execução da query """
        import time
        while True:
            status = self.get_status(boto3_session)
            if status in ['CANCELLED' ]:
                raise RuntimeError('Query cancelled by user')

            if status in ['FAILED']:
                error_message = (
                    self.get_execution_info_(boto3_session)
                    ['QueryExecution' ]
                    ['Status']
                    ['AthenaError']
                    ['ErrorMessage' ]
                )

                raise RuntimeError(
                    'Query failed with the following '
                    f'message: {error_message}'
                )

            if status in ['SUCCEEDED']:
                break

            time.sleep(self.wait_seconds)

        return self

    def get_query_results(self, boto_session: boto3.Session):
        """ Captura resultados da query """
        athena = boto_session.client('athena')

        self.wait(boto_session)

        tkn_key_detect = 'NextToken'
        tkn_key = 'NextToken'
        res = {}
        results = []

        while True:
            tkn = (
                {}
                if tkn_key_detect not in res
                else {tkn_key: res[tkn_key]}
            )
            res = athena.get_query_results(
                QueryExecutionId=self.query_id,
                **tkn
            )
            results.append(res)

            if tkn_key not in res:
                break

            return results


def run_query(
    query: str, schema_name: str, data_catalog: str,
    workgroup: str, boto3_session: boto3.Session
) -> QueryExec:
    """ Roda uma query """
    athena = boto3_session.client('athena')

    query_exec = athena.start_query_execution(
        QueryString=query,
        WorkGroup=workgroup,
        QueryExecutionContext={
            'Database': schema_name,
            'Catalog': data_catalog
        }
    )

    return QueryExec(
        query=query,
        query_id=query_exec['QueryExecutionId'],
        workgroup=workgroup,
        schema_name=schema_name,
        data_catalog=data_catalog,
    )
