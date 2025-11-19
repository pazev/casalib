TODO
====

2025-11-13
----------
[ ] Cloud utils
    [ ] List Processing Jobs
    [ ] CloudWatch
        [ ] get_metrics
        [ ] get_log
    [ ] Athena
        [ ] list_queries
        [ ] List tables in schema

[ ] Mapping
    [ ] Abstract
        [ ] Get
            [ ] rows_count
            [ ] nulls_count
            [ ] not_nulls_count
            [ ] zeros_count
            [ ] sample
            [ ] common_values (threeshold)
            [ ] percentiles
        [ ] Count

[ ] Enricher
    [x] Ajustar pylint e mypy (MUITO ERRO)
    [ ] Tabela de target final
    [ ] Enriquecer: tabela de público com todos os targets
    [ ] Adicionar parâmetro para input de partições
    [ ] Alert: duplicates

    [ ] AthenaQueryObjects
        - Steps
            - Public
            - Targets
            - Query final
            - Vars

[ ] TableManager
    [ ] Adicionar discover_params_ na interface base
    [ ] last_partition sem conn
    [ ] Iterar params no PandasTableManager

[ ] Salvar dataframe em Excel
    [ ] Usando openpyxl, abrir ou criar um arquivo excel e salvar o dataframe dentro

[ ] ConnectionAAbstract
    [x] Objeto Querier (templates aqui)
    [x] List duplicates (no template)
    [ ] Left Join function
    [X] Promover last_partition para o querier - não faz sentido; dropado
    [x] Adicionar Deprecated warnings na casalib
        [x] Remove method
        [x] Remove from any place inside casalib

[ ] Remote execution
    [ ] Passar um diretório como tar.gz
    [ ] Criar bootloader especial
            - Unpack
            - Exec

[ ] Objeto KnownNames / KnownTables


[ ] No processo de criação de um novo modelo, temos que
    [ ] 1.  Criar as tabelas intermediárias, que são
            basicamente o público enriquecido com o target;
            [ ] Criar a tabela de público com os diversos
                targets;
    [ ] 2.  Fazer a seleção das variáveis;
    [ ] 3.  Criar a tabela final, com as variáveis
                necessárias para o modelo.

    O Enricher então precisa quebrar este processo, gerando
        as informações conforme a necessidade.
        [ ] Targets: cruza tudo e deixa pronto
        [ ] Sources: cruza a tabela de saída dos targets com a fonte


[-] TableManager
    [x] Refactoring
        [x] Move abstract classes and collection
            code to base folder
        [x] Rename TableManagerAbstract to
                BaseTableManager
        [x] Create a new TableManagerAbstract
                template, that
            implements all methoods
        [x] Refactor other TableManagers to use this
            new TableManagerAbstract (only implement
            what must be overloaded)
            [x] OtherOwnerTableManager
            [x] ManageableTableManager
            [x] QueryTableManager
            [x] PandasTableManager
    [x] Add ConnectionType to TableManager
    [ ] Add known_names to all TableManagers
        [ ] get_known_names
        [ ] add_known_names

[ ] Método de contagem (counter_map)
    - Contar os nulos, valores indicados
        nos eqs, e posições entre pontos
    [ ] Separado do data_connection
    [ ] Rodar sobre Pandas e conexão

[ ] Utils: relocate a table
    - Create table schema
    - Prepare location
    - Repair table



[ ] Módulo data_ops
    [ ] Mapeamento
        [ ] Contagem de linhas
        [ ] Contagem de nulos
        [ ] Contagem de distintos
        [ ] Contagem de -infty / infty
        [ ] Percentis (sem common_values)
        [ ] Valores comuns
    [ ] Método de Contagem
        [ ] Dada uma lista de valores, contar valores igual
            a estes valores e valores entre eles
    [ ] Métricas
        [ ] WOE
        [ ] Information Value
        [ ] KS
        [ ] PSI


[ ] API para obter conhecimento


[ ] Mecanismo de plugin para importar os módulos da pasta queries
    [ ] Dicionários
        [ ] table_manager
        [ ] runner
        [ ] source

[ ] Alteraçõea TableManager
    [x] Query last partition
    [x] Gerar código para capturar última partição
                - Novo tipo de tabela

    [ ] Adicionar skip

    [x] Ajustar código para expandir tipos de TableManager
        que podem ser criados
        [x] Módulo movido para um diretório
        [x] Classe base com operações gerais (TableHelper)
            [x] Mapear operações básicas que sempre devem
                estar disponíveis
                    .set_conn_maker()
                    .get_conn()
                    .drop()
                    .drop_partitions()
                    .list_partitions()
                    .list_partitions_filter()
                    .drop_partitions_filter()
                    .sample()
                    .metadata()
        [x] Classe para tabela geral, AnyTableManager
        [x] Ajustar TableManager, para usar o TableHelper
        [x] Classe abstrata, com método run, que roda um
            método Python qualquer com a missão de Adicionar
            dados na tabela >>> ExecutableTableManagerAbstract

    [x] Classe para código Python
        [x] PythonTableManager
            - method: Callable[[], pd.DataFrame]
            - input_tabs: List[str]

    [x] Método run, __call__
        [ ] Adicionada lib makefun para assinatura dinâmica
        [ ] Post init

[ ] Adicionar registro de diretórios no scaffolding
    [ ] PanTemplates


[ ] DataPath
    [ ] Return DataPath creation

[ ] ConnectionAbstract
    [ ] Pandas pipeline:
        [ ] Download the table, using if possible partition_cols
        [ ] Apply pandas pipeline over the table
        [ ] Saves the table at Athena


============================================================



=====
FEITO
=====

[x] Connection: Partições
    - Todos os métodos de partição
        terminando com partitions
    - Adicionar drop_partitions_filter_pd


3. [X] Query class (namedtuple)
    - A ideia é ter um método apéos o comando, .query, que
     permita ver a query gerada.
        .create_insert(-) -> Roda
        .create_insert.queries(-) -> retorna as queries
    - params: Dict[str, str]
    - method: Callable[[...], Any]
    - make_query_method: Callable[[...], List[str]]
        .__call__(self, *args, **kwargs)
            return self.method(*args, **kwargs)
        .query(*args, **kwargs)
            return self.make_query_method(*args, **kwargs)

    - OBSERVAÇÃO: FAZER O SIMPLES - UM OBJETO, make_queries,
        que gera as queries necessárias


[x] ConnectionAbstract
    [x] List partition filter, similar to what is implanted
        in TableManager, on ConnectionAthena
        [x] list_partitions_filter
        [x] drop_partitions_filter
        [x] adjust TableManager to use connection

        [x] List partition filter as pandas DataFrame

[x] TableManagerCollection
    [x] add_table_manager
    [x] required_vars
        [x] Roda os runs, vé o que falta
            [x] TableManager: required_vars
    [x] known_variables
    [x] Add known_tables method and dict
        [x] Inicializar passando lista módulos e varrer os
            mesmos atrás de variáveis TableManager
        [x] Passando um diretório
    [x] Ordenar os tm: quem depende de quem?
