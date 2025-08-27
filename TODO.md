TODO
====
[x] Connection: Partições
    - Todos os métodos de partição
        terminando com partitions
    - Adicionar drop_partitions_filter_pd

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

[ ] List tables in schema

[ ] Utils: relocate a table
    - Create table schema
    - Prepare location
    - Repair table
