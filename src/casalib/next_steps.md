Casalib next steps
==================

0.  Ensure sql_connection has a function

    ```python
    def add_feature_store_fields(
        query: str,
        info_date_col: str,
        ingestion_date_colname: str
    )
    ```


1. Multiprocessing function:

    1.  Format:

        ```python
        def multiprocess_func(func_params: List[ExecTarget])
        ```

    1.  Considering the structures below:
        ```python
        @dataclass
        class ExecTarget:
            func: Callable[[Any], Any]
            params: Dict[str, Any]
        ```

        and using multiprocess (compatible with
        multiprocessing, but using dill instead of pickle),
        create a function that runs the targets

    2.  Create a async version for IO operations


2.  Selenium helper
    1.  All functions must be pipelines
    2.  Functions to implement
        1.  open_url
        2.  get_url
        3.  send_keys (str)
        4.  send_combo_keys (send combinations like Ctrl+S)
        5.  Move to active tab
        6.  Get active element data
        7.  Set active element data
        8.  Click / Wait / Get value / Get properties
            1.  With text
            2.  Visible
            3.  Active


3.  set_logging_func
    1.  Bring back from last version;
    2.  Use fnmatch logic to module and message
    3.  Add log_message_decorator, to help in creating logic
    4.  Add set_exception_log(python: bool, jupyter: bool)
        -   Basically, change the exception mechanism to log
            any raised exception in code.


5.  KnownTables // KnownQueries
    1.  Save tables and queries, with metadata, to use in
        other codes with aliases


6.  Line commands
    1.  dumzip (all, no-git, no-env)
    2.  tree (no .pyc, no .git, no __pycache__)
    3.  trim_spaces
    4.  set_git_lola and gitattribute to LF only (and not
        CRLF)
    5.  make_config (for starting the casalib config file)


7.  Instructions (show in terminal how to perform some
    tasks):
    1.  Create my windows stack
    2.  Create .conda envs for another platform


8.  Features library
    1.  Every feature transformation is made using
        functions;
        1.  A step can create several features at one;
        2.  Ideally, we will have a description indicating
            the feature meaning

    2.  Feature Transform Step

        ```python
        @dataclass
        class TransformationStep:
            output_features: List[str]
            input_features: List[str]
            operation: str
            params: Dict[str, Any]
            func: Callable[[pd.DataFrame], pd.DataFrame]
            metadata: Dict[str, str]    # Description for
                                        # each feature
                                        # created
        ```

    3.  Difference between Table and FeatureStore
        -   **Table**: set of rows with some level of trust
        -   **FeatureStore**: the information is organized
            in a way that every row has `key_columns`, an
            `info_date` column and an `ingestion_date`
            column;
            -   The keys are unique for `info_date` and
                `ingestion_date`
            -   It is a special case of a table


    3.  Table Operations: will define how to perform an
        operation over a known table; some kind:

        -   input_table: query, table, feature_store,
            pipeline
        -   Operations:
            1.  transform: the TransformationStep from above
                1.  agg_columns
                2.  conditionals
                3.  model execution
                4.  python function execution
            2.  aggregation
            2.  filter
            3.  filter_with_table
            4.  enrich
            5.  columnizer
            7.  analysis (not sure if this needs to be here)
                1.  get_duplicates
                2.  get_diffs (keys and values)

    4.  Pipeline
        -   I want every step above to be a pipeline
            -   func(Pipeline) -> Pipeline
            -   We have the chain information of how the
                steps combine into a solution
            -   We be able to validate the pipeline without
                input_tables
                -   required_fields
                -   created_fields
        -   Log each pipeline execution
            -   Time spent in each step

        -   Create a simple interface where we can see the
            steps