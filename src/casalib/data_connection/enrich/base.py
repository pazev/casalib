"""
Base classes for enrich module.
"""
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Callable, Dict, List, Optional


@dataclass
class Public:
    """ Class that defines a Public to be enriched """
    event_ymd_column: str
    columns: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """ Test if the event column is in the columns """
        if self.event_ymd_column not in self.columns:
            raise ValueError(
                'event_ymd_column '
                f'`{self.event_ymd_column}` not in columns'
            )

    def missing_columns(self, cols: List[str]) -> List[str]:
        """ Returns True if contains all columns passed """
        if cols is None:
            return True
        return list(set(cols) - set(self.columns))


@dataclass
class Source:
    """
    Class that defines the an info source, to be used to
    enrich the public.
    """
    prefix: str
    keys: List[str] = field(repr=False)
    info_ymd_column: str = field(repr=False)
    ingestion_column: str = field(repr=False)
    columns: List[str] = field(repr=False)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """ Test if the event column is in the columns  """
        if self.info_ymd_column not in self.columns:
            raise ValueError(
                f'info_ymd_column `{self.info_ymd_column}`'
                'not in columns'
            )

        if self.ingestion_column not in self.columns:
            raise ValueError(
                'ingestion_column '
                f'`{self.ingestion_column}` not in columns'
            )

        missing_keys = set(self.keys) - set(self.columns)

        if missing_keys:
            raise ValueError(
                'the following keys are not in columns: '
                f'{missing_keys}'
            )

    def missing_columns(self, cols: List[str]) -> List[str]:
        " Returns True if contains all columns passed """
        if cols is None:
            return []
        return list(set(cols) - set(self.columns))


@dataclass
class EnrichmentPlan:
    """
    Class that defines the enrichment plan, the step that
    must be executed.
    """
    public: Public = field(repr=False)
    source: Source
    included_columns: List[str] = (
        field(default_factory=list, repr=False)
    )
    excluded_columns: List[str] = (
        field(default_factory=list, repr=False)
    )
    renaming_keys: Dict[str, str] = (
        field(default_factory=dict, repr=False)
    )
    renaming_columns: Dict[str, str] = (
        field(default_factory=dict, repr=False)
    )
    keep_source_date_cols: bool = True

    @property
    def output_columns(self) -> Dict[str, str]:
        """
        Return the output columns
        """
        selected_columns = (
            self.included_columns or self.source.columns
        )
        selected_columns_set = (
            set(selected_columns) -
            set(self.excluded_columns)
        )

        # Remove keys
        selected_columns_set = (
            selected_columns_set - set(*self.source.keys)
        )

        if not self.keep_source_date_cols:
            selected_columns_set = (
                selected_columns_set - set([
                    self.source.info_ymd_column,
                    self.source.ingestion_column,
                ])
            )

        # Rename columns
        renamed_columns = {
            col: (prefix + col_ren)
            for col in self.source.columns
            if col in selected_columns_set
            for col_ren in [
                self.renaming_columns.get(col, col)
            ]
            for prefix in [self.source.prefix]
        }

        return renamed_columns

    def validate(self) -> List[str]:
        """
        Validate the current enrichment plan.

        Will execute all methods in this class that begins
        with `testv`. Each of these methods must have the
        signature Callable[[], bool, List[str]], where the
        returned list of strings explains the found errors.
        """
        test_functions = [
            getattr(self, func)
            for func in dir(self)
            if func.startswith('testv')
        ]
        collected_msgs_ = [
            res
            for func in test_functions
            for res in [func()]
            if res
        ]
        return collected_msgs_

    def testv_source_columns_present_on_public_(
        self
    ) -> List[str]:
        """ Test if the source columns are present in the
            public
        """
        source_keys_ren = {
            col: self.renaming_keys.get(col, col)
            for col in self.source.keys
        }
        missed_cols = self.public.missing_columns(
            list(source_keys_ren.values())
        )

        if missed_cols:
            msg = (
                'Source demands the following key columns '
                f'not present in the public: {missed_cols}'
            )
            return [msg]
        return []

    def testv_source_included_columns_present_(
        self
    ) -> List[str]:
        """ Test if the included columns passed are present
            in the source
        """
        missed_cols = self.source.missing_columns(
            self.included_columns
        )

        if missed_cols:
            msg = (
                'The following included_cols were not '
                f'found in the source: {missed_cols}'
            )
            return [msg]
        return []


@dataclass
class Enricher:
    """ Class to create the queries that enrich the public
    """
    public: Public
    steps: List[EnrichmentPlan] = (
        field(default_factory=list)
    )

    def add(
        self,
        source: Source,
        included_columns: Optional[List[str]] = None,
        excluded_columns: Optional[List[str]] = None,
        renaming_keys: Optional[Dict[str, str]] = None,
        renaming_columns: Optional[Dict[str, str]] = None,
        keep_source_date_cols: bool = True,
    ) -> "Enricher":
        """
        Add a new enrichment request to the handler
        """
        # pylint: disable=too-many-arguments

        plan = EnrichmentPlan(
            public=self.public,
            source=source,
            included_columns=included_columns or [],
            excluded_columns=excluded_columns or [],
            renaming_keys=renaming_keys or {},
            renaming_columns=renaming_columns or {},
            keep_source_date_cols=keep_source_date_cols,
        )
        self.steps.append(plan)

        return self

    def __getitem__(self, idx: int) -> EnrichmentPlan:
        return self.steps[idx]

    def __len__(self) -> int:
        return len(self.steps)

    def compile(
        self, compiler: Callable[["Enricher"], Any]
    ) -> Any:
        """ Compile the asked enrichments to an output
            defined by the compiler
        """
        # Validate all the enrichers
        messages = [
            (enr, chain.from_iterable(messages))
            for enr in self.steps
            for messages in [enr.validate()]
            if messages
        ]

        if messages:
            raise ValueError(messages)

        return compiler(self)
