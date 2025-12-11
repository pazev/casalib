"""
Module define a factory to create names using attributes and
recover these attributes from the name.
"""

from dataclasses import make_dataclass
from typing import (
    Any, Callable, Dict, List, Optional, Type, Tuple
)


def naming_generator_factory(
    class_name: str,
    fields: List[str],
    sep: str = '___',
    prefix_: str = '',
    suffix_: str = '',
    validation_procedures: Optional[
        Dict[
            str,
            List[Callable[[str], bool]]
        ]
    ] = None,
    constraints: Optional[Dict[str, List[str]]] = None,
) -> Type[Any]:
    '''
    Generates a new dataclass with the given name and
    fields.
    '''
    # pylint: disable=too-many-arguments, too-complex
    validation_procedures = validation_procedures or {}
    constraints = constraints or {}

    def make_template_() -> str:
        fields_ = []
        if prefix_:
            fields_.append('prefix_')
        fields_.extend(fields)
        if suffix_:
            fields_.append('suffix_')

        return sep.join(map(lambda x: f'{{{x}}}', fields_))


    def make_name(self) -> str:
        ''' Function to generate name '''
        template = getattr(self, 'make_template_')()
        data = {
            'prefix_': prefix_,
            'suffix_': suffix_,
        } | {fld: getattr(self, fld) for fld in fields}

        generated_name = template.format(**data)

        other = self.process_name(generated_name)

        if other == self:
            return generated_name

        raise ValueError(
            "Was not possible generate the same object "
            "from the parameters passed. "
            f"{self} != {other}."
        )


    def process_name(cls, name: str):
        if (
            not name.startswith(prefix_) or
            not name.endswith(suffix_)
        ):
            raise ValueError("Prefix or suffix mismatch")

        core = name

        if prefix_:
            core = core[len(prefix_) + len(sep):]

        if suffix_:
            core = core[:-(len(suffix_) + len(sep))]

        parts = core.split(sep)
        if len(parts) != len(fields):
            raise ValueError(
                "Invalid number of components. "
                f"{fields} != {parts}"
            )

        return cls(*parts)

    def process_name_as_tuple(cls, name: str) -> Tuple[str, ...]:
        obj = cls.process_name(name)
        return obj.as_tuple()

    def __reduce__(self):
        return (
            _reconstruct_instance,
            (
                class_name,
                fields,
                sep,
                prefix_,
                suffix_,
                validation_procedures,
                constraints,
                tuple(getattr(self, f) for f in fields),
            )
        )

    def as_tuple(self) -> Tuple[str]:
        return tuple(getattr(self, fld) for fld in fields)

    def __post_init__(self) -> None:
        for f in fields:
            value = getattr(self, f)

            if sep in value:
                raise ValueError(
                    f"Field '{f}' cannot contain the "
                    f"separator '{sep}'"
                )

            allowed = constraints.get(f)
            if allowed is not None:
                if value not in allowed:
                    raise ValueError(
                        f"Field '{f}' must be one of "
                        f"{allowed}, got: {value}"
                    )

            validators = validation_procedures.get(f, [])
            for validator in validators:
                if not validator(value):
                    raise ValueError(
                        "Validation failed for field "
                        f"'{f}': {value}"
                    )

    cls = make_dataclass(
        cls_name=class_name,
        fields=[(field, str) for field in fields],
        namespace={
            'make_template_': staticmethod(make_template_),
            'make_name': make_name,
            'process_name': classmethod(process_name),
            '__reduce__': __reduce__,
            '__post_init__': __post_init__,
            'as_tuple': as_tuple,
            'process_name_as_tuple': classmethod(process_name_as_tuple),
        },
        frozen=True,
        eq=True,
    )

    return cls


def _reconstruct_instance(
    class_name: str,
    fields: List[str],
    sep: str,
    prefix: str,
    suffix: str,
    validation_procedures: Dict[
        str, List[Callable[[str], bool]]
    ],
    constraints: Dict[str, List[str]],
    values: Tuple,
):
    # pylint: disable=too-many-arguments
    cls = naming_generator_factory(
        class_name,
        fields,
        sep,
        prefix,
        suffix,
        validation_procedures,
        constraints,
    )
    return cls(*values)
