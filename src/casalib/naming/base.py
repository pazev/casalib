from dataclasses import make_dataclass
from typing import Any, Callable, Dict, List, Optional, Type


def naming_generator_factory(
    class_name: str,
    fields: List[str],
    sep: str = '___',
    prefix_: str = '',
    suffix_: str = '',
    validation_procedures: Optional[Dict[str, Callable[[str], bool]]] = None,
    constraints: Optional[Dict[str, List[str]]] = None,
) -> Type[Any]:
    ''' Generates a new dataclass with the given name and fields. '''
    validation_procedures = validation_procedures or {}
    constraints = constraints or {}

    @staticmethod
    def make_template_() -> str:
        fields_ = []
        if prefix_:
            fields_.append('prefix_')
        fields_.extend(fields)
        if suffix_:
            fields_.append('suffix_')

        return sep.join(map(lambda x: f'{{{x}}}', fields_))

    def make_name(self) -> str:
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
            "Was not possible generate a valid name with the "
            "parameters passed."
        )


    @classmethod
    def process_name(cls, name: str):
        if not name.startswith(prefix_) or not name.endswith(suffix_):
            raise ValueError("Prefix or suffix mismatch")

        core = name

        if prefix_:
            core = core[len(prefix_) + len(sep):]

        if suffix_:
            core = core[:-(len(suffix_) + len(sep))]

        parts = core.split(sep)
        if len(parts) != len(fields):
            raise ValueError("Invalid number of components")

        return cls(*parts)

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

    def as_tuple(self):
        return tuple([getattr(fld, self) for fld in fields])

    def __post_init__(self):
        for f in fields:
            value = getattr(self, f)

            if sep in value:
                raise ValueError(f"Field '{f}' cannot contain the separator '{sep}'")

            allowed = constraints.get(f)
            if allowed is not None:
                if value not in allowed:
                    raise ValueError(f"Field '{f}' must be one of {allowed}, got: {value}")

            validator = validation_procedures.get(f)
            if validator:
                if not validator(value):
                    raise ValueError(f"Validation failed for field '{f}': {value}")

    cls = make_dataclass(
        cls_name=class_name,
        fields=[(field, str) for field in fields],
        namespace={
            'make_template_': make_template_,
            'make_name': make_name,
            'process_name': process_name,
            '__reduce__': __reduce__,
            '__post_init__': __post_init__,
            'as_tuple': as_tuple,
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
    validation_procedures: Dict[str, Callable[[str], bool]],
    constraints: Dict[str, List[str]],
    values: tuple,
):
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
