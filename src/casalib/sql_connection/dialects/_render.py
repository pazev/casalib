"""Shared Jinja2 template renderer factory."""
from pathlib import Path
from typing import (
    Protocol,
    Union
)


class RenderProtocol(Protocol):  # pylint: disable=too-few-public-methods
    """Protocol for dialect template renderers."""

    def __call__(self, name: str, **ctx: object) -> str:
        ...

def make_template_render(
    template_path: Union[str, Path]
) -> RenderProtocol:
    """ Load and render the template for dialects. """
    from jinja2 import (
        Environment,
        FileSystemLoader,
        StrictUndefined,
    )
    _env = Environment(
        loader=FileSystemLoader(str(template_path)),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    def _render(name: str, **ctx: object) -> str:
        return _env.get_template(name).render(**ctx)
    return _render
