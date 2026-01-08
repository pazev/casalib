"""
Query templates for AWS Athena.
"""
from ._load_templates import templates_dict
from ._template import AthenaTemplates


# TODO: check which object is using templates_dict
__all__ = [
    'AthenaTemplates',
    'templates_dict'
]
