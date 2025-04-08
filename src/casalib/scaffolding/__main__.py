from fire import Fire

from .main import list_templates, run_template


if __name__ == "__main__":
    Fire({
        'run': run_template,
        'list': list_templates,
    })