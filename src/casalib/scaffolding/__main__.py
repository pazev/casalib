from fire import Fire

from .main import list_templates, run_template


if __name__ == "__main__":
    Fire({
        'run_template': run_template,
        'list_templates': list_templates,
    })