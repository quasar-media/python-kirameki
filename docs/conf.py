from kirameki import __about__

project = "kirameki"
copyright = "2021, Auri"
author = "Auri"

release = __about__.__version__


# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "psycopg2": ("https://www.psycopg.org/docs/", None),
    "flask": ("https://flask.palletsprojects.com/en/latest/", None),
}

html_theme = "sphinx_rtd_theme"

# templates_path = ['_templates']
# html_static_path = ['_static']

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
