import json
import copy
from pathlib import Path


def read_ipynb(notebook_path):
    with open(notebook_path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_ipynb(notebook, notebook_path):
    with open(notebook_path, "w", encoding="utf-8") as f:
        json.dump(notebook, f)


def merge_ipynb_files(
    relative_path=".", notebook_search="*", output_filename="temp_all.ipynb"
):
    """Merge ipynb in relative_path found by wildcard notebook_search into one file"""
    local_nb_paths = list(Path(relative_path).glob(f"{notebook_search}ipynb"))
    local_nb_all = copy.deepcopy(read_ipynb(local_nb_paths[0]))
    for nb in local_nb_paths:
        nb = read_ipynb(nb)
        local_nb_all["cells"] = local_nb_all["cells"] + nb["cells"]
    write_ipynb(local_nb_all, output_filename)
