# Spark Data Transformation Pipeline

A PySpark notebook in JupyterLab that loads a text corpus into an RDD with `SparkContext`, counts the lines across all files and counts occurrences of the word "antibiotics".

| | Result |
|---|---|
| Files in the corpus | 50 |
| Total lines (`rdd.count()`) | 1,645 |
| Occurrences of "antibiotics" | 2 |

"antibiotics" is matched as a whole word, ignoring case. The singular "antibiotic" appears once and is not counted. `wc -l` reports 1,643 lines because two files don't end with a newline. Spark still counts their last lines, which gives 1,645.

## Files

- `Data_Transformation_Pipeline.ipynb`: the notebook, with outputs.
- `Data_Transformation_Pipeline.py`: the notebook exported as a Python script (part e).
- `requirements.txt`: Python packages.

The dataset is **not** in this repository. Put the corpus's `.text` files in a folder and point the notebook at it (see below).

## Setup (macOS)

```bash
# Java 17 for Spark
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Python environment with JupyterLab and PySpark
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m ipykernel install --sys-prefix --name spark-pipeline --display-name "Python 3 (PySpark)"

# Where the corpus lives (default: ~/Desktop/Text Corpus)
export CORPUS_SOURCE="$HOME/Desktop/Text Corpus"

jupyter lab
```

Open `Data_Transformation_Pipeline.ipynb`, choose the **Python 3 (PySpark)** kernel and run all cells (**Run → Run All Cells**). The notebook starts a local Spark cluster (`local[*]`) and copies the corpus into `data/corpus`, which git ignores. It then answers parts a to e.
