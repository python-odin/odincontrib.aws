import os
import sys

HERE = os.path.abspath(os.path.dirname(__file__))
SRC = os.path.normpath(os.path.join(HERE, "..", "src"))
sys.path.insert(0, SRC)
