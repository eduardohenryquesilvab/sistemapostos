import os
import sys

# Adiciona o diretório atual ao path do Python
sys.path.insert(0, os.path.dirname(__file__))

# O Passenger exige que o objeto Flask se chame 'application'
from app import app as application
