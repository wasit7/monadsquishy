# monadsquishy/__init__.py

__title__ = 'DigitalStoreMesh Services'
__version__ = '0.1.16'
__author__ = 'Wasit Limprasert'
__license__ = 'MIT'

VERSION = __version__

from . import functions as sf
from .squishy import Squishy
from .squishy import Monad
from .squishy_dask import SquishyDask