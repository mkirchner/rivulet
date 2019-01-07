"""rivulet - Redis-Based Message Broker for Python"""

from typing import List

__version__ = '0.1.1'
__author__ = 'Marc Kirchner'
__all__ = ['connect', 'Client', 'IndexPolicy']

from rivulet.rivulet import connect, Client, IndexPolicy
