#####################################
rivulet: A redis-based message broker
#####################################

rivulet
    | riv·u·let
    | /ˈriv(y)ələt/
    | *A very small stream*


:code:`rivulet` is a message broker implementation on top of a raw redis
connection.  The entire broker is implemented in the client and does not
require deployment.


.. warning:: :code:`rivulet` is in active devlopment and has not been
    extensively tried and tested in production-level environments.
    Use at your own risk.

.. include:: quickstart.rst


******************
The rivulet client
******************

.. automodule:: rivulet.rivulet
    :members:

Exceptions
==========

.. automodule:: rivulet.exceptions
    :members:

.. toctree::
   :maxdepth: 2
   :caption: Contents

