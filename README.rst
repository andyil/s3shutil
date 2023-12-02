===========================================================
Python library to copy, sync and move files to and from s3
===========================================================
|Unittests| |License| |Downloads| |Language| |PyVersions|

.. image:: https://raw.githubusercontent.com/andyil/s3shutil/master/s3shutil-logo.png
  :width: 800
  :alt: s3shutil logo

.. |Unittests| image:: https://github.com/andyil/s3shutil/actions/workflows/unitests.yml/badge.svg
    
.. |Downloads| image:: https://img.shields.io/pypi/dw/s3shutil
    
.. |License| image:: https://img.shields.io/github/license/andyil/s3shutil
    :target: https://github.com/andyil/s3shutil/blob/develop/LICENSE
    :alt: License

.. |Language| image:: https://img.shields.io/github/languages/top/andyil/s3shutil

.. |PyVersions| image:: https://img.shields.io/pypi/pyversions/s3shutil.svg

**s3shutil is the easiest to use and fastest way of moving around directories and files in s3.**


.. note::
   December 1st, 2023. Just released sync operation

   Sync operation allows you to incrementally copy to destination files that
   were added to source since the last copy
   Supports all directions: s3 to s3, s3 to local drive, local drive to s3.


Installation
---------------
We recommend installing from the official PyPI repository.

.. code-block:: sh

    $ pip install s3shutil
    




Design Principles
~~~~~~~~~~~~~~~~~
* A simple and intuitive string based API.
* Symmetric API: download and uploads work equally
* Exposes powerful and performant one-liners.
* Emulate the well known `shutil <https://docs.python.org/3/library/shutil.html>`_ standard module API.
* Use performance boosts behind the scenes (multithreading, batching, server to server operations)
* No dependencies except boto3


Using s3shutil
~~~~~~~~~~~~~~
s3shutil uses `boto3 <https://github.com/boto/boto3>`_ internally and we assume you have your credentials set up properly.

Using s3shutil is super easy:

**Import is mandatory, no suprises here**:

.. code-block:: python

    import s3shutil

**Then you can do powerful things with simple one liners:**:

.. code-block:: python

    # download a tree from s3
    s3shutil.copytree('s3://bucket/my/path', '/home/myuser/files/')

    # upload a tree to s3
    s3shutil.copytree('/home/users/pics/', 's3://bucket/path/archive/')

    # copy between two s3 locations
    # same or different bucket
    s3shutil.copytree('s3://bucket2/files/someth/', 's3://bucket1/backup/old/')

    # delete (recursively) entire prefix
    s3shutil.rmtree('s3://bucket/my-files/documents/')


**Just released! (December 2023), tree_sync operation:**

Only copies files that are missing in the destination.
Also deletes extra files.


.. code-block:: python

    # sync download
    s3shutil.tree_sync('s3://bucket/files/docs/', '/home/myuser/docs')

    # sync upload
    s3shutil.tree_sync('/home/myuser/files/', 's3://bucket/files/docs-v2/')

    # sync two bucket locations
    s3shutil.tree_sync('s3://bucket/files/docs/', 's3://bucket2/a/b/c')


Conclusions
~~~~~~~~~~~~~~
s3shutil will notice alone if the location is s3 (starts with s3://) or not
All operations have a similar string based API of powerfull one liners


Test Matrix
~~~~~~~~~~~~~~
s3shutil is thoroughly unit tested in all the combinations of:

Python Versions:

+ 3.12
+ 3.11 
+ 3.10
+ 3.9
+ 3.8
+ 3.7

And boto3 Versions: 

+ 1.33
+ 1.30
+ 1.28
+ 1.27
+ 1.26
+ 1.25
+ 1.24
+ 1.23


Contact
~~~~~~~~~~~~~~
Just use it! You can send an email as well `andyworms@gmail.com`.
All emails are (eventually) answered.
Also read the code, fork, open a PR, start a discussion.

