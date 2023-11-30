===================================================
Easy pythonic API to copy and sync to and from s3
===================================================
|Unittests| |License| |Downloads| |Language|

.. |Unittests| image:: https://github.com/andyil/s3shutil/actions/workflows/unitests.yml/badge.svg
    
.. |Downloads| image:: https://img.shields.io/pypi/dw/s3shutil
    
.. |License| image:: https://img.shields.io/github/license/andyil/s3shutil
    :target: https://github.com/andyil/s3shutil/blob/develop/LICENSE
    :alt: License

.. |Language| image:: https://img.shields.io/github/languages/top/andyil/s3shutil

s3shutil is the easiest to use and fastest way of manipulating directory files in s3,
probably at the expense of hiding API details you don't usually care.


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
* Expose powerful one-liners.
* Emulate the well known `shutil <https://docs.python.org/3/library/shutil.html>`_ standard module API.
* Use performance boosts behind the scenes (multithreading, batching, server to server operations)
* No dependencies, where possible


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
    s3shutil.copytree('s3://bucket/remote/files/', '/home/myuser/my-directory/')


    # upload a tree to s3
    s3shutil.copytree('/home/myuser/documents', 's3://bucket/my-files/documents/')


    # copy between two s3 locations
    # same or different bucket
    s3shutil.copytree('s3://other-bucket/source-files/whatever/', 's3://bucket/my-files/documents/')


    # delete (recursively) entire prefix
    s3shutil.rmtree('s3://bucket/my-files/documents/')


**Just released! (December 2023), tree_sync operation:**

Only copies files that are missing in the destination.
Also deletes extra files.


.. code-block:: python

    # sync download
    s3shutil.tree_sync('s3://bucket/my-files/documents/', '/home/myuser/documents')


    # sync upload
    s3shutil.tree_sync('/home/myuser/documents', 's3://bucket/my-files/documents/')


    # sync two bucket locations
    s3shutil.tree_sync('s3://bucket/my-files/documents/', 's3://another-bucket/a/b/c')



Conclusions
~~~~~~~~~~~~~~
s3shutil will notice alone if the location is s3 (starts with s3://) or not
All operations have a similar string based API of powerfull one liners

Contact
~~~~~~~~~~~~~~
Just use it! You are also invited to send an email `andyworms@gmail.com`.
All emails are (eventually) answered.
Read the code, fork, open a PR, start a discussion.

