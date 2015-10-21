.. _start:

**************************************************************
Getting Started
**************************************************************

Lunr All-in-One
============================

Get source::

    git clone git@github.com:rackerlabs/lunr.git

Run setup-all-in-one script (tested on lucid, oneiric & precise)::

    ./lunr/bin/lunr-setup-all-in-one

Log out and back in to ensure your group membership is up to date.

Start services in screen::

    ./bin/lunr-screen

Add volume_type::

    lunr-admin types create vtype

Deploy storage node::

    lunr-admin nodes deploy --all


To build the documentation
==========================

Append the precise-backports repo to the `/etc/apt/sources.list` file::

    deb http://archive.ubuntu.com/ubuntu precise-backports main universe multiverse restricted

Create the file `/etc/apt/preferences` with::

    Package: *
    Pin: release a=precise-backports
    Pin-Priority: 400

Install the python-sphix package::

    sudo apt-get update
    sudo apt-get install -t precise-backports python-sphinx

Build the documentation::

    python setup.py build_sphinx

    
To build the packages
=====================

Install package tools and build dependencies::

    sudo apt-get install devscripts debhelper git-core python-all python-support python-setuptools

Get source::

    git clone git@github.com:rackerlabs/lunr.git && cd lunr


Run build command from root of lunr directory::

    debuild -us -uc

.. note::

    The default output directory of debuild is '..'

Install Storage Packages::

    sudo dpkg -i python-lunr*.deb lunr-storage*.deb

Install API Packages::

    sudo dpkg -i python-lunr*.deb lunr-api*.deb

If install fails because of missing dependencies, you should run::

    sudo apt-get -f install

... and try again.

Have fun!!
(report errors to letterj)


To manage the database
======================

Initialize::

    lunr-manage version_control

Upgrade::

    lunr-manage upgrade

Downgrade::

    lunr-manage downgrade 0 (Or your desired version.)

Information::

    lunr-manage version (Version of the source repo.)

    lunr-manage db_version (Version of the db.)
