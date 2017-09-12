#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import sqlalchemy

import lsst.daf.persistence as dafPersist
from .sqlalchemy_utils import database_exists


class SqlStorage(dafPersist.StorageInterface):
    """Defines the interface for a connection to a Storage location.

    Parameters
    ----------
    uri : string
        URI or path that is used as the storage location.
        Must start with the scheme of the expected database type. For example 'sqlite:///'
    create : bool
        If True The StorageInterface subclass should create a new
        repository at the root location. If False then a new repository
        will not be created.

    Raises
    ------
    NoRepositroyAtRoot
        If create is False and a repository does not exist at the root
        specified by uri then NoRepositroyAtRoot is raised.
    """

    formatters = {}

    def __init__(self, uri, create):
        """initialzer"""
        # TODO need a way to indicate to create_engine to create (or not) the database if it does not exist.
        self._engine = sqlalchemy.create_engine(uri)
        self.root = uri
        if create is False and not database_exists(uri):
            raise dafPersist.NoRepositroyAtRoot('No repository at {}'.format(uri))

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by ButlerLocation

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object to be written.
        obj : object instance
            The object to be written.
        """
        writeFormatter = self.getWriteFormatter(type(obj))
        if writeFormatter is None:
            raise RuntimeError(
                "No write formatter registered with {} for {}".format(__class__.__name__, type(obj)))
        writeFormatter(self._engine, butlerLocation, obj)

    def read(self, butlerLocation):
        """Read from a butlerLocation.

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object(s) to be read.

        Returns
        -------
        A list of objects as described by the butler location. One item for
        each location in butlerLocation.getLocations()
        """
        readFormatter = self.getReadFormatter(butlerLocation.getPythonType())
        if readFormatter is None:
            raise RuntimeError(
                "No read formatter registered with {} for {}".format(__class__.__name__,
                                                                     butlerLocation.getPythonType()))
        return readFormatter(self._engine, butlerLocation)

    def getLocalFile(self, path):
        """Used to get a handle to a local copy of the file, downloading it to a
        temporary if needed.

        It is not immediately obvious what the role of this fucntion for a database storage is (although one
        can think of examples, e.g. dowload to a local sqlite file). Requirements for this function are TBD,
        in the meantime raises NotImplementedError.

        Parameters
        ----------
        path : string
            A path to the the file in storage, relative to root.

        Raises
        ------
        NotImplementedError
            This funciton is not yet implemented for sql storage.
        """
        raise NotImplementedError

    def exists(self, location):
        """Check if location exists.

        Assumes that 'location' is a table name.

        Parameters
        ----------
        location : ButlerLocation or string
            A a string or a ButlerLocation that describes the location of an
            object in this storage.

        Returns
        -------
        bool
            True if exists, else False.
        """
        tableName = location.getLocations()[0]
        md = sqlalchemy.MetaData()
        # todo is this a good & efficient way to be checking for the existance of a table?
        try:
            sqlalchemy.Table(tableName, md, autoload=True, autoload_with=self._engine)
            return True
        except sqlalchemy.exc.NoSuchTableError:
            return False

    def instanceSearch(self, path):
        """Search for the given path in this storage instance.

        In this StorageInterface `path` is the name of a table within the connected database.

        Parameters
        ----------
        path : string
            The name of a table within the connected database.

        Returns
        -------
        bool
            True if the table exists, else false.

        """
        location = dafPersist.ButlerLocation(pythonType=None, cppType=None, storageName=None,
                                             locationList=[path], dataId={}, mapper=None, storage=None)
        return bool(self.exists(location))

    @classmethod
    def search(cls, root, path):
        """Look for the given path in the current root.

        Currently this function is not implemented, but it can be implemented by instantiating an SqlStorage
        and calling `instanceSearch` if/when needed.

        Parameters
        ----------
        root : string
            The path to the root directory.
        path : string
            The path to the file within the root directory.

        Raises
        ------
        NotImplementedError
            This funciton is not yet implemented for sql storage.
        """
        raise NotImplementedError

    def copyFile(self, fromLocation, toLocation):
        """Copy a file from one location to another on the local filesystem.

        It is not immediately obvious what the role of this fucntion for a sql storage is (although one can
        think of examples, like copying an entire table within a database) and if it should be implemetned.
        Requirements for this function are TBD, in the meantime raises NotImplementedError.

        Parameters
        ----------
        fromLocation : string
            Path and name of existing file.
         toLocation : string
            Path and name of new file.

        Raises
        ------
        NotImplementedError
            This funciton is not yet implemented for sql storage.
        """
        raise NotImplementedError

    def locationWithRoot(self, location):
        """Get the full path to the location.

        It is not immediately obvious what the role of this fucntion for a sql storage is and if it should be
        implemetned. Requirements for this function are TBD, in the meantime raises NotImplementedError.

        Parameters
        ----------
        location : string
            Path to a location within the repository relative to repository
            root.

        Raises
        ------
        NotImplementedError
            This funciton is not yet implemented for sql storage.
        """
        raise NotImplementedError

    @classmethod
    def getRepositoryCfg(cls, uri):
        """Get a persisted RepositoryCfg

        Parameters
        ----------
        uri : URI or path to a RepositoryCfg
            Description

        Returns
        -------
        A RepositoryCfg instance or None
        """
        storageInterface = dafPersist.Storage.makeFromURI(uri)
        if isinstance(storageInterface, cls):
            raise RuntimeError("Currently there is no way to read a RepositoryCfg from a database. " +
                               "A cfg outside the database repo must be used.")
        storageInterface.getRepositoryCfg(uri)

    @classmethod
    def putRepositoryCfg(cls, cfg, loc=None):
        """Serialize a RepositoryCfg to a location.

        When loc == cfg.root, the RepositoryCfg is to be written at the root
        location of the repository. In that case, root is not written, it is
        implicit in the location of the cfg. This allows the cfg to move from
        machine to machine without modification.

        Parameters
        ----------
        cfg : RepositoryCfg instance
            The RepositoryCfg to be serailized.
        loc : string, optional
            The URI location (can be relative path) to write the RepositoryCfg.
            If loc is None, the location will be read from the root parameter
            of loc.

        Returns
        -------
        None
        """
        if loc is None:
            raise RuntimeError("cfgRoot location must be specified for sql repositories.")
        storageInterface = dafPersist.Storage.makeFromURI(loc)
        if isinstance(storageInterface, cls):
            raise RuntimeError("Currently there is no way to write a RepositoryCfg to a database. " +
                               "A cfg outside the database repo must be used.")
        storageInterface.putRepositoryCfg(cfg, loc)

    @classmethod
    def getMapperClass(cls, root):
        """Get the mapper class associated with a repository root.

        Parameters
        ----------
        root : string
            The location of a persisted RepositoryCfg is (new style repos).

        Returns
        -------
        A class object or a class instance, depending on the state of the
        mapper when the repository was created.
        """
        storageInterface = dafPersist.Storage.makeFromURI(root)
        if isinstance(storageInterface, cls):
            raise RuntimeError("Currently there is no way to read a RepositoryCfg to a database. " +
                               "A cfg outside the database repo must be used to get the mapper class.")
        cfg = storageInterface.getRepositoryCfg(root)
        return cfg.mapper


dafPersist.Storage.registerStorageClass(scheme='sqlite', cls=SqlStorage)
