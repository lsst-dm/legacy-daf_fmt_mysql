#!/usr/bin/env python

#
# See COPYRIGHT file at the top of the source tree
#

from lsst.daf.persistence import (Storage, StorageInterface,
                                  NoRepositoryAtRoot, RepositoryCfg)
from contextlib import closing
import copy
import MySQLdb
import os
import urlparse


class MySqlStorage(StorageInterface):
    """Defines the interface for a connection to a Storage location.

    Requires that a .my.cnf file be present in the home directory
    (``~/.my.cnf``) with the user and password to log into the database.

    Each database represents a single repository.

    Parameters
    ----------
    uri : string
        A URI to connect to a mysql database storage location. The form of the
        URI is:
        `mysql://[host][:port][/[database]]`
        For example:
        mysql://localhost:3306/myRepository

        URI or path that is used as the storage location.
    create : bool
        If True the  a new repository should be created at the root location.
        If False then a new repository will not be created.

    Raises
    ------
    NoRepositoryAtRoot
        If create is False and a repository does not exist at the root
        specified by uri then NoRepositoryAtRoot is raised.
    """

    @staticmethod
    def registerFormatter(formattable, formatter):
        if formattable in MySqlStorage.formatters:
            raise RuntimeError("Registration of second formatter {} for " +
                               "formattable class {}".format(formatter,
                                                             formattable))
        MySqlStorage.formatters[formattable] = formatter

    formatters = {}

    def __init__(self, uri, create):
        self.mysql = MySQLdb.connect(read_default_file="~/.my.cnf")
        self.uri = uri
        parseRes = urlparse.urlparse(uri)
        try:
            self.host, self.port = parseRes.netloc.split(':')
            self.port = int(self.port)
        except ValueError:
            raise RuntimeError(
                "Could not interpret {} to get host and port.".format(
                    parseRes.netloc))
        self.repoDbName = parseRes.path.strip('/')

        if create:
            self._createDb()
        else:
            if self._repoDbExists() is False:
                raise NoRepositoryAtRoot(
                    "Database {} does not exist.".format(self.repoDbName))

    def identifierQuote(self, field):
        """Wrap a field identifier in identifier quote characters. Which quote
        character is used can depend on the state of the backing database (but
        this is not yet implemented).

        Parameters
        ----------
        field : string
            A field identifier to be wrapped in identifier quote characters.

        Returns
        -------
        string
            The field identifier wrapped in identifier quote characters.
        """
        if '`' in field:
            raise RuntimeError('Illegal ` character in field.')
        return "`{}`".format(field)

    def _connection(self):
        return MySQLdb.connect(host=self.host, port=self.port,
                               db=self.repoDbName,
                               read_default_file="~/.my.cnf")

    def _createDb(self):
        self._testSqlIsClean(self.repoDbName)
        # Ideally the execute should be executing the string format (execute
        # would be called with 2 input arguments; the string and the
        # subtitution values), but I can't figure out how to make it work on
        # CREATE DATABASE
        self.mysql.cursor().execute("CREATE DATABASE IF NOT EXISTS %s" %
                                    self.repoDbName)

    def _repoDbExists(self):
        sql = """SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE
            SCHEMA_NAME = %s;"""
        cursor = self.mysql.cursor()
        len = cursor.execute(sql, (self.repoDbName,))
        if len == 1:
            return True
        elif len == 0:
            return False
        else:
            raise RuntimeError("Unexpected number of results.")

    def _connect(self):
        return MySQLdb.connect(host=self.host, port=self.port,
                               db=self.repoDbName,
                               read_default_file="~/.my.cnf")

    def write(self, butlerLocation, obj):
        """Writes an object to a location and persistence format specified by ButlerLocation

        Parameters
        ----------
        butlerLocation : ButlerLocation
            The location & formatting for the object to be written.
        obj : object instance
            The object to be written.
        """
        formatter = self._getFormatter(butlerLocation.pythonType)
        if formatter is None:
            raise RuntimeError("""No formatter registered with storage {} for
                python type {}.""".format(self, butlerLocation.pythonType))
        formatter = formatter()
        formatter.write(obj,
                        table_name=butlerLocation.locationList[0],
                        host=self.host,
                        db=self.repoDbName,
                        port=self.port,
                        view_name=None)

    def _getFormatter(self, objType):
        """Search in the registered formatters for the formatter for obj.

        Will attempt to find formatters registered for the objec type, and then
        for base classes of the object in resolution order.

        Parameters
        ----------
        objType : class type
            The type of class to find a formatter for.

        Returns
        -------
        formatter class object
            The formatter class object to instantiate & use to read/write the
            object from/into the database.

        """
        return self.formatters.get(objType, None)

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
        formatter = self._getFormatter(butlerLocation.pythonType)
        if formatter is None:
            raise RuntimeError("""No formatter registered with storage {} for
                python type {}.""".format(self, butlerLocation.pythonType))
        formatter = formatter()
        return formatter.read(table_name=butlerLocation.locationList[0],
                              host=self.host,
                              db=self.repoDbName,
                              port=self.port,
                              view_name=None)

    def getLocalFile(self, path):
        """Get a handle to a local copy of the file, downloading it to a
        temporary if needed.

        This is not currently not implemented in mysqlStorage.

        As of April 2017 he only place in the LSST stack outside of storage-
        specific locations this is used is in CameraMapper, to get a local copy
        of the  registry.sqlite3 file when connected to an object store. It is
        not obvious that this function is needed for that, and it is possible
        that the registry class should be extended to connect to a mysql
        database. TBD.

        Parameters
        ----------
        path : string
            A path the the file in storage, relative to root.

        Returns
        -------
        A handle to a local copy of the file. If storage is remote it will be
        a temporary file. If storage is local it may be the original file or
        a temporary file. The file name can be gotten via the 'name' property
        of the returned object.
        """
        raise NotImplementedError()

    def exists(self, location):
        """Check if location exists.

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
        return bool(self.instanceSearch(location.locationList[0]))

    def instanceSearch(self, path):
        """Search for the given path in this storage instance.

        Currently HDU indicator processing is not supported by mysqlStorage and
        the header is left on the search path.

        Parameters
        ----------
        path : string
            A filename (and optionally prefix path) to search for within root.

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
        """
        with closing(self._connect()) as conn:
            cursor = conn.cursor()
            len = cursor.execute(
                """SELECT * FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
                LIMIT 1;""",
                (self.repoDbName, path))
            if len == 1:
                return path
            return None

    @classmethod
    def search(cls, root, path):
        """Look for the given path in the current root.

        Also supports searching for the path in Butler v1 repositories by
        following the Butler v1 _parent symlink

        If the path contains an HDU indicator (a number in brackets, e.g.
        'foo.fits[1]', this will be stripped when searching and so
        will match filenames without the HDU indicator, e.g. 'foo.fits'. The
        path returned WILL contain the indicator though, e.g. ['foo.fits[1]'].

        Parameters
        ----------
        root : string
            The path to the root directory.
        path : string
            The path to the file within the root directory.

        Returns
        -------
        string or None
            The location that was found, or None if no location was found.
        """
        storage = MySqlStorage(root, create=False)
        if storage:
            return storage.instanceSearch(path)
        return None

    def copyFile(self, fromLocation, toLocation):
        """Copy a file from one location to another on the local filesystem.

        Parameters
        ----------
        fromLocation : string
            Path and name of existing file.
         toLocation : string
            Path and name of new file.

        Returns
        -------
        None
        """
        with closing(self._connect()) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS {}
                AS SELECT * FROM {};""".format(
                    self.identifierQuote(toLocation),
                    self.identifierQuote(fromLocation)))

    def locationWithRoot(self, location):
        """Get the full path to the location.

        Parameters
        ----------
        location : string
            Path to a location within the repository relative to repository
            root.

        Returns
        -------
        string
            Absolute path to to the locaiton within the repository.
        """
        return os.path.join(self.uri, location)

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
        try:
            mysqlStorage = MySqlStorage(uri, create=False)
        except NoRepositoryAtRoot:
            return None
        if mysqlStorage is None:
            return None
        formatter = mysqlStorage._getFormatter(RepositoryCfg)
        if formatter is None:
            raise RuntimeError("""No formatter registered with storage {} for
                python type {}.""".format(mysqlStorage, RepositoryCfg))
        formatter = formatter()
        return formatter.read(table_name='repositoryCfg',
                              host=mysqlStorage.host,
                              db=mysqlStorage.repoDbName,
                              port=mysqlStorage.port)

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
        uri = loc if loc else cfg.root
        if cfg.root == uri:
            cfg = copy.copy(cfg)
            cfg.root == ''
        try:
            mysqlStorage = MySqlStorage(uri, create=False)
        except NoRepositoryAtRoot:
            return None
        if mysqlStorage is None:
            return None
        formatter = mysqlStorage._getFormatter(RepositoryCfg)
        if formatter is None:
            raise RuntimeError("""No formatter registered with storage {} for
                python type {}.""".format(mysqlStorage, RepositoryCfg))
        formatter = formatter()
        return formatter.write(obj = cfg,
                               table_name='repositoryCfg',
                               host=mysqlStorage.host,
                               db=mysqlStorage.repoDbName,
                               port=mysqlStorage.port)

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
        cfg = cls.getRepositoryCfg(root)
        if cfg is None:
            return None
        return cfg.mapper

    @staticmethod
    def _testSqlIsClean(sql):
        """Raise if illegal characters are found in the sql.

        Most of the time we can be sure our sql is clean by allowing
        execute() to substitute the user-generated strings into the sql
        statement, for example
        ``cursor.execute('select * from %s', (myTableName,))``.
        However there are cases where this does not work, for example
        generating a database with
        ``cursor.execute("CREATE DATABASE IF NOT EXISTS %s", self.repoDbName)
        will fail (because it puts the db name in quotes, which mysql does not
        allow).

        Parameters
        ----------
        sql : string
            An SQL string to examine.

        Raises
        ------
        RuntimeError
            If illegal characters are foudn in the SQL string.
        """
        if ';' in sql:
            raise RuntimeError("Illegal SQL.")


Storage.registerStorageClass(scheme='mysql', cls=MySqlStorage)
