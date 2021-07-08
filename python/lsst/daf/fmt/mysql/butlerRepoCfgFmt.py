#
# See COPYRIGHT file at the top of the source tree
#

from .import MySqlStorage
from contextlib import closing
import lsst.daf.persistence as dafPersist
from lsst.log import Log
import MySQLdb
import pickle


class ButlerRepoCfgFmt():

    def __init__(self):
        self.log = Log.getLogger("daf.mysql.ButlerRepoCfgFmt")

    @staticmethod
    def connect(host, port, db):
        """Connect to the specified MySQL database server."""
        # TODO we probably want to support the other standard cnf file
        # locations
        return MySQLdb.connect(host=host, port=port, db=db,
                               read_default_file="~/.my.cnf")

    def read(self, table_name, host, db, port):
        with closing(self.connect(host, port, db)) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """SELECT sVersion FROM repositoryCfg""")
            except MySQLdb.ProgrammingError:
                return None
            row = cursor.fetchone()
            if row[0] == 1:
                return self._readV1(conn, table_name, host, db, port)
            else:
                raise RuntimeError(("Unsupported serilizaion version {} for " +
                                    "repository in database:{}").format(
                                        row[0], db))

    def _readV1(self, connection, table_name, host, db, port):
        cursor = connection.cursor()
        try:
            cursor.execute(
                """SELECT root, mapper, mapperArgs, parents, policy
                FROM repositoryCfg
                WHERE id = 0""")
        except MySQLdb.ProgrammingError:
            return None
        row = cursor.fetchone()
        return dafPersist.RepositoryCfg(root=pickle.loads(row[0]),
                                        mapper=pickle.loads(row[1]),
                                        mapperArgs=pickle.loads(row[2]),
                                        parents=pickle.loads(row[3]),
                                        policy=pickle.loads(row[1]),
                                        deserializing=True)

    def write(self, obj, table_name, host, db, port):
        """Write a RepositoryCfg object to the database.

        Parameters
        ----------
        obj : RepositoryCfg instance
            The object to be written
        table_name : string
            The name of the table to write the RepositoryCfg into
        host : string
            The database host
        db : string
            The database name
        port : int
            The database port

        Returns
        -------
        None
        """
        self._writeV1(obj, table_name, host, db, port)

    def _writeV1(self, obj, table_name, host, db, port):
        """Versioned writer for a RepositoryCfg object."""
        with closing(self.connect(host, port, db)) as conn:
            cursor = conn.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS repositoryCfg (
                sVersion INT,
                id INT PRIMARY KEY,
                root TEXT,
                mapper TEXT,
                mapperArgs TEXT,
                parents TEXT,
                policy TEXT)""")
            # the repositoryCfg is always index 0; there should be exactly one
            # entry in the repositoryCfg table.
            sVersion = 1
            idx = 0
            root = pickle.dumps(obj.root)
            mapper = pickle.dumps(obj.mapper)
            mapperArgs = pickle.dumps(obj.mapperArgs)
            parents = pickle.dumps(obj.parents)
            policy = pickle.dumps(obj.policy)

            cursor.execute(
                """INSERT INTO repositoryCfg
                (sVersion, id, root, mapper, mapperArgs, parents, policy)
                VALUES(%s, %s, %s, %s, %s, %s, %s)""",
                (sVersion, idx, root, mapper, mapperArgs, parents, policy))
            conn.commit()


MySqlStorage.registerFormatter(dafPersist.RepositoryCfg, ButlerRepoCfgFmt)
