#!/usr/bin/env python

#
# See COPYRIGHT file at the top of the source tree
#

from __future__ import print_function
from contextlib import closing
import lsst.afw.table
from lsst.daf.fmt.mysql import AfwTableSqlFormatter
import lsst.daf.persistence as dafPersist
import lsst.utils.tests
import MySQLdb
import unittest
import os

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))


class MapperForTest(dafPersist.Mapper):

    def __init__(self, *args, **kwargs):
        super(MapperForTest, self).__init__(*args, **kwargs)
        self.storage = dafPersist.Storage.makeFromURI(
            kwargs['repositoryCfg'].root)
        pass

    def map_foo(self, dataId, write):
        # todo test in storage for object if write == False
        return dafPersist.ButlerLocation(
            pythonType=lsst.afw.table.BaseCatalog, cppType=None,
            storageName=None, locationList=['foo'], dataId=dataId, mapper=self,
            storage=self.storage)


class TestAfwTableSqlFormatter(unittest.TestCase):
    """A test case for the AFWTable Sql Formatter"""

    def setUp(self):
        self.host = 'localhost'
        self.port = 3306
        self.removeDatabases = []

    def tearDown(self):
        with closing(self._connect()) as conn:
            for dbName in self.removeDatabases:
                conn.cursor().execute(
                    "DROP DATABASE IF EXISTS {}".format(dbName))

    def _connect(self):
        return MySQLdb.connect(host=self.host, port=self.port,
                               read_default_file="~/.my.cnf")

    def testFloatUnitWritingViaFormatter(self):
        """Test that Float columns can be written into the database storage and
        read back again.
        """
        repoDbName = 'TestAfwTableSqlFormatter'
        self.removeDatabases.append(repoDbName)
        with closing(self._connect()) as conn:
            conn.cursor().execute(
                """CREATE DATABASE IF NOT EXISTS {}""".format(repoDbName))

        schema = lsst.afw.table.Schema()
        aKey = schema.addField("a", type=float)
        bKey = schema.addField("b", type=float)
        outCat = lsst.afw.table.BaseCatalog(schema)
        record = outCat.addNew()
        record.set(aKey, 1.0)
        record.set(bKey, 1.5)

        formatter = AfwTableSqlFormatter()
        formatter.write(obj=outCat,
                        table_name="template_derived_name",
                        host=self.host,
                        port=self.port,
                        db=repoDbName)
        obj = formatter.read(table_name="template_derived_name",
                             host=self.host,
                             port=self.port,
                             db=repoDbName)
        self.assertEqual(obj.asAstropy(), outCat.asAstropy())

    def testFloatUnitWritingViaButler(self):
        schema = lsst.afw.table.Schema()
        aKey = schema.addField("a", type=float)
        bKey = schema.addField("b", type=float)
        outCat = lsst.afw.table.BaseCatalog(schema)
        record = outCat.addNew()
        record.set(aKey, 1.0)
        record.set(bKey, 1.5)
        dbName = "TestAfwTableSqlFormatter"
        self.removeDatabases.append(dbName)
        uri = "mysql://{}:{}/{}".format(self.host, self.port, dbName)
        butler = dafPersist.Butler(outputs={'root': uri,
                                            'mapper': MapperForTest,
                                            'mode': 'rw'})
        butler.put(outCat, 'foo')
        obj = butler.get('foo')
        self.assertEqual(obj.asAstropy(), outCat.asAstropy())

        # Put a record in the same spot as the first one. The put should raise.
        outCat1 = lsst.afw.table.BaseCatalog(schema)
        record = outCat1.addNew()
        record.set(aKey, 2.0)
        record.set(bKey, 2.5)
        with self.assertRaises(RuntimeError):
            butler.put(outCat1, 'foo')


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
