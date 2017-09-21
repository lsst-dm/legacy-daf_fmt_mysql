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


import numpy
import os
import tempfile
import shutil
import sqlalchemy
import unittest
import yaml

import lsst.utils.tests
from lsst.daf.fmt.mysql import SqlStorage
import lsst.daf.persistence as dafPersist
from lsst.obs.base import CameraMapper
from dafFmtMysqlTestUtils import make_afw_base_catalog, columnSchema


def setup_module(module):
    lsst.utils.tests.init()


ROOT = os.path.abspath(os.path.dirname(__file__))


class MyMapper(CameraMapper):
    packageName = "daf_fmt_mysql"

    def __init__(self, *args, **kwargs):
        # policyFile = dafPersist.Policy.defaultPolicyFile(self.packageName, "MyMapper.yaml", "policy")
        policy = dafPersist.Policy(yaml.load("""
            camera: "camera"
            defaultLevel: "sensor"
            datasets: {}
            exposures: {}
            calibrations: {}
            images: {}"""))
        super(MyMapper, self).__init__(policy, repositoryDir=ROOT, **kwargs)

    def map_table(self, dataId, write):
        loc = dafPersist.ButlerLocation(pythonType=lsst.afw.table.BaseCatalog,
                                        cppType=None,
                                        storageName=None,
                                        locationList=['testname'],
                                        dataId={},
                                        mapper=self,
                                        storage=self.rootStorage)
        return loc


class TestCameraMapper(unittest.TestCase):

    def setUp(self):
        self.testDir = tempfile.mkdtemp(dir=ROOT, prefix="TestCameraMapper-")

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)

    def test_write_read(self):
        """Test that Butler can write a BaseCatalog to an sqlite database, and that the database can be read
        by sqlalchemy and compares equal to the original.
        """
        cat_expected = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'), columnSchema('b', numpy.float64, 'a')],
            ((12345, 1.2345), (4321, 4.123)))
        dbLocation = os.path.join('sqlite:///', os.path.relpath(self.testDir), 'test.db')
        butler = dafPersist.Butler(outputs={'cfgRoot': self.testDir, 'root': dbLocation, 'mapper': MyMapper})

        self.assertFalse(butler.datasetExists('table'))

        # Test writing first
        butler.put(cat_expected, 'table')
        del butler

        # Test reading back with raw object
        engine = sqlalchemy.create_engine(dbLocation)
        rows = engine.execute("select a, b from testname")
        # FIXME: Hopefully tables implement __eq__ in the future
        self._compare_table(cat_expected, rows)

        # Test reading back via butler.get
        butler = dafPersist.Butler(outputs={'root': self.testDir, 'mode': 'rw'})
        self.assertTrue(butler.datasetExists('table'))
        cat_reloaded = butler.get('table')
        self._compare_table(cat_expected, cat_reloaded)

        # append more data and test reading it back
        cat2 = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'), columnSchema('b', numpy.float64, 'a')],
            ((42, 4.2), (24, 2.4)))
        butler.put(cat2, 'table')

        appended_cat_expected = make_afw_base_catalog(
            [columnSchema('a', numpy.int64, 'a'), columnSchema('b', numpy.float64, 'a')],
            ((12345, 1.2345), (4321, 4.123), (42, 4.2), (24, 2.4)))

        # Test reading back via butler.get
        appended_cat_reloaded = butler.get('table')
        self._compare_table(appended_cat_expected, appended_cat_reloaded)

    def _compare_table(self, afw_cat, rows):
        columns = [str(i.field.getName()) for i in afw_cat.schema]
        for cat1_row, cat2_row in zip(afw_cat, rows):
            for column in columns:
                self.assertEqual(cat1_row[column], cat2_row[column])


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


if __name__ == '__main__':
    lsst.utils.tests.init()
    unittest.main()
