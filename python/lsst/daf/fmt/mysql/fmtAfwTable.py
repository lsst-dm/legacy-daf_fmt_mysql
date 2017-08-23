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

import lsst.afw
from lsst.daf.io.table.sql import to_sql, read_sql
from . import SqlStorage


__all__ = []


def write(engine, butlerLocation, obj):
    """Write an AFWTable to a database using a connected sqlalchemy engine.

    Parameters
    ----------
    engine : sqlalchemy.Engine
        A connected sqlalchemy engine.
    butlerLocation : ButlerLocation
        Location info for writing into the database.
        Only getLocations is used.
    obj : object instance
        The object to write into the database.
    """
    to_sql(obj, butlerLocation.getLocations()[0], engine)


def read(engine, butlerLocation):
    """Read an AFWTable from a database.

    engine : sqlalchemy.Engine
        A connected sqlalchemy engine.
    butlerLocation : ButlerLocation
        Location info for reading from the database.
        Only getLocations is used.
    """
    return read_sql(butlerLocation.getLocations()[0], engine)


SqlStorage.registerFormatters(lsst.afw.table.BaseCatalog, read, write)
