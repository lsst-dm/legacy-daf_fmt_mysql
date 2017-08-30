# the function `database_exists` is taken from sqlalchemy_utils at
# https://github.com/kvesteri/sqlalchemy-utils/
# and is distributed under the following license:
#
# Copyright (c) 2012, Konsta Vesterinen
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * The names of the contributors may not be used to endorse or promote products
#   derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import copy
import os
import sqlalchemy


def database_exists(url):
    """Check if a database exists.
    :param url: A SQLAlchemy engine URL.
    Performs backend-specific testing to quickly determine if a database
    exists on the server. ::
        database_exists('postgres://postgres@localhost/name')  #=> False
        create_database('postgres://postgres@localhost/name')
        database_exists('postgres://postgres@localhost/name')  #=> True
    Supports checking against a constructed URL as well. ::
        engine = create_engine('postgres://postgres@localhost/name')
        database_exists(engine.url)  #=> False
        create_database(engine.url)
        database_exists(engine.url)  #=> True
    """
    url = copy.copy(sqlalchemy.engine.url.make_url(url))
    database = url.database
    if url.drivername.startswith('postgresql'):
        url.database = 'template1'
    else:
        url.database = None

    engine = sqlalchemy.create_engine(url)

    if engine.dialect.name == 'postgresql':
        text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
        return bool(engine.execute(text).scalar())

    elif engine.dialect.name == 'mysql':
        text = ("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = '%s'" % database)
        return bool(engine.execute(text).scalar())

    elif engine.dialect.name == 'sqlite':
        if database:
            return database == ':memory:' or os.path.exists(database)
        else:
            # The default SQLAlchemy database is in memory,
            # and :memory is not required, thus we should support that use-case
            return True

    else:
        text = 'SELECT 1'
        try:
            url.database = database
            engine = sqlalchemy.create_engine(url)
            engine.execute(text)
            return True

        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.OperationalError):
            return False
