#
# See COPYRIGHT file at the top of the source tree
#

from contextlib import closing
import MySQLdb
import math
import re
import struct
import tempfile

from . import MySqlStorage
import lsst.afw.table as afw_table
from lsst.log import Log

MYSQL_ER_TABLE_EXISTS_ERROR = 1050

class FieldFormatter(object):
    """Formatter for fields in an |afw catalog|.

    This class is a container for a function that maps an |afw table| field to
    a MySQL type, and a function that maps a field value to a literal suitable
    for use in a MySQL ``INSERT`` or ``REPLACE`` statement.
    """

    def __init__(self, sql_type_callable, format_value_callable):
        """Store the field formatting information."""
        self.sql_type_callable = sql_type_callable
        self.format_value_callable = format_value_callable

    def sql_type(self, field):
        """Return the SQL type of values for `field`."""
        return self.sql_type_callable(field)

    def format_value(self, value):
        """Return a string representation of `value`.

        The return value will be suitable for use as a literal in an
        ``INSERT``/``REPLACE`` statement.  ``None`` values are always
        converted to ``"NULL"``.
        """
        if value is None:
            return "NULL"
        return self.format_value_callable(value)


def _format_number(format_string, number):
    """Format a number for use as a literal in a SQL statement.

    NaNs and infinities are converted to ``"NULL"``, because MySQL does not
    support storing such values in ``FLOAT`` or ``DOUBLE`` columns. Otherwise,
    `number` is formatted according to the given `format string`_.

    .. _format string:
        https://docs.python.org/library/string.html#format-string-syntax
    """
    if math.isnan(number) or math.isinf(number):
        return "NULL"
    return format_string.format(number)


def _format_string(string):
    """Format a string for use as a literal in a SQL statement.

    The input is quoted, and embedded backslashes and single quotes are
    backslash-escaped.
    """
    return "'" + string.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _format_array(format_char, array):
    """Format an array for use as a literal in a SQL statement.

    The array elements are packed into a sequence of bytes, with bytes
    comprising individual elements arranged in little-endian order. This
    sequence is then transformed into a MySQL hexadecimal literal and returned.

    Parameters
    ----------

    format_char : str
        One of the `format characters`_ defined by the :mod:`struct` module.

    array : sequence
        A homogeneous sequence.

    .. _format characters:
        https://docs.python.org/library/struct.html#format-characters
    """
    byte_string = struct.pack("<" + str(len(array)) + format_char, *array)
    return "x'" + byte_string.encode("hex_codec") + "'"


def _sql_type_for_string(field):
    """Compute the SQL column type of a string valued field."""
    sz = field.getSize()
    if sz > 65535:
        # If necessary, longer strings could be ingested as TEXT.
        raise RuntimeError("String field is too large for ingestion")
    elif sz == 0:
        raise RuntimeError("String field has zero size")
    # A string containing trailing spaces cannot round-trip to a CHAR
    # column and back. Therefore, use VARCHAR. Also, assume strings are
    # ASCII for now.
    return ("VARCHAR({}) CHARACTER SET ascii COLLATE ascii_bin NOT NULL"
            .format(sz))


def _sql_type_for_array(format_char, field):
    """Compute the SQL column type of an array valued |afw table| field.

    Parameters
    ----------

    format_char : str
        One of the `format characters`_ defined by the :mod:`struct` module.

    field : field
        A descriptor for an array-valued field (e.g. a |Field_ArrayF|).

    .. _format characters:
        https://docs.python.org/library/struct.html#format-characters
    .. |Field_ArrayF|  replace::  :class:`~lsst.afw.table.Field_ArrayF`
    """
    sz = field.getSize()
    if sz == 0:
        return "BLOB NOT NULL"
    sz *= struct.calcsize("<" + format_char)
    if sz > 65535:
        raise RuntimeError("Array field is too large for ingestion")
    return "BINARY({}) NOT NULL".format(sz)


"""A mapping from |afw table| field type strings to field |formatter|s.

This mapping is used by :class:`.IngestCatalogTask` to determine how to format
|afw table| field values from the input catalog.  If new field types are added
to the |afw table| library, they should also be added here.
"""
field_formatters = dict(
    U=FieldFormatter(lambda f: "SMALLINT UNSIGNED NOT NULL",
                     lambda v: str(v)),
    I=FieldFormatter(lambda f: "INT NOT NULL",
                     lambda v: str(v)),
    L=FieldFormatter(lambda f: "BIGINT NOT NULL",
                     lambda v: str(v)),
    F=FieldFormatter(lambda f: "FLOAT",
                     lambda v: _format_number("{:.9g}", v)),
    D=FieldFormatter(lambda f: "DOUBLE",
                     lambda v: _format_number("{:.17g}", v)),
    Flag=FieldFormatter(lambda f: "BIT NOT NULL",
                        lambda v: "1" if v else "0"),
    Angle=FieldFormatter(lambda f: "DOUBLE",
                         lambda v: _format_number("{:.17g}", v.asDegrees())),
    String=FieldFormatter(_sql_type_for_string,
                          _format_string),
    ArrayU=FieldFormatter(lambda f: _sql_type_for_array("H", f),
                          lambda v: _format_array("H", v)),
    ArrayI=FieldFormatter(lambda f: _sql_type_for_array("i", f),
                          lambda v: _format_array("i", v)),
    ArrayF=FieldFormatter(lambda f: _sql_type_for_array("f", f),
                          lambda v: _format_array("f", v)),
    ArrayD=FieldFormatter(lambda f: _sql_type_for_array("d", f),
                          lambda v: _format_array("d", v)),
)


def canonicalize_field_name(field_name):
    """Return a MySQL-compatible version of the given field name.

    For now, the implementation simply changes all non-word characters to
    underscores.
    """
    return re.sub(r"[^\w]", "_", field_name)


def quote_mysql_identifier(identifier):
    """Return a MySQL compatible version of the given identifier.

    The given string is quoted with back-ticks, and any embedded back-ticks
    are doubled up.
    """
    return "`" + identifier.replace("`", "``") + "`"


def aliases_for(name, mappings):
    """Compute the set of possible aliases for the given field name.

    The |afw table| library processes a given field name F by replacing the
    longest prefix of F that can be found in a |schema|'s |alias map| with
    the corresponding target.  Replacement is repeated until either no such
    prefix is found, or N replacements have been made (where N is the size of
    the |alias map|).

    Parameters
    ----------

    name: str
        Field name to compute aliases for.

    mappings: sequence of (str, str)
        A sorted sequence of substitutions. Each substitution is a 2-tuple
        of strings (prefix, target).

    Returns
    -------

    set of str
        The set of aliases for `name`.
    """
    # TODO: DM-3401 may revisit afw table aliases, and this code should be
    #       updated in accordance with any changes introduced there.
    n = 0
    aliases, names = set(), set()
    names.add(name)
    while n < len(mappings) and len(names) > 0:
        # Perform one round of reverse alias substitution
        n += 1
        new_names = set()
        for name in names:
            for i, (source, target) in enumerate(mappings):
                if name.startswith(target):
                    alias = source + name[len(target):]
                    # alias is only valid if source is its longest prefix in
                    # mappings. A prefix strictly longer than source must occur
                    # after it in the sorted list of mappings.
                    valid = True
                    for s, _ in mappings[i + 1:]:
                        if not s.startswith(source):
                            break
                        if alias.startswith(s):
                            valid = False
                            break
                    if valid:
                        aliases.add(alias)
                        new_names.add(alias)
        names = new_names
    return aliases


class IngestCatalogConfig():
    """Configuration for :class:`~IngestCatalogTask`."""

    # Allow replacement of existing rows with the same unique IDs
    allow_replace = False

    # Maximum length of a query string. None means use a non-standard,
    # database- specific way to get the maximum.
    max_query_len = None

    # Maximum length of a database column name or alias. Fields that map to
    # longer column names will not be ingested.
    max_column_len = 64

    # Name of the unique ID field.
    id_field_name = None

    # A mapping from afw table field names to desired SQL column names.
    # Column names containing special characters must be quoted.
    remap = {}

    # Extra column definitions, comma-separated, to put into the CREATE TABLE
    # statement (if the table is being created).
    extra_columns = ""


class AfwTableSqlFormatter():
    """Formatter for writing AFW catalogs from Butler to mysql databases.

    .. warning::

        This formatter is incomplete.

        Incomplete data type handling: It is possible that any AFW Table object
        will successfully write to a database repository using this formatter,
        but non-pod data type information is NOT PRESERVED and the schema of
        the reloaded object will loose any related non-pod information. For
        example the Angle data type will be reloaded as a float.

        Incomplete serialization versioning support: There is currently no
        support for serialization versioning in this  code, and it should not
        be used for production output.

    A single database is a single repository.
    A table represents an afw catalog.

    Any afw catalog subclass (with an arbitrary schema) can be ingested into a
    MySQL database table.

    This task contacts a MySQL server. It requires a  ``my.cnf`` in the user's
    home directory with credentials for contacting the server.

    The ingestion process creates the destination table in the database if it
    doesn't already exist. The database schema is translated from the input
    catalog's schema, and may contain a (configurable) unique identifier
    field. The only index provided is a unique one on this field. (Additional
    ones can be created later, of course.) Additionally, a database view that
    provides the field aliases of the input catalog's schema can be created.

    Rows are inserted into the database via ``INSERT`` statements.  As many
    rows as possible are packed into each ``INSERT`` to maximize throughput.
    The limit on ``INSERT`` statement length is either set by configuration or
    determined by querying the database (in a MySQL-specific way).  This may
    not be as efficient in its use of the database as converting to CSV and
    doing a bulk load, but it eliminates the use of (often shared) disk
    resources.  The use of ``INSERT`` (committed once at the end) may not be
    fully parallelizable (particularly if a unique id index exists), but tests
    seem to indicate that it is at least not much slower to execute many
    ``INSERT`` statements in parallel compared with executing them all
    sequentially. This remains an area for future optimization.

    The important configuration parameters are:

    id_field_name:
        A unique identifier field in the input catalog. If it is specified and
        the field exists, a unique index is created for the corresponding
        column.

    max_column_len:
        Fields and field aliases with database names longer than the value of
        this parameter are automatically dropped.

    remap:
        If a canonicalized field name is problematic (say because it is too
        long, or because it matches a SQL keyword), then it can be changed by
        providing a more suitable name via this parameter.

    extra_columns:
        Extra columns (e.g. ones to be filled in later by spatial indexing
        code) can be added to the database table via this parameter.
    """

    def __init__(self):
        self.config = IngestCatalogConfig()
        self.log = Log.getLogger("fmt.mysql.AfwTableSqlFormatter")

    def read(self, table_name, host, db, port, view_name=None):
        return self._egest(table_name, host, db, port, view_name)

    def write(self, obj, table_name, host, db, port, view_name=None):
        self._ingest(obj, table_name, host, db, port, view_name)

    def _egest(self, table_name, host, db, port, view_name):
        """Create an AFW BaseCatalog from a mysql table.

        Parameters
        ----------
        table_name : str
            Name of the database table to create.
        host : str
            Name of the database host machine.
        db : str
            Name of the database to ingest into.
        port : int
            Port number on the database host.
        view_name : str
            Name of the database view to create.

        Returns
        -------
        lsst.afw.table.BaseCatalog
            The BaseCatalog instantiated from data in mysql.
        """
        table_name = quote_mysql_identifier(table_name)
        view_name = quote_mysql_identifier(view_name) if view_name else None
        with closing(self.connect(host, port, db)) as conn:
            sql = "SELECT * FROM {}".format(table_name)
            c = conn.cursor()
            c.execute(sql)

            keys, schema = self._egest_makeSchema(c.description)
            catalog = afw_table.BaseCatalog(schema)
            while True:
                row = c.fetchone()
                if row is None:
                    break
                record = catalog.addNew()
                for key, val in zip(keys, row):
                    record.set(key, val)
        return catalog

    @staticmethod
    def _egest_makeSchema(rowDescription):
        """Make the schema for egesting from database to AFW Table

        Parameters
        ----------
        rowDescription : tuple of tuples
            The description of the connection object. See pep-0249 .description
            under Cursor attributes

        Returns
        -------
        tuple (list of keys, lsst.afw.table.Schema)
            Description
        """
        schema = afw_table.Schema()
        keys = []
        for col in rowDescription:
            keys.append(
                schema.addField(col[0],
                                AfwTableSqlFormatter._egest_getType(col[1])))
        return keys, schema

    @staticmethod
    def _egest_getType(typecode):
        """Transform the connection.description typecode field to a python
        Type.

        Parameters
        ----------
        typecode : int
            The type_cde from a Cursor Object's description. The type_code must
            compare equal to one of Type Objects. Note that Type Objects may be
            equal to more than one type code (e.g. DATETIME could be equal to
            the type codes for date, time and timestamp columns). See pep-0249
            in the Type Objects and Constructors section for more details.

        Returns
        -------
        class object
            Python class object that matches the type_code.

        Raises
        ------
        NotImplementedError
            Not all type_code values are implemented. Raises when a type_code
            is passed in but not implemented. (We should eventually implement
            all of the type_codes and/or implement the AFW Table metadata
            translation table.)
        """
        if MySQLdb.Date == typecode:
            raise NotImplementedError()
        elif MySQLdb.Time == typecode:
            raise NotImplementedError()
        elif MySQLdb.Timestamp == typecode:
            raise NotImplementedError()
        elif MySQLdb.DateFromTicks == typecode:
            raise NotImplementedError()
        elif MySQLdb.TimeFromTicks == typecode:
            raise NotImplementedError()
        elif MySQLdb.TimestampFromTicks == typecode:
            raise NotImplementedError()
        elif MySQLdb.Binary == typecode:
            raise NotImplementedError()
        elif MySQLdb.STRING == typecode:
            raise NotImplementedError()
        elif MySQLdb.BINARY == typecode:
            raise NotImplementedError()
        elif MySQLdb.NUMBER == typecode:
            return float
        elif MySQLdb.DATETIME == typecode:
            raise NotImplementedError()
        elif MySQLdb.ROWID == typecode:
            raise NotImplementedError()
        elif MySQLdb.SQL == typecode:
            raise NotImplementedError()

    def _ingest(self, cat, table_name, host, db, port, view_name=None):
        """Ingest an |afw catalog| passed as an object.

        Parameters
        ----------
        cat : lsst.afw.table.BaseCatalog or subclass
            Catalog to ingest.
        table_name : str
            Name of the database table to create.
        host : str
            Name of the database host machine.
        db : str
            Name of the database to ingest into.
        port : int
            Port number on the database host.
        view_name : str
            Name of the database view to create.
        """
        table_name = quote_mysql_identifier(table_name)
        view_name = quote_mysql_identifier(view_name) if view_name else None
        with closing(self.connect(host, port, db)) as conn:
            # Determine the maximum query length (MySQL-specific) if not
            # configured.
            if self.config.max_query_len is None:
                with closing(conn.cursor()) as cursor:
                    cursor.execute(
                        """SELECT variable_value
                        FROM information_schema.session_variables
                        WHERE variable_name = 'max_allowed_packet'
                        """
                    )
                    max_query_len = int(cursor.fetchone()[0])
            else:
                max_query_len = self.config.max_query_len
            self.log.debug("max_query_len: %d", max_query_len)
            tempTableName = self._create_table(conn, cat.schema)
            if view_name is not None:
                self._create_view(conn, tempTableName, view_name, cat.schema)
            self._do_ingest(conn, cat, tempTableName, max_query_len)
            self._rename_table(conn, tempTableName, table_name)

    @staticmethod
    def connect(host, port, db):
        """Connect to the specified MySQL database server."""
        # TODO we probably want to support the other standard cnf file
        # locations
        return MySQLdb.connect(host=host, port=port, db=db,
                               read_default_file="~/.my.cnf")

    def _execute_sql(self, conn, sql):
        """Execute a SQL query with no expectation of a result."""
        self.log.debug(sql)
        conn.query(sql)

    def _schema_items(self, schema):
        """Yield ingestible schema items."""
        for item in schema:
            field = item.field
            if field.getTypeString() not in field_formatters:
                self.log.warn("Skipping field %s: type %s not supported",
                              field.getName(), field.getTypeString())
            else:
                column = self._column_name(field.getName())
                if len(column) > self.config.max_column_len:
                    self.log.warn("Skipping field %s: column name %d too long",
                                  field.getName(), column)
                else:
                    yield item

    def _do_ingest(self, conn, cat, table_name, max_query_len):
        """Ingest an afw catalog.

        This is accomplished by converting it to one or more (large) INSERT or
        REPLACE statements, executing those statements, and committing the
        result.
        """
        sql_prefix = "REPLACE" if self.config.allow_replace else "INSERT"
        sql_prefix += " INTO {} (".format(table_name)
        keys = []
        column_names = []
        for item in self._schema_items(cat.schema):
            keys.append((item.key, field_formatters[item.field.getTypeString()]))
            column_names.append(self._column_name(item.field.getName()))
        sql_prefix += ",".join(column_names)
        sql_prefix += ") VALUES "
        pos = 0
        while pos < len(cat):
            sql = sql_prefix
            initial_pos = pos
            max_value_len = max_query_len - len(sql)
            while pos < len(cat):
                row = cat[pos]
                value = "("
                value += ",".join([f.format_value(row.get(k)) for (k, f) in keys])
                value += "),"
                max_value_len -= len(value)
                if max_value_len < 0:
                    break
                else:
                    sql += value
                    pos += 1
            if pos == initial_pos:
                # Have not made progress
                raise RuntimeError("Single row is too large to insert")
            self._execute_sql(conn, sql[:-1])
        conn.commit()

    def _column_name(self, field_name):
        """Return the SQL column name for the given afw table field."""
        if field_name in self.config.remap:
            return self.config.remap[field_name]
        return canonicalize_field_name(field_name)

    def _column_def(self, field):
        """Return the SQL column definition for the given afw table field."""
        sql_type = field_formatters[field.getTypeString()].sql_type(field)
        return self._column_name(field.getName()) + " " + sql_type

    def _create_table(self, conn, schema):
        """Create a table corresponding to the given afw table schema, indented
        to be temporary for writing into safely in the presence of paralell
        database access.

        Any extra columns specified in the task config are added in. If a
        unique id column exists, it is given a key.

        Returns the name of the table that was created for writing into.
        """
        fields = [item.field for item in self._schema_items(schema)]
        names = [f.getName() for f in fields]
        equivalence_classes = {}
        for name in names:
            equivalence_classes.setdefault(name.lower(), []).append(name)
        clashes = ',\n'.join('\t{' + ', '.join(c) + '}'
                             for c in equivalence_classes.itervalues() if len(c) > 1)
        if clashes:
            raise RuntimeError(
                "Schema contains columns that differ only by non-word "
                "characters and/or case:\n{}\nIn the database, these cannot "
                "be distinguished and hence result in column name duplicates. "
                "Use the remap configuration parameter to resolve this "
                "ambiguity.".format(clashes)
            )
        tableName = next(tempfile._get_candidate_names())
        sql = "CREATE TABLE {} (\n\t".format(tableName)
        sql += ",\n\t".join(self._column_def(field) for field in fields)
        if self.config.extra_columns:
            sql += ",\n\t" + self.config.extra_columns
        if self.config.id_field_name:
            if self.config.id_field_name in names:
                sql += ",\n\tUNIQUE({})".format(
                    self._column_name(self.config.id_field_name))
            else:
                self.log.warn(
                    "No field matches the configured unique ID field name "
                    "(%s)", self.config.id_field_name)
        sql += "\n)"
        self._execute_sql(conn, sql)
        return tableName

    def _rename_table(self, conn, from_name, to_name):
        """Rename a table, deleting the previous table at to_name if needed."""
        self._testSqlIsClean(from_name)
        self._testSqlIsClean(to_name)
        cursor = conn.cursor()
        num_tries = 3
        for i in range(num_tries):
            try:
                cursor.execute("RENAME TABLE {} TO {}".format(from_name,
                                                              to_name))
                return
            except MySQLdb.OperationalError as e:
                if e[0] == MYSQL_ER_TABLE_EXISTS_ERROR:
                    cursor.execute("DROP TABLE {}".format(to_name))
        self.log.warn(
            ("Could not rename temp table to final dataset name {}, " +
             "perhaps another process is thrashing against this one?").format(
                to_name))

    def _create_view(self, conn, table_name, view_name, schema):
        """Create a view allowing columns to be referred to by their aliases."""
        sql = ("CREATE OR REPLACE "
               "ALGORITHM = MERGE SQL SECURITY INVOKER "
               "VIEW {} AS SELECT\n\t").format(view_name)
        with closing(conn.cursor()) as cursor:
            cursor.execute("SHOW COLUMNS FROM " + table_name)
            column_names = [row[0] for row in cursor.fetchall()]
        sql += ",\n\t".join(column_names)
        # Technically, this isn't quite right. In afw, it appears to be legal
        # for an alias to shadow an actual field name. So for full rigor,
        # shadowed field names would have to be removed from the column_names
        # list.
        #
        # For now, construct an invalid view and fail in this case.
        mappings = sorted((s, t) for (s, t) in schema.getAliasMap().iteritems())
        for item in self._schema_items(schema):
            field_name = item.field.getName()
            aliases = sorted(aliases_for(field_name, mappings))
            column = self._column_name(field_name)
            for a in aliases:
                alias = self._column_name(a)
                if len(alias) > self.config.max_column_len:
                    self.log.warn("Skipping alias %s for %d: "
                                  "alias too long", alias, column)
                    continue
                sql += ",\n\t{} AS {}".format(column, alias)
        sql += "\nFROM "
        sql += table_name
        self._execute_sql(conn, sql)

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

MySqlStorage.registerFormatter(afw_table.BaseCatalog, AfwTableSqlFormatter)
