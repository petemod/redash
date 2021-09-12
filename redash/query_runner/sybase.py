import logging
import sys
import uuid

from redash.query_runner import *
from redash.utils import json_dumps, json_loads

logger = logging.getLogger(__name__)

try:
    import jaydebeapi
    from jaydebeapi import DatabaseError

    enabled = True
except ImportError:
    enabled = False

# from _mssql.pyx ## DB-API type definitions & http://www.freetds.org/tds.html#types ##
types_map = {
    1: TYPE_STRING,
    2: TYPE_STRING,
    # Type #3 supposed to be an integer, but in some cases decimals are returned
    # with this type. To be on safe side, marking it as float.
    3: TYPE_FLOAT,
    4: TYPE_DATETIME,
    5: TYPE_FLOAT,
}


class SybaseServer(BaseSQLQueryRunner):
    should_annotate_query = False
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "user": {"type": "string"},
                "password": {"type": "string"},
                "server": {"type": "string", "default": "127.0.0.1"},
                "port": {"type": "number", "default": 4001},
                "driver": {
                    "type": "string",
                    "default": "jdbc:sybase:Tds",
                    "title": "JDBC Driver",
                },
                "charset": {
                    "type": "string",
                    "default": "UTF-8",
                    "title": "Character Set",
                },
                "db": {"type": "string", "title": "Database Name"},
                "extra_params": {"type": "string", "title": "Extra connection paramaters", "default": "?charset=utf8"},
                "startup_command": {"type": "string", "title": "Startup Command"},
            },
            "required": ["db"],
            "secret": ["password"],
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def name(cls):
        return "Sybase ASE"

    @classmethod
    def type(cls):
        return "sybase"

    def _get_tables(self, schema):
        query = """SELECT
NULL TABLE_SCHEMA,
so.name name,
sc.name name1
FROM 
.dbo.sysobjects so
INNER JOIN 
.dbo.syscolumns sc
ON sc.id = so.id
inner join systypes st on st.usertype = sc.usertype 
where so.type <> 'S'
        """

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        results = json_loads(results)

        for row in results["rows"]:
            table_name = row["name"]

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(row["name1"])

        return list(schema.values())

    def run_query(self, query, user):
        connection = None

        try:
            server = self.configuration.get("server", "")
            user = self.configuration.get("user", "")
            password = self.configuration.get("password", "")
            db = self.configuration["db"]
            port = self.configuration.get("port", 4001)
            driver = self.configuration.get("driver", "jdbc:sybase:Tds")
            charset = self.configuration.get("charset", "UTF-8")
            params = self.configuration.get("extra_params", "?charset=utf8")

            startup = self.configuration.get("startup_command", "")

            conn_str = "%s:%s:%s/%s%s" % (driver, server, port, db, params)

            logger.warning("Sybase conn_str: %s" % conn_str)

            connection = jaydebeapi.connect('com.sybase.jdbc3.jdbc.SybDriver', conn_str, [user, password],
                                            '/app/java/jconn3.jar')

            if isinstance(query, str):
                query = query.encode(charset)

            if len(startup) > 0:
                sqls = startup.split(';')
                for sql in sqls:
                    curs = connection.cursor()
                    curs.execute(sql)
                    curs.close()

            cursor = connection.cursor()
            logger.debug("Sybase running query: %s", query)

            cursor.execute(query)
            data = cursor.fetchall()

            if cursor.description is not None:
                columns = self.fetch_columns(
                    [(i[0], types_map.get(i[1], None)) for i in cursor.description]
                )
                rows = [
                    dict(zip((column["name"] for column in columns), row))
                    for row in data
                ]

                data = {"columns": columns, "rows": rows}
                json_data = json_dumps(data)
                error = None
            else:
                error = "No data was returned."
                json_data = None

            cursor.close()
        except DatabaseError as e:
            logger.error(e)
            error = e.args[0].args[0]
            json_data = None
        except (KeyboardInterrupt, JobTimeoutException):
            connection.cancel()
            raise
        except Exception as e:
            logger.error(e)
            error = e.args[0]
            json_data = None
        finally:
            if connection:
                connection.close()
        return json_data, error


register(SybaseServer)
