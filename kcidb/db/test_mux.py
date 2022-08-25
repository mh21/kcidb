"""kcdib.db.mux module tests"""

from itertools import zip_longest
import textwrap
from kcidb_io.schema import V1, V2, V3, V4, V5
from kcidb.unittest import TestCase, local_only
from kcidb.db.mux import Driver as MuxDriver
from kcidb.db.null import Driver as NullDriver


class DummyDriver(NullDriver):
    """A dummy driver with configurable number of schemas"""

    @classmethod
    def get_doc(cls):
        """
        Get driver documentation.

        Returns:
            The driver documentation string.
        """
        return textwrap.dedent("""\
            The dummy driver allows creating null drivers supporting
            specifying minimum and maximum I/O schema major version numbers,
            number of major schema versions per I/O schema, major schema
            version number increment, initial major schema version, initial
            minor schema version, and number of minor versions per major
            version.

            Parameters: <MIN_IO_MAJOR>:
                        <MAX_IO_MAJOR>:
                        <MAJORS_PER_IO_SCHEMA>:
                        <MAJOR_STEP>:
                        <MAJOR>:
                        <MINOR>:
                        <MINORS_PER_MAJOR>

                        All optional, default being "1:5:1:1:0:0:1".
        """)

    def __init__(self, params):
        """
        Initialize the driver.

        Args:
            params: A string describing how many and which schema versions the
                    driver should have. See the return value of get_doc() for
                    details.
        """
        assert params is None or isinstance(params, str)
        super().__init__(None)

        min_io_major, max_io_major, \
            majors_per_io_schema, self.major_step, \
            self.major, self.minor, self.minors_per_major = (
                default if param is None else param
                for param, default in zip_longest(
                    (int(number) for number in (
                        params.split(":") if params else []
                    )),
                    (1, 5, 1, 1, 0, 0, 1)
                )
            )
        assert min_io_major <= max_io_major
        assert majors_per_io_schema >= 1
        assert self.major_step >= 1
        assert self.major >= 0
        assert self.major % self.major_step == 0
        assert self.minor >= 0
        assert self.minors_per_major >= 1
        assert self.minor < self.minors_per_major
        io_schemas = filter(
            lambda x: min_io_major <= x.major <= max_io_major,
            V5.history
        )
        self.schemas = [
            io_schema
            for io_schema in io_schemas
            for i in range(0, majors_per_io_schema * self.minors_per_major)
        ]
        assert \
            0 <= (self.major / self.major_step * self.minors_per_major +
                  self.minor) < len(self.schemas)
        self.params = params

    def __repr__(self):
        return f"Dummy<{self.params}>"

    def get_schemas(self):
        """
        Retrieve available driver schemas: a dictionary of major version
        numbers of the driver's schemas (non-negative integers), and
        corresponding I/O schemas (kcidb_io.schema.abstract.Version instances)
        supported by them.

        Returns:
            The schema dictionary.
        """
        return {
            (int(index / self.minors_per_major * self.major_step),
             index % self.minors_per_major): io_schema
            for index, io_schema in enumerate(self.schemas)
        }

    def get_schema(self):
        """
        Get the driven database schema's major version number and the I/O
        schema supported by it. The database must be initialized.

        Returns:
            The major version number (a non-negative integer) of the database
            schema and the I/O schema (a kcidb_io.schema.abstract.Version)
            supported by it.
        """
        return \
            (self.major, self.minor), \
            self.schemas[int(self.major / self.major_step) *
                         self.minors_per_major]

    def upgrade(self, target_version):
        """
        Upgrade the database to the specified schema.
        The database must be initialized.

        Args:
            target_version: A tuple of the major and minor version numbers of
                            the schema to upgrade to (must be one of the
                            database's available schema versions, newer than
                            the current one).
        """
        assert self.is_initialized()
        assert isinstance(target_version, tuple)
        assert len(target_version) == 2
        assert target_version in self.get_schemas(), \
            "Target schema version is not available for the driver"
        assert target_version >= (self.major, self.minor), \
            "Target schema is older than the current schema"
        (self.major, self.minor) = target_version


class DummyMuxDriver(MuxDriver):
    """A driver muxing dummy drivers"""

    @classmethod
    def get_drivers(cls):
        """
        Retrieve a dictionary of driver names and types available for driver's
        control.

        Returns:
            A driver dictionary.
        """
        return dict(dummy=DummyDriver)


@local_only
class MuxDriverTestCase(TestCase):
    """Test case for the Mux driver"""

    def setUp(self):
        """Setup tests"""
        # pylint: disable=invalid-name
        self.maxDiff = None

    def test_param_parsing(self):
        """Check that parameters are parsed correctly"""
        # Single driver without parameters
        driver = DummyMuxDriver("dummy")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V2, (2, 0): V3, (3, 0): V4, (4, 0): V5
        })
        # Two drivers without parameters
        driver = DummyMuxDriver("dummy dummy")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V1, (2, 0): V2, (3, 0): V2,
            (4, 0): V3, (5, 0): V3, (6, 0): V4, (7, 0): V4,
            (8, 0): V5
        })
        # Single driver with parameters
        driver = DummyMuxDriver("dummy:1:3")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V2, (2, 0): V3
        })
        # Two drivers with parameters
        driver = DummyMuxDriver("dummy:1:3 dummy:1:3")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V1, (2, 0): V2, (3, 0): V2, (4, 0): V3
        })
        # First driver with parameters
        driver = DummyMuxDriver("dummy:1:3 dummy")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V1, (2, 0): V2, (3, 0): V2,
            (4, 0): V3, (5, 0): V3, (6, 0): V3
        })
        # Second driver with parameters
        driver = DummyMuxDriver("dummy dummy:1:3")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V1, (2, 0): V2, (3, 0): V2,
            (4, 0): V3, (5, 0): V3, (6, 0): V3
        })

        # Newline separation
        driver = DummyMuxDriver("dummy\ndummy")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V1, (2, 0): V2, (3, 0): V2, (4, 0): V3,
            (5, 0): V3, (6, 0): V4, (7, 0): V4, (8, 0): V5
        })

        # Long separation
        driver = DummyMuxDriver("dummy \r\n\t\vdummy")
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V1, (2, 0): V2, (3, 0): V2, (4, 0): V3,
            (5, 0): V3, (6, 0): V4, (7, 0): V4, (8, 0): V5
        })

    def test_schemas(self):  # It's OK, pylint: disable=too-many-branches
        """Check that schemas are enumerated and are upgradable"""
        self.maxDiff = None  # pylint: disable=invalid-name

        # Single driver with simple version history
        driver = DummyMuxDriver("dummy")
        self.assertEqual(driver.get_schema(), ((0, 0), V1))
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V2, (2, 0): V3, (3, 0): V4, (4, 0): V5,
        })
        for version in driver.get_schemas():
            if version > driver.get_schema()[0]:
                driver.upgrade(version)
        self.assertEqual(driver.get_schema(), ((4, 0), V5))

        # Multiple drivers starting with different I/O versions
        driver = DummyMuxDriver("""
            dummy:1:5:1:1:0
            dummy:1:5:2:1:2
            dummy:1:5:3:1:6
        """)
        self.assertEqual(driver.get_schema(), ((0, 0), V1))
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V2, (2, 0): V2, (3, 0): V2,
            (4, 0): V3, (5, 0): V3, (6, 0): V3, (7, 0): V3,
            (8, 0): V3, (9, 0): V3, (10, 0): V4, (11, 0): V4,
            (12, 0): V4, (13, 0): V4, (14, 0): V4, (15, 0): V4,
            (16, 0): V5, (17, 0): V5, (18, 0): V5, (19, 0): V5,
        })
        for version in driver.get_schemas():
            if version > driver.get_schema()[0]:
                driver.upgrade(version)
        self.assertEqual(driver.get_schema(), ((19, 0), V5))

        # Staggered driver schema version numbers
        driver = DummyMuxDriver("""
            dummy:1:5:1:3:0
            dummy:1:5:2:2:4
            dummy:1:5:3:1:6
        """)
        self.assertEqual(driver.get_schema(), ((0, 0), V1))
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V2, (2, 0): V2, (3, 0): V2,
            (4, 0): V3, (5, 0): V3, (6, 0): V3, (7, 0): V3,
            (8, 0): V3, (9, 0): V3, (10, 0): V4, (11, 0): V4,
            (12, 0): V4, (13, 0): V4, (14, 0): V4, (15, 0): V4,
            (16, 0): V5, (17, 0): V5, (18, 0): V5, (19, 0): V5,
        })
        for version in driver.get_schemas():
            if version > driver.get_schema()[0]:
                driver.upgrade(version)
        self.assertEqual(driver.get_schema(), ((19, 0), V5))

        # Misaligned I/O version histories
        driver = DummyMuxDriver("""
            dummy:2:5:1:1:0
            dummy:1:4:1:1:0
            dummy:2:3:1:1:0
        """)
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V2, (2, 0): V2, (3, 0): V2,
            (4, 0): V3, (5, 0): V3, (6, 0): V3, (7, 0): V3
        })
        for version in driver.get_schemas():
            if version > driver.get_schema()[0]:
                driver.upgrade(version)
        self.assertEqual(driver.get_schema(), ((7, 0), V3))

        # Disconnected I/O version histories
        driver = DummyMuxDriver("""
            dummy:1:2:1:1:0
            dummy:4:5:1:1:0
        """)
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V2, (2, 0): V2
        })
        for version in driver.get_schemas():
            if version > driver.get_schema()[0]:
                driver.upgrade(version)
        self.assertEqual(driver.get_schema(), ((2, 0), V2))

        # Multiple minor versions
        driver = DummyMuxDriver("""
            dummy:1:5:1:1:0:0:1
            dummy:1:5:1:1:0:0:2
        """)
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (1, 0): V1, (1, 1): V1,
            (2, 0): V2, (3, 0): V2, (3, 1): V2,
            (4, 0): V3, (5, 0): V3, (5, 1): V3,
            (6, 0): V4, (7, 0): V4, (7, 1): V4,
            (8, 0): V5, (8, 1): V5,
        })
        for version in driver.get_schemas():
            if version > driver.get_schema()[0]:
                driver.upgrade(version)
        self.assertEqual(driver.get_schema(), ((8, 1), V5))

        # Multiple minor versions
        driver = DummyMuxDriver("""
            dummy:1:5:1:1:0:0:2
            dummy:1:5:1:1:0:0:1
        """)
        self.assertEqual(driver.get_schemas(), {
            (0, 0): V1, (0, 1): V1,
            (1, 0): V1, (2, 0): V2, (2, 1): V2,
            (3, 0): V2, (4, 0): V3, (4, 1): V3,
            (5, 0): V3, (6, 0): V4, (6, 1): V4,
            (7, 0): V4, (8, 0): V5, (8, 1): V5,
        })
        for version in driver.get_schemas():
            if version > driver.get_schema()[0]:
                driver.upgrade(version)
        self.assertEqual(driver.get_schema(), ((8, 1), V5))
