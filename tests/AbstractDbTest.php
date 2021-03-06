<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Faker\Internet;
use Faker\Name;
use Garden\Db\Db;
use Garden\Db\TableDef;
use PHPUnit\Framework\TestCase;

/**
 * The base class for database tests.
 */
abstract class AbstractDbTest extends TestCase {
    /**
     * @var Db The database connection for the tests.
     */
    protected static $db;

    /**
     * Set up the db link for the test cases.
     */
    public static function setUpBeforeClass() {
        // Drop all of the tables in the database.
        $db = static::createDb();
        $tables = $db->fetchTableNames();
        array_map([$db, 'dropTable'], $tables);

        self::$db = $db;
        static::createTestTable(24);
    }

    protected static function getPx() {
        $px = 'db_';
        if (preg_match('`Db(.+)Test$`i', get_called_class(), $m)) {
            $px = (strtolower($m[1]) ?: 'db').'_';
        }

        return $px;
    }

    /**
     * Get the database connection for the test.
     *
     * @return Db Returns the db object.
     */
    protected static function createDb() {
        return null;
    }

    protected static function createTestTable($rows = 12) {
        $db = static::$db;
        $def = new TableDef('test');

        $def->setPrimaryKey('id', 'uint')
            ->setColumn('name', 'varchar(50)')
            ->setColumn('gid', 'int', 0)
            ->setColumn('num', 'int', 0)
            ->setColumn('eta', 'datetime', null)
            ->exec($db);

        $fn = function () use ($rows) {
            for ($i = 0; $i < $rows; $i++) {
                yield static::provideTestRow();
            }
        };

        $db->load('test', $fn());
    }

    protected static function provideTestRow() {
        $i = mt_rand();

        $timeZones = ['UTC', 'America/Montreal', 'America/Vancouver', 'Europe/London', 'Asia/Tokyo'];

        $tz = new \DateTimeZone($timeZones[$i % count($timeZones)]);

        $r = [
            'name' => Name::name(),
            'gid' => $i % 3,
            'num' => $i % 4,
            'eta' => new \DateTimeImmutable(\Faker\DateTime::dateTime(), $tz)
        ];

        return $r;
    }

    /**
     * Get the database def.
     *
     * @return TableDef Returns the db def.
     */
    protected static function createTableDef() {
        return new TableDef(self::$db);
    }

    /**
     * Assert that two table definitions are equal.
     *
     * @param array $expected The expected table definition.
     * @param array $actual The actual table definition.
     * @param bool $subset Whether or not expected can be a subset of actual.
     */
    public function assertDefEquals($expected, $actual, $subset = true) {
        $this->assertEquals($expected['name'], $actual['name'], "Table names are not equal.");
        $tablename = $actual['name'];

        $colsExpected = $expected['columns'];
        $colsActual = $actual['columns'];

        if ($subset) {
            $colsActual = array_intersect_key($colsActual, $colsExpected);
        }
        $this->assertEquals($colsExpected, $colsActual, "$tablename columns are not the same.");

        $ixExpected = (array)$expected['indexes'];
        $ixActual = (array)$actual['indexes'];

        $isExpected = [];
        foreach ($ixExpected as $ix) {
            $type = $ix['type'] ?: Db::INDEX_IX;
            $isExpected[] = $type.'('.implode(', ', $ix['columns']).')';
        }
        sort($isExpected);

        $isActual = [];
        foreach ($ixActual as $ix) {
            $type = $ix['type'] ?: Db::INDEX_IX;
            $isActual[] = $type.'('.implode(', ', $ix['columns']).')';
        }

        if ($subset) {
            $isActual = array_intersect($isActual, $isExpected);
        }
        sort($isActual);
        $this->assertEquals($isExpected, $isActual, "$tablename indexes are not the same.");
    }

    /**
     * Assert that a dataset array is ordered by appropriate columns.
     *
     * @param \Traversable $dataset The dataset to check.
     * @param array $order An array of column names, optionally starting with "-".
     */
    public function assertOrder($dataset, ...$order) {
        $cmp = function ($a, $b) use ($order) {
            foreach ($order as $column) {
                $desc = 1;
                if ($column[0] === '-') {
                    $desc = -1;
                    $column = substr($column, 1);
                }

                $r = strnatcmp($a[$column], $b[$column]);

                if ($r !== 0) {
                    return $r * $desc;
                }
            }
            return 0;
        };

        $array = iterator_to_array($dataset);

        $actual = $array;
        usort($array, $cmp);

        $this->assertEquals($array, $actual);
    }

    /**
     * Provide some random user rows.
     *
     * @param int $count The number of users to provide.
     * @return \Generator Returns a {@link \Generator} of users.
     */
    public function provideUsers($count = 10) {
        for ($i = 0; $i < $count; $i++) {
            yield $this->provideUser();
        }
    }

    /**
     * Provide a single random user.
     *
     * @param string $fullName The full name of the user.
     * @return array
     */
    public function provideUser($fullName = '') {
        static $num = 0;

        if (empty($fullName)) {
            $fullName = Name::name();
        }

        $user = [
            'name' => Internet::userName($fullName),
            'email' => Internet::email($fullName),
            'fullName' => $fullName,
            'insertTime' => time(),
            'num' => ($num++) % 3
        ];

        return $user;
    }

    /**
     * Generate a new user table for testing.
     *
     * @param string $name The name of the table.
     * @return TableDef
     */
    protected function getUserTableDef($name) {
        $def = new TableDef($name);
        $def->setPrimaryKey('userID')
            ->setColumn('name', 'varchar(50)')
            ->setColumn('email', 'varchar(255)')
            ->setColumn('fullName', 'varchar(50)')
            ->setColumn('insertTime', 'int')
            ->setColumn('num', 'int', 0)
            ->addIndex(Db::INDEX_IX, 'name');

        return $def;
    }

    /**
     * Create and populate a user table.
     *
     * @param string $name The name of the table to create.
     * @param int $count The number of users to insert.
     */
    protected function createPopulatedUserTable($name, $count = 10) {
        $this->getUserTableDef($name)->exec(self::$db);

        self::$db->load($name, $this->provideUsers($count));
    }
}
