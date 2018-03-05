<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\Sqlite;

use Garden\Db\Db;
use Garden\Db\Drivers\SqliteDb;
use PDO;

trait SqliteTestTrait {
    protected static function getPx() {
        $px = 'db_';
        if (preg_match('`Sqlite(?:Db)?([a-z]+?)Test$`i', get_called_class(), $m)) {
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
        $px = static::getPx();

        if (getenv('TRAVIS')) {
            $path = ':memory:';
        } else {
            $path = __DIR__."/../cache/{$px}test.sqlite";
        }
        $pdo = new PDO("sqlite:$path", null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

        $class = Db::driverClass($pdo);
        $db = new $class($pdo);
        $db->setPx($px);

        return $db;
    }
}
