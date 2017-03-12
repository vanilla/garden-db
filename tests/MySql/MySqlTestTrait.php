<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\MySql;

use Garden\Db\Db;
use Garden\Db\MySqlDb;
use PDO;

trait MySqlTestTrait {
    protected abstract static function getPx();

    /**
     * Get the database connection for the test.
     *
     * @return Db Returns the db object.
     */
    protected static function createDb() {
        $pdo = new PDO(
            "mysql:host=127.0.0.1;charset=utf8",
            'travis',
            '',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
        $pdo->query("create database if not exists `phpunit_garden`");
        $pdo->query("use `phpunit_garden`");

        $db = new MySqlDb($pdo);

        $db->setPx(static::getPx());


        return $db;
    }
}
