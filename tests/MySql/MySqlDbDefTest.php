<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests\MySql;

use Garden\Db\MySqlDb;
use Garden\Db\Tests\DbDefTest;
use PDO;

/**
 * Run the {@link DbDefTest} against {@link MySqlDb}.
 */
class MySqlDbDefTest extends DbDefTest {
    /**
     * Create the {@link MySqlDb}.
     *
     * @return \Garden\Db\MySqlDb Returns the new database connection.
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

        $db->setPx('gdndef_');


        return $db;
    }
}
