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
    protected static function getPx() {
        return '';
    }

    /**
     * Get the database connection for the test.
     *
     * @return Db Returns the db object.
     */
    protected static function createDb() {
        $attr = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true
        ];

        if (getenv('TRAVIS')) {
            $pdo = new PDO(
                "mysql:host=127.0.0.1;charset=utf8mb4",
                'travis',
                '',
                $attr
            );
            $pdo->query("create database if not exists `phpunit_garden`");
            $pdo->query("use `phpunit_garden`");
        } else {
            $pdo = new PDO(
                "mysql:host=127.0.0.1;dbname=phpunit_garden;charset=utf8mb4",
                'travis',
                '',
                $attr
            );
        }

        $db = new MySqlDb($pdo);
        $db->setPx(static::getPx());


        return $db;
    }
}
