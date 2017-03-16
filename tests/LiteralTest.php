<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;


use Garden\Db\Increment;

abstract class LiteralTest extends AbstractDbTest {
    public function testIncrement() {
        $db = self::$db;

        $id = $db->insert('test', static::provideTestRow());
        $row = $db->getOne('test', ['id' => $id]);

        $db->update('test', ['num' => new Increment(1)], ['id' => $id]);
        $row2 = $db->getOne('test', ['id' => $id]);
        $this->assertEquals($row['num'] + 1, $row2['num']);

        $db->update('test', ['num' => new Increment(-1)], ['id' => $id]);
        $row3 = $db->getOne('test', ['id' => $id]);
        $this->assertEquals($row['num'], $row3['num']);
    }
}
