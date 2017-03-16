<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;


use Garden\Db\Aggregate;
use Garden\Db\Increment;

abstract class LiteralTest extends AbstractDbTest {
    /**
     * Test incrementing.
     */
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

    /**
     * Test aggregates.
     */
    public function testAggregate() {
        $db = self::$db;

        $allRows = $db->get('test', [])->fetchAll();

        $count = $db->getOne('test', [], ['columns' => [new Aggregate(Aggregate::COUNT, '*')]])['count'];
        $this->assertEquals(count($allRows), $count);

        $sum = $db->getOne('test', [], ['columns' => [new Aggregate(Aggregate::SUM, 'num')]])['sum'];
        $allSum = array_sum(array_column($allRows, 'num'));
        $this->assertEquals($allSum, $sum);
    }
}
