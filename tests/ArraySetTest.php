<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Faker\Name;
use Garden\Db\ArraySet;
use Garden\Db\TableQuery;
use Garden\Db\Utils\ArrayObject;

/**
 * Tests for the {@link ArraySet} class.
 */
abstract class ArraySetTest extends AbstractDbTest {
    /**
     * Test paging through an {@link ArraySet}.
     */
    public function testPaging() {
        $arr = new ArraySet(self::$db->get('test', []));
        $tbl = new TableQuery('test', [], self::$db);

        $arr->setLimit(4);
        $tbl->setLimit(4);

        for ($p = 1; $p < 4; $p++) {
            $ra = $arr->fetchAll();
            $rt = $tbl->fetchAll();

            $this->assertEquals($rt, $ra);
        }
    }

    /**
     * Test calculated counts.
     */
    public function testCount() {
        $arr = new ArraySet($this->provideUsers(10));

        $arr->setLimit(7);

        $this->assertSame(7, $arr->count());
        $arr->setPage(2);
        $this->assertSame(3, $arr->count());
    }

    /**
     * Test in-place sorting.
     */
    public function testSorting() {
        $arr = new ArraySet($this->provideUsers(10));

        $arr->setOrder('num', '-email');
        $this->assertOrder($arr, ...$arr->getOrder());
    }

    public function testResorting() {
        $arr = new ArraySet([]);

        $arr->setOrder('num', '-email');
        $arr->setData($this->provideUsers(10));
        $this->assertOrder($arr, ...$arr->getOrder());
        $arr->setOrder('name');
        $this->assertOrder($arr, ...$arr->getOrder());
    }
}
