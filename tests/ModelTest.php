<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Garden\Db\Model;

/**
 * Tests for the {@link Model} class.
 */
abstract class ModelTest extends AbstractDbTest {
    /**
     * A basic model smoke test.
     */
    public function testModelInsertAndGetID() {
        $name = __FUNCTION__;

        $this->createPopulatedUserTable($name, 0);
        $model = new Model($name, self::$db);

        $user = $this->provideUser('Tom Bombadil');
        $id = $model->insert($user);
        $dbUser = $model->getID($id);

        $this->assertEquals($id, $dbUser['userID']);
        unset($dbUser['userID']);
        $this->assertEquals($user, $dbUser);
    }

    public function testModelRepeatableFetch() {
        $name = __FUNCTION__;

        $this->createPopulatedUserTable($name, 10);
        $model = new Model($name, self::$db);

        $rows = $model->get([])->setLimit(5);

        foreach ($rows as $i => $row) {
        }

        foreach ($rows as $j => $row) {
        }

        $data = iterator_to_array($rows);

        $this->assertSame($i, $j);
    }
}
