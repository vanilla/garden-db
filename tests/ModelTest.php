<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Garden\Db\Model;
use Garden\Db\TableDef;
use Garden\Db\Utils\ArrayObject;
use PDO;

/**
 * Tests for the {@link Model} class.
 */
abstract class ModelTest extends AbstractDbTest {
    /**
     * A basic model smoke test.
     */
    public function testModelInsertAndGetID() {
        $model = $this->createUserModel(__FUNCTION__, 0);

        $user = $this->provideUser('Tom Bombadil');
        $id = $model->insert($user);
        $dbUser = $model->getID($id);

        $this->assertEquals($id, $dbUser['userID']);
        unset($dbUser['userID']);
        $this->assertEquals($user, $dbUser);
    }

    /**
     * The dataset should be able to be iterated over again and again.
     */
    public function testModelRepeatableFetch() {
        $model = $this->createUserModel(__FUNCTION__);

        $rows = $model->get([])->setLimit(5);

        $data1 = [];
        foreach ($rows as $i => $row) {
            $data1[] = $row;
        }

        $data2 = [];
        foreach ($rows as $j => $row) {
            $data2[] = $row;
        }

        $this->assertSame($i, $j);
        $this->assertEquals($data1, $data2);


        $data = iterator_to_array($rows);
        $this->assertEquals($data1, $data);
    }

    /**
     * Test that basic getters and setters work.
     */
    public function testTableQueryPropertyAccess() {
        $model = $this->createUserModel(__FUNCTION__, 0);

        $ds = $model->get([]);

        $this->assertSame(4, $ds->setLimit(4)->getLimit());
        $this->assertSame(3, $ds->setOffset(3)->getOffset());
        $this->assertSame(2, $ds->setPage(2)->getPage());
        $this->assertSame(['name', '-email'], $ds->setOrder('name', '-email')->getOrder());
    }

    /**
     * Changing the limit after a query should be OK.
     */
    public function testChangeLimit() {
        $model = $this->createUserModel(__FUNCTION__, 8);

        $dataset = $model->get([])->setOrder('userID');

        $rows8 = $dataset->setLimit(8)->fetchAll();
        $rows4 = $dataset->setLimit(4)->fetchAll();
        
        $this->assertEquals(array_slice($rows8, 0, 4), $rows4);
        $this->assertSame(4, $dataset->count());
    }

    /**
     * Test paging through data.
     */
    public function testPaging() {
        $model = $this->createUserModel(__FUNCTION__, 8);

        $dataset = $model->get([])->setOrder('userID');

        $rows8 = $dataset->setLimit(8)->fetchAll();

        $dataset->setLimit(4);
        $page1 = $dataset->setPage(1)->fetchAll();
        $page2 = $dataset->setPage(2)->fetchAll();

        $this->assertEquals(array_merge($page1, $page2), $rows8);
    }

    /**
     * Assert that {@link TableQuery::fetchAll()} behaves the same way as {@link \PDOStatement::fetchAll()}.
     * @param int $defaultMode The default mode of the model
     * @param array ...$fetchArgs Arguments for **fetchAll()**.
     */
    protected function assertFetchAll($defaultMode, ...$fetchArgs) {
        $model = $this->createUserModel('userModel', 0, $defaultMode);

        if ($model->get([])->count() === 0) {
            self::$db->load($model->getName(), $this->provideUsers(12));
        }

        if (reset($fetchArgs) === PDO::FETCH_KEY_PAIR) {
            $rows1 = $model->query([], ['columns' => ['userID', 'name']])->fetchAll(...$fetchArgs);
        } else {
            $rows1 = $model->query([])->fetchAll(...$fetchArgs);
        }
        $rows2 = $model->get([])->fetchAll(...$fetchArgs);

        $this->assertNotEmpty($rows1);
        $this->assertEquals($rows1, $rows2);
    }

    /**
     * Compare fetching columns on {@link Model} to the {@link \PDOStatement}.
     *
     * @param int|array $defaultMode The default fetch mode of the model.
     * @dataProvider provideBaseFetchModes
     */
    public function testFetchAllColumn($defaultMode) {
        $this->assertFetchAll($defaultMode, PDO::FETCH_COLUMN);
        $this->assertFetchAll($defaultMode, PDO::FETCH_COLUMN, 2);
    }

    /**
     * Compare fetching columns on {@link Model} to the {@link \PDOStatement}.
     *
     * @param int|array $defaultMode The default fetch mode of the model.
     * @dataProvider provideBaseFetchModes
     */
    public function testFetchAllColumnGrouped($defaultMode) {
        $this->assertFetchAll($defaultMode, PDO::FETCH_COLUMN | PDO::FETCH_GROUP, 5);
    }

    /**
     * Compare fetching columns on {@link Model} to the {@link \PDOStatement}.
     *
     * @param int|array $defaultMode The default fetch mode of the model.
     * @dataProvider provideBaseFetchModes
     */
    public function testFetchAllKeyPair($defaultMode) {
        $this->assertFetchAll($defaultMode, PDO::FETCH_KEY_PAIR);
    }

    /**
     * The model's default limit should be passed along to queries.
     */
    public function testDefaultLimit() {
        $model = new Model('test', self::$db);
        $model->setDefaultLimit(3);

        $this->assertSame(3, count($model->get([])));
        $this->assertSame(3, $model->query([])->rowCount());
    }

    /**
     * The model's default order should be passed along to queries.
     */
    public function testDefaultOrder() {
        $model = new Model('test', self::$db);

        $order = ['gid', 'name'];
        $model->setDefaultOrder(...$order);

        $this->assertOrder($model->get([]), ...$order);
        $this->assertOrder($model->query([]), ...$order);
    }

    /**
     * Provide **PDO::FETCH_*** constants for testing different fetch modes.
     *
     * @return array Returns a data provider array.
     */
    public function provideBaseFetchModes() {
        $r = [
            'assoc' => [PDO::FETCH_ASSOC],
            'obj' => [PDO::FETCH_OBJ],
            'class' => [[PDO::FETCH_CLASS | PDO::FETCH_PROPS_LATE, ArrayObject::class]]
        ];

        return $r;
    }

    /**
     * Create a model against a populated user table.
     *
     * @param string $name The name of the table.
     * @param int $count The number of rows to insert.
     * @param int|array The default row type for the model.
     * @return Model Returns a new model.
     */
    private function createUserModel($name, $count = 10, $rowType = null) {
        $this->createPopulatedUserTable($name, $count);
        $model = new Model($name, self::$db, $rowType);
        return $model;
    }
}
