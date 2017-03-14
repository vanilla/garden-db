<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Tests;

use Garden\Db\Db;
use Garden\Db\Query;
use Garden\Db\TableDef;

/**
 * Test the basic functionality of the Db* classes.
 */
abstract class DbTest extends AbstractDbTest {
    protected static function getPx() {
        return 'db_';
    }

    /**
     * Test a create table.
     */
    public function testCreateTable() {
        $tableDef = [
            'name' => 'user',
            'columns' => [
                'userID' => ['dbtype' => 'int', 'primary' => true, 'autoIncrement' => true],
                'name' => ['dbtype' => 'varchar(50)'],
                'email' => ['dbtype' => 'varchar(255)'],
                'fullName' => ['dbtype' => 'varchar(50)', 'allowNull' => true],
                'banned' => ['dbtype' => 'tinyint', 'default' => 0],
                'insertTime' => ['dbtype' => 'int']
            ],
            'indexes' => [
                ['columns' => ['name'], 'type' => Db::INDEX_UNIQUE],
                ['columns' => ['email']],
                ['columns' => ['insertTime']]
            ]
        ];

        self::$db->defineTable($tableDef);

        return self::$db->fetchTableDef('user');
    }

    /**
     * Test {@link Db::insert()}.
     *
     * @depends testCreateTable
     */
    public function testInsert() {
        $db = self::$db;

        $user = $this->provideUser('Insert Test');
        $userID = $db->insert('user', $user);

        $dbUser = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($user, array_intersect_key($dbUser, $user));
    }

    /**
     * Test {@link Db::insert()} with the ignore option.
     *
     * @depends testCreateTable
     */
    public function testInsertIgnore() {
        $db = self::$db;

        $user = $this->provideUser('Insert Ignore');

        $userID = $db->insert('user', $user);
        $dbUser = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($user, array_intersect_key($dbUser, $user));

        $user2 = $this->provideUser('Insert Ignore2');
        $this->assertNotEquals($user, $user2);

        $user2['userID'] = $userID;
        $id = $db->insert('user', $user2, [Db::OPTION_IGNORE => true]);

        $dbUser2 = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($user, array_intersect_key($dbUser2, $user));
    }

    /**
     * Test {@link Db::insert()} with the replace option.
     *
     * @depends testCreateTable
     */
    public function testInsertReplace() {
        $db = self::$db;

        $user = $this->provideUser('Insert Replace');

        $userID = $db->insert('user', $user);
        $dbUser = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($user, array_intersect_key($dbUser, $user));

        $user2 = $this->provideUser('Insert Replace2');
        $this->assertNotEquals($user, $user2);

        $user2['userID'] = $userID;
        $id = $db->insert('user', $user2, [Db::OPTION_REPLACE => true]);

        $dbUser2 = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($user2, array_intersect_key($dbUser2, $user2));
    }

    /**
     * Test {@link Db::insert()} with the upsert option.
     *
     * @depends testCreateTable
     */
    public function testInsertUpsert() {
        $db = self::$db;

        $user = $this->provideUser('Upsert Test');

        $userID = $db->insert('user', $user);
        $dbUser = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($user, array_intersect_key($dbUser, $user));

        $user2 = $this->provideUser();
        $this->assertNotEquals($user2, $user);
        unset($user2['fullName']);
        $user2['userID'] = $userID;

        $db->insert('user', $user2, [Db::OPTION_UPSERT => true]);

        $dbUser2 = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($user['fullName'], $dbUser2['fullName']);

        $this->assertEquals($user2, array_intersect_key($dbUser2, $user2));

    }

    /**
     * Test {@link Db::insert()} with the upsert option and a primary key composed of multiple columns.
     */
    public function testInsertUpsertMultiKey() {
        $db = self::$db;
        $dbdef = new TableDef($db);

        $db->dropTable('userMeta', [Db::OPTION_IGNORE => true]);
        $dbdef->setTable('userMeta')
            ->setColumn('userID', 'int')
            ->setColumn('key', 'varchar(50)')
            ->setColumn('value', 'text')
            ->addIndex(Db::INDEX_PK, 'userID', 'key');
        $db->defineTable($dbdef->toArray());

        $db->insert(
            'userMeta',
            ['userID' => 1, 'key' => 'bio', 'value' => 'Just some dude.'],
            [Db::OPTION_UPSERT => true]
        );

        $row = $db->getOne('userMeta', ['userID' => 1, 'key' => 'bio']);
        $this->assertEquals(
            ['userID' => 1, 'key' => 'bio', 'value' => 'Just some dude.'],
            $row
        );

        $db->insert(
            'userMeta',
            ['userID' => 1, 'key' => 'bio', 'value' => 'Master of the universe.'],
            [Db::OPTION_UPSERT => true]
        );

        $rows = $db->get('userMeta', ['userID' => 1, 'key' => 'bio'])->fetchAll();
        $this->assertEquals(1, count($rows));
        $firstRow = reset($rows);
        $this->assertEquals(
            ['userID' => 1, 'key' => 'bio', 'value' => 'Master of the universe.'],
            $firstRow
        );
    }

    /**
     * Test {@link Db::update()}.
     */
    public function testUpdate() {
        $db = self::$db;

        $user = $this->provideUser('Update Test');

        $userID = $db->insert('user', $user);

        $email = sha1(microtime()).'@foo.com';
        $updated = $db->update(
            'user',
            ['email' => $email],
            ['userID' => $userID]
        );
        $this->assertEquals(1, $updated, "Db->update() must return the number of rows updated.");

        $dbUser = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals($email, $dbUser['email'], "Update value not in the db.");

        // Update on another column.
        $updated2 = $db->update(
            'user',
            ['name' => 'tupdate'],
            ['email' => $email]
        );
        $this->assertEquals(1, $updated2);

        $dbUser2 = $db->getOne('user', ['userID' => $userID]);
        $this->assertEquals('tupdate', $dbUser2['name'], "Update value not in the db.");
    }

    /**
     * Test {@link Db::update()} with the ignore option.
     */
    public function testUpdateIgnore() {
        $db = self::$db;

        $user1 = $this->provideUser('First Update');
        $userID1 = $db->insert('user', $user1);

        $user2 = $this->provideUser('Second Update');
        $userID2 = $db->insert('user', $user2);

        $updated = $db->update(
            'user',
            ['name' => $user2['name']],
            ['userID' => $userID1],
            [Db::OPTION_IGNORE => true]
        );
        $this->assertEquals(0, $updated);
    }

    /**
     * Test various where operators.
     *
     * @dataProvider provideTupleTests
     */
    public function testWhereOperators($where, $expected) {
        $db = self::$db;

        // Create a table for the test.
        $db->defineTable(
            [
                'name' => 'tuple',
                'columns' => [
                    'id' => ['dbtype' => 'int', 'allowNull' => true]
                ],
                'indexes' => [
                    ['columns' => ['id']],
                ]
            ]
        );
        $db->delete('tuple', []);

        $data = [['id' => null]];
        for ($i = 1; $i <= 5; $i++) {
            $data[] = ['id' => $i];
        }

        $db->load('tuple', $data);

        // Test some logical gets.
        $dbData = $db->get('tuple', $where, ['order' => ['id']])->fetchAll();
        $values = array_column($dbData, 'id');
        $this->assertEquals($expected, $values);
    }

    /**
     * Provide somet tests for the where clause test.
     *
     * @return array Returns an array of function args.
     */
    public function provideTupleTests() {
        $result = [
            '>' => [['id' => [Db::OP_GT => 3]], [4, 5]],
            '>=' => [['id' => [Db::OP_GTE => 3]], [3, 4, 5]],
            '<' => [['id' => [Db::OP_LT => 3]], [1, 2]],
            '<=' => [['id' => [Db::OP_LTE => 3]], [1, 2, 3]],
            '=' => [['id' => [Db::OP_EQ => 2]], [2]],
            '<>' => [['id' => [Db::OP_NEQ => 3]], [1, 2, 4, 5]],
            'is null' => [['id' => null], [null]],
            'is not null' => [['id' => [Db::OP_NEQ => null]], [1, 2, 3, 4, 5]],
            'all' => [[], [null, 1, 2, 3, 4, 5]],
            'in' => [['id' => [Db::OP_IN => [3, 4, 5]]], [3, 4, 5]],
            'in (short)' => [['id' => [3, 4, 5]], [3, 4, 5]],
            '= in' => [['id' => [Db::OP_EQ => [3, 4, 5]]], [3, 4, 5]],
            '<> in' => [['id' => [Db::OP_NEQ => [3, 4, 5]]], [1, 2]],
            'and' =>[['id' => [
                Db::OP_AND => [
                    Db::OP_GT => 3,
                    Db::OP_LT => 5
                ]
            ]], [4]],
            'or' =>[['id' => [
                Db::OP_OR => [
                    Db::OP_LT => 3,
                    Db::OP_EQ => 5
                ]
            ]], [1, 2, 5]]
        ];

        return $result;
    }

    /**
     * Test {@link Db::load()}.
     *
     * @depends testCreateTable
     */
    public function testLoad() {
        $db = self::$db;

        $db->load('user', $this->provideUsers(100), [Db::OPTION_IGNORE => true]);
    }

    /**
     * Test {@link Db::load()} with an array of data.
     *
     * @depends testCreateTable
     */
    public function testLoadArray() {
        $db = self::$db;

        $users = iterator_to_array($this->provideUsers(10));

        $db->load('user', $users, [Db::OPTION_REPLACE => true]);
    }

    /**
     * Create and populate a simple test table for various queries.
     */
    public function testComments() {
        $db = self::$db;

        $def = new TableDef('comment');
        $def->setColumn('id', 'int')
            ->setColumn('parentID', 'int')
            ->setColumn('count', 'int', 0)
            ->addIndex(Db::INDEX_PK, 'id');
        $db->defineTable($def->toArray());

        $rows = [
            ['id' => 1, 'parentID' => 1, 'count' => 0],
            ['id' => 2, 'parentID' => 1, 'count' => 1],
            ['id' => 3, 'parentID' => 2, 'count' => 0],
            ['id' => 4, 'parentID' => 2, 'count' => 1]
        ];
        $db->load('comment', $rows);

        $dbRows = $db->get('comment', [])->fetchAll();
        $this->assertEquals($rows, $dbRows);
    }

    /**
     * Test a group of AND expressions.
     *
     * @sdepends testComments
     */
    public function testAndGroup() {
        $qry = new Query('comment');
        $qry->addWhere('parentID', 1)
            ->beginAnd()
            ->addWhere('count', 0)
            ->end();

        $rows = $qry->exec(self::$db)->fetchAll();

        $this->assertEquals([1], array_column($rows, 'id'));
    }

    /**
     * Test a group of OR expressions.
     *
     * @depends testComments
     */
    public function testOrGroup() {
        $qry = new Query('comment');
        $qry->beginOr()
            ->addWhere('parentID', 1)
            ->addWhere('count', 0)
            ->end();

        $rows = $qry->exec(self::$db)->fetchAll();
        $this->assertEquals([1, 2, 3], array_column($rows, 'id'));
    }

    /**
     * Test query ordering.
     */
    public function testOrder() {
        $table = __FUNCTION__;
        $this->createPopulatedUserTable($table, 5);

        $order = ['-userID', 'name'];
        $dbUsers = self::$db->get($table, [], ['order' => $order]);
        $this->assertOrder($dbUsers, $order);
    }
}
