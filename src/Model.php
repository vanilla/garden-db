<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

use Garden\Schema\Schema;

class Model {
    use Utils\FetchModeTrait { setFetchMode as private; }

    const DEFAULT_LIMIT = 30;

    /**
     * @var string The name of the table.
     */
    private $name;

    /**
     * @var Db
     */
    private $db;

    /**
     * @var array
     */
    private $primaryKey;

    /**
     * @var Schema
     */
    private $schema;

    /**
     * @var int
     */
    private $defaultLimit = Model::DEFAULT_LIMIT;

    /**
     * @var string[]
     */
    private $defaultOrder = [];

    public function __construct($name, Db $db, $rowType = null) {
        $this->name = $name;
        $this->db = $db;

        $fetchMode = $rowType !== null ? $rowType : $db->getFetchMode();
        if (!empty($fetchMode)) {
            $this->setFetchMode(...(array)$fetchMode);
        }
    }

    /**
     * Get the name.
     *
     * @return string Returns the name.
     */
    public function getName() {
        return $this->name;
    }

    /**
     * Get the primaryKey.
     *
     * @return array Returns the primaryKey.
     */
    public function getPrimaryKey() {
        if ($this->primaryKey === null) {
            $schema = $this->getSchema();

            $pk = [];
            foreach ($schema->getSchemaArray()['properties'] as $column => $property) {
                if (!empty($property['primary'])) {
                    $pk[] = $column;
                }
            }
            $this->primaryKey = $pk;
        }
        return $this->primaryKey;
    }

    /**
     * Set the primaryKey.
     *
     * @param array $primaryKey The names of the columns in the primary key.
     * @return $this
     */
    protected function setPrimaryKey(...$primaryKey) {
        $this->primaryKey = $primaryKey;
        return $this;
    }

    /**
     * Get the db.
     *
     * @return Db Returns the db.
     */
    public function getDb() {
        return $this->db;
    }

    /**
     * Set the db.
     *
     * @param Db $db
     * @return $this
     */
    public function setDb($db) {
        $this->db = $db;
        return $this;
    }

    /**
     * Map primary key values to the primary key name.
     *
     * @param mixed $id An ID value or an array of ID values. If an array is passed and the model has a mult-column
     * primary key then all of the values must be in order.
     * @return array Returns an associative array mapping column names to values.
     */
    protected function mapID($id) {
        $idArray = (array)$id;

        $result = [];
        foreach ($this->getPrimaryKey() as $i => $column) {
            if (isset($idArray[$i])) {
                $result[$column] = $idArray[$i];
            } elseif (isset($idArray[$column])) {
                $result[$column] = $idArray[$column];
            } else {
                $result[$column] = null;
            }
        }

        return $result;
    }

    /**
     * Gets the row schema for this model.
     *
     * @return Schema Returns a schema.
     */
    final public function getSchema() {
        if ($this->schema === null) {
            $this->schema = $this->fetchSchema();
        }
        return $this->schema;
    }

    /**
     * Fetch the row schema from the database meta info.
     *
     * This method works fine as-is, but can also be overridden to provide more specific schema information for the model.
     * This method is called only once for the object and then is cached in a property so you don't need to implement
     * caching of your own.
     *
     * If you are going to override this method we recommend you still call the parent method and add its result to your schema.
     * Here is an example:
     *
     * ```php
     * protected function fetchSchema() {
     *     $schema = Schema::parse([
     *         'body:s', // make the column required even if it isn't in the db.
     *         'attributes:o?' // accept an object instead of string
     *     ]);
     *
     *     $dbSchema = parent::fetchSchema();
     *     $schema->add($dbSchema, true);
     *
     *     return $schema;
     * }
     * ```
     *
     * @return Schema Returns the row schema.
     */
    protected function fetchSchema() {
        $columns = $this->getDb()->fetchColumnDefs($this->name);
        if ($columns === null) {
            throw new \InvalidArgumentException("Cannot fetch schema foor {$this->name}.");
        }

        $schema = [
            'type' => 'object',
            'dbtype' => 'table',
            'properties' => $columns
        ];

        $required = $this->requiredFields($columns);
        if (!empty($required)) {
            $schema['required'] = $required;
        }

        return new Schema($schema);
    }

    /**
     * Figure out the schema required fields from a list of columns.
     *
     * A column is required if it meets all of the following criteria.
     *
     * - The column does not have an auto increment.
     * - The column does not have a default value.
     * - The column does not allow null.
     *
     * @param array $columns An array of column schemas.
     */
    private function requiredFields(array $columns) {
        $required = [];

        foreach ($columns as $name => $column) {
            if (empty($column['autoIncrement']) && !isset($column['default']) && empty($column['allowNull'])) {
                $required[] = $name;
            }
        }

        return $required;
    }

    /**
     * Query the model.
     *
     * @param array $where A where clause to filter the data.
     * @return DatasetInterface
     */
    public function get(array $where) {
        $options = [
            Db::OPTION_FETCH_MODE => $this->getFetchArgs(),
            'rowCallback' => [$this, 'unserialize']
        ];

        $qry = new TableQuery($this->name, $where, $this->db, $options);
        $qry->setLimit($this->getDefaultLimit())
            ->setOrder(...$this->getDefaultOrder());

        return $qry;
    }

    /**
     * Query the database directly.
     *
     * @param array $where A where clause to filter the data.
     * @param array $options Options to pass to the database. See {@link Db::get()}.
     * @return \PDOStatement Returns a statement from the query.
     */
    public function query(array $where, array $options = []) {
        $options += [
            'order' => $this->getDefaultOrder(),
            'limit' => $this->getDefaultLimit(),
            Db::OPTION_FETCH_MODE => $this->getFetchArgs()
        ];

        $stmt = $this->db->get($this->name, $where, $options);
        return $stmt;
    }

    /**
     * @param mixed $id A primary key value for the model.
     * @return mixed|null
     */
    public function getID($id) {
        $r = $this->get($this->mapID($id));
        return $r->firstRow();
    }

    public function insert($row, array $options = []) {
        $valid = $this->validate($row, false);
        $serialized = $this->serialize($valid);

        $r = $this->db->insert($this->name, $serialized, $options);
        return $r;
    }

    public function update(array $set, array $where, array $options = []) {
        $valid = $this->validate($set, true);
        $serialized = $this->serialize($valid);

        $r = $this->db->update($this->name, $serialized, $where, $options);
        return $r;
    }

    public function updateID($id, $set) {
        $r = $this->update($set, $this->mapID($id));
        return $r;
    }

    /**
     * Validate a row of data.
     *
     * @param array|\ArrayAccess $row The row to validate.
     * @param bool $sparse Whether or not the validation should be sparse (during update).
     * @return array Returns valid data.
     */
    public function validate($row, $sparse = false) {
        $schema = $this->getSchema();
        $valid = $schema->validate($row, $sparse);

        return $valid;
    }

    /**
     * Serialize a row of data into a format that can be native to the database.
     *
     * This method should always take an array of data, even if your model is meant to use objects of some sort. This is
     * possible because the row that gets passed into this method is the output of {@link validate()}.
     *
     * @param array $row The row to serialize.
     * @return array Returns a row of serialized data.
     */
    public function serialize(array $row) {
        return $row;
    }

    /**
     * Unserialize a row from the database and make it ready for use by the user of this model.
     *
     * The base model doesn't do anything in this method which is intentional for speed.
     *
     * @param mixed $row
     * @return mixed
     */
    public function unserialize($row) {
        return $row;
    }

    /**
     * Get the defaultLimit.
     *
     * @return int Returns the defaultLimit.
     */
    public function getDefaultLimit() {
        return $this->defaultLimit;
    }

    /**
     * Set the defaultLimit.
     *
     * @param int $defaultLimit
     * @return $this
     */
    public function setDefaultLimit($defaultLimit) {
        $this->defaultLimit = $defaultLimit;
        return $this;
    }

    /**
     * Get the defaultOrder.
     *
     * @return array Returns the defaultOrder.
     */
    public function getDefaultOrder() {
        return $this->defaultOrder;
    }

    /**
     * Set the defaultOrder.
     *
     * @param string[] $columns
     * @return $this
     */
    public function setDefaultOrder(...$columns) {
        $this->defaultOrder = $columns;
        return $this;
    }
}
