<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;


class Model {
    /**
     * @var string The name of the table.
     */
    private $name;

    /**
     * @var array
     */
    private $primaryKey;

    /**
     * @var Db
     */
    private $db;

    /**
     * @var int
     */
    private $defaultLimit = 30;

    /**
     * @var array
     */
    private $defaultOrder = [];

    public function __construct($name = '', Db $db) {
        $this->name = $name;
        $this->db = $db;
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
     * Set the name.
     *
     * @param string $name
     * @return $this
     */
    public function setName($name) {
        $this->name = $name;
        return $this;
    }

    /**
     * Get the primaryKey.
     *
     * @return array Returns the primaryKey.
     */
    public function getPrimaryKey() {
        return $this->primaryKey;
    }

    /**
     * Set the primaryKey.
     *
     * @param array|string $primaryKey
     * @return $this
     */
    public function setPrimaryKey($primaryKey) {
        $this->primaryKey = (array)$primaryKey;
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
     * @param array $where
     * @return DatasetInterface
     */
    public function get(array $where) {
        $qry = new TableQuery($this->name, $where, $this->db);

        $qry->setLimit($this->getDefaultLimit())
            ->setCalculator([$this, 'calculate']);

    }

    /**
     * @param mixed $id A primary key value for the model.
     * @return DatasetInterface
     */
    public function getID($id) {
        $r = $this->get($this->mapID($id));
        return $r;
    }

    public function insert(array $row) {

    }

    public function update(array $set, array $where) {
        $valid = $this->validate($set, true);
        $serialized = $this->serialize($valid);

        $r = $this->db->update($this->name, $serialized, $where);
        return $r;
    }

    public function updateID($id, $set) {
        $r = $this->update($set, $this->mapID($id));
        return $r;
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
     * @param array $defaultOrder
     * @return $this
     */
    public function setDefaultOrder($defaultOrder) {
        $this->defaultOrder = $defaultOrder;
        return $this;
    }
}
