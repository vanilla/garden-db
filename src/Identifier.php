<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license Proprietary
 */

namespace Garden\Db;

/**
 * Contains a database identifier string such as a table name or a column name.
 *
 * The database object is meant to take plain database strings for tables. However, when it takes
 * such a string it will always prepend the database prefix to the table name. If you want to query a
 * table without the prefix then wrap it in this class.
 */
class Identifier {
    private $parts;

    /**
     * Identifier constructor.
     * @param array ...$identifier
     */
    public function __construct(...$identifier) {
        if (empty($identifier) || empty($identifier[0])) {
            throw new \InvalidArgumentException("The identifier is empty.", 500);
        }
        if (count($identifier) === 1) {
            $this->parts = explode('.', $identifier[0]);
        } else {
            $this->parts = $identifier;
        }
    }

    /**
     * Convert the identifier to a simple string.
     *
     * @return string Returns the identifier as a string.
     */
    public function __toString() {
        return implode('.', $this->parts);
    }

    /**
     * Escape the identifier.
     *
     * @param Db $db The database used to escape the identifier.
     * @return string Returns the full escaped identifier.
     */
    public function escape(Db $db) {
        return implode('.', array_map([$db, 'escape'], $this->parts));
    }
}
