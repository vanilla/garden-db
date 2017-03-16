<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db\Utils;

use PDO;

trait FetchModeTrait {
    /**
     * @var array The fetch arguments (if any).
     */
    private $fetchArgs = [PDO::FETCH_ASSOC];

    /**
     * Get the default fetch mode.
     *
     * @return int Returns the current fetch mode.
     */
    public function getFetchMode() {
        return reset($this->fetchArgs);
    }

    /**
     * Get the fetch arguments.
     *
     * @return array Returns an array of arguments including the fetch mode.
     */
    public function getFetchArgs() {
        return $this->fetchArgs;
    }

    /**
     * Set the default fetch mode.
     *
     * @param int|string $mode One of the **PDO::FETCH_*** constants or a class name.
     * @param array $args Additional arguments for {@link \PDOStatement::fetchAll()}.
     * @return $this
     * @see http://php.net/manual/en/pdostatement.fetchall.php
     */
    public function setFetchMode($mode, ...$args) {
        if (is_string($mode)) {
            array_unshift($args, PDO::$mode);
            $mode = PDO::FETCH_CLASS;
        }
        $this->fetchArgs = array_merge([$mode], $args);

        return $this;
    }
}
