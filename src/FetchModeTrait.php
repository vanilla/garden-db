<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2017 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;


trait FetchModeTrait {
    /**
     * @var array The default fetch mode.
     */
    private $fetchMode = 0;

    /**
     * Get the default fetch mode.
     *
     * @return array|int Returns the default fetch mode.
     */
    public function getFetchMode() {
        if (empty($this->fetchMode)) {
            return 0;
        } elseif (count($this->fetchMode) === 1) {
            return $this->fetchMode[0];
        } else {
            return $this->fetchMode;
        }
    }

    /**
     * Set the default fetch mode..
     *
     * @param array $mode This should be arguments compatible with {@link PDO::setFetchMode()}.
     * @return $this
     */
    public function setFetchMode(...$mode) {
        if (!empty($mode[0]) && is_string($mode[0])) {
            // We are specifying a class name so specify the class here.
            array_unshift($mode, \PDO::FETCH_CLASS);
        }

        $this->fetchMode = $mode;
        return $this;
    }
}
