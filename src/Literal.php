<?php
/**
 * @author Todd Burry <todd@vanillaforums.com>
 * @copyright 2009-2014 Vanilla Forums Inc.
 * @license MIT
 */

namespace Garden\Db;

/**
 * Represents an string that won't be escaped in queries.
 */
class Literal {
    /// Properties ///

    /**
     * @var array An array that maps driver names to literals.
     */
    protected $driverValues = [];

    /// Methods ///

    /**
     * Initialize an instance of the {@link Literal} class.
     *
     * @param string|array $value Either a string default value or an array in the form
     * `['driver' => 'literal']` to specify different literals for different database drivers.
     */
    public function __construct($value) {
        if (is_string($value)) {
            $this->driverValues['default'] = $value;
        } elseif (is_array($value)) {
            foreach ($value as $key => $value) {
                $this->driverValues[$this->driverKey($key)] = $value;
            }
        }
    }

    /**
     * Get the literal value.
     *
     * @param Db $db The database driver getting the value.
     * @param mixed ...$args Arguments to pass into the literal. This uses **sprintf()** so arguments must already be escaped.
     * @return string Returns the value for the specific driver, the default literal, or "null" if there is no default.
     */
    public function getValue(Db $db, ...$args) {
        $driver = $this->driverKey($db);

        if (isset($this->driverValues[$driver])) {
            return sprintf($this->driverValues[$driver], ...$args);
        } elseif (isset($this->driverValues['default'])) {
            return sprintf($this->driverValues['default'], ...$args);
        } else {
            throw new \InvalidArgumentException("No literal for driver '$driver'.", 500);
        }
    }

    /**
     * Set the literal value.
     *
     * @param string $value The new value to set.
     * @param string|object $driver The name of the database driver to set the value for.
     * @return Literal Returns $this for fluent calls.
     */
    public function setValue($value, $driver = 'default') {
        $driver = $this->driverKey($driver);
        $this->driverValues[$driver] = $value;
        return $this;
    }

    /**
     * Normalize the driver name for the drivers array.
     *
     * @param string|object $key The name of the driver or an instance of a database driver.
     * @return string Returns the driver name normalized.
     */
    protected function driverKey($key) {
        if (is_object($key)) {
            $key = get_class($key);
        }
        $key = strtolower(basename($key));

        if (preg_match('`([az]+)(Db)?$`', $key, $m)) {
            $key = $m;
        }
        return $key;
    }

    /**
     * Create and return a {@link Literal} object.
     *
     * @param string|array $value The literal value(s) as passed to {@link Literal::__construct()}.
     * @return Literal Thew new literal value.
     */
    public static function value($value) {
        $literal = new Literal($value);
        return $literal;
    }

    /**
     * Create and return a {@link Literal} object that will query the current unix timesatmp.
     *
     * @return Literal Returns the timestamp expression.
     */
    public static function timestamp() {
        $literal = new Literal([
            MySqlDb::class => 'unix_timestamp()',
            SqliteDb::class => "date('now', 'unixepoch')",
//            'posgresql' => 'extract(epoch from now())',
            'default' => time()
        ]);
        return $literal;
    }
}
