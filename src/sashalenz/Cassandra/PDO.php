<?php
/**
 * User: vhnvn
 * Date: 5/30/17
 * Time: 5:36 PM
 */

namespace sashalenz\Cassandra;


class PDO
{
    /**
     * The Cassandra connection handler.
     *
     * @var \Cassandra\Session
     */
    protected $connection;

    public function __construct($connection)
    {
        $this->connection = $connection;
    }

    function __call($name, $arguments)
    {
        return $this->connection->$name(...$arguments);
    }
}