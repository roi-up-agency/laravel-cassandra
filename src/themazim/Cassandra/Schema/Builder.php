<?php

namespace themazim\Cassandra\Schema;

use Closure;
use themazim\Cassandra\Connection;

class Builder extends \Illuminate\Database\Schema\Builder
{
    /**
     * Create a new database Schema manager.
     *
     * @param  Connection $connection
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        parent::__construct($connection);
    }

    /**
     * Determine if the given collection exists.
     *
     * @param  string $collection
     * @return bool
     */
    public function hasCollection($collection)
    {
        $db = $this->connection->getCassandra();

        foreach ($db->listCollections() as $collectionFromCassandra) {
            if ($collectionFromCassandra->getName() == $collection) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determine if the given collection exists.
     *
     * @param  string $collection
     * @return bool
     */
    public function hasTable($collection)
    {
        return $this->hasCollection($collection);
    }

    /**
     * Modify a collection on the schema.
     *
     * @param  string $collection
     * @param  Closure $callback
     * @return bool
     */
    public function collection($collection, Closure $callback)
    {
        return $this->table($collection, $callback);
    }

    /**
     * Create a new command set with a Closure.
     *
     * @param  string $table
     * @param  \Closure|null $callback
     * @return Blueprint
     */
    protected function createBlueprint($table, Closure $callback = null)
    {
        if (isset($this->resolver)) {
            return call_user_func($this->resolver, $table, $callback);
        }

        return new Blueprint($table, $callback);
    }
}
