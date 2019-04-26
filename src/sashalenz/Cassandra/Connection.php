<?php

namespace sashalenz\Cassandra;

use Illuminate\Database\Events\StatementPrepared;

class Connection extends \Illuminate\Database\Connection
{
    /**
     * The Cassandra keyspace name.
     *
     * @var string
     */
    protected $keyspace;

    /**
     * The Cassandra connection handler.
     *
     * @var \Cassandra\Session
     */
    protected $connection;

    /**
     * Create a new database connection instance.
     *
     * @param  array $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        // You can pass options directly to the Cassandra constructor
        $options = array_get($config, 'options', []);

        // Create the connection
        $this->connection = $this->createConnection(null, $config, $options);

        $this->useDefaultPostProcessor();
        $this->useDefaultSchemaGrammar();
        $this->useDefaultQueryGrammar();
        $this->setPdo(new PDO($this->connection));
    }

    /**
     * Get the default post processor instance.
     *
     * @return Query\Processor
     */
    protected function getDefaultPostProcessor()
    {
        return new Query\Processor();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultQueryGrammar()
    {
        return new Query\Grammar();
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param  string $collection
     * @return Query\Builder
     */
    public function collection($collection)
    {
        $grammar = $this->getQueryGrammar();

        $query = new Query\Builder($this, $grammar);

        return $query->from($collection);
    }

    /**
     * Begin a fluent query against a database collection.
     *
     * @param  string $table
     * @return Query\Builder
     */
    public function table($table)
    {
        return $this->collection($table);
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return Schema\Builder
     */
    public function getSchemaBuilder()
    {
        return new Schema\Builder($this);
    }

    /**
     * @inheritdoc
     */
    public function getDefaultSchemaGrammar()
    {
        return new \sashalenz\Cassandra\Schema\Grammars\Grammar(this);
    }

    /**
     * return Cassandra object.
     *
     * @return \Cassandra\Session
     */
    public function getCassandraSession()
    {
        return $this->connection;
    }

    /**
     * Return the Cassandra keyspace
     *
     * @return string
     */
    public function getKeyspace()
    {
        return $this->keyspace;
    }

    /**
     * Create a new Cassandra connection.
     *
     * @param  string $dsn
     * @param  array $config
     * @param  array $options
     * @return Cassandra
     */
    protected function createConnection($dsn, array $config, array $options)
    {
        // By default driver options is an empty array.
        $driverOptions = [];

        if (isset($config['driver_options']) && is_array($config['driver_options'])) {
            $driverOptions = $config['driver_options'];
        }

        // Check if the credentials are not already set in the options
        if (!isset($options['username']) && !empty($config['username'])) {
            $options['username'] = $config['username'];
        }
        if (!isset($options['password']) && !empty($config['password'])) {
            $options['password'] = $config['password'];
        }

        /*return new Client($dsn, $options, $driverOptions);*/

        $cluster = \Cassandra::cluster();

        // Authentication
        if (isset($options['username']) && isset($options['password'])) {
            $cluster->withCredentials($options['username'], $options['password']);

        }
        // Contact Points
        if (isset($options['contactpoints']) || (isset($config['host']) && !empty($config['host']))) {
            $contactPoints = $config['host'];
            if (isset($options['contactpoints'])) {
                $contactPoints = $options['contactpoints'];
            }
            $cluster->withContactPoints($contactPoints);
        }

        if (!isset($options['port']) && !empty($config['port'])) {
            $cluster->withPort($config['port']);
        }

        if (isset($options['database']) || isset($config['database'])) {
            $this->keyspace = $config['database'];
            $session = $cluster->build()->connect($config['database']);
        } else {
            $this->keyspace = null;
            $session = $cluster->build()->connect();
        }

        return $session;
    }

    /**
     * Disconnect from the underlying Cassandra connection.
     */
    public function disconnect()
    {
        unset($this->connection);
    }

    /**
     * Create a DSN string from a configuration.
     *
     * @param  array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        // First we will create the basic DSN setup as well as the port if it is in
        // in the configuration options. This will give us the basic DSN we will
        // need to establish the Cassandra and return them back for use.
        extract($config);

        // Check if the user passed a complete dsn to the configuration.
        if (!empty($dsn)) {
            return $dsn;
        }

        // Treat host option as array of hosts
        $hosts = is_array($host) ? $host : [$host];

        foreach ($hosts as &$host) {
            // Check if we need to add a port to the host
            if (strpos($host, ':') === false and isset($port)) {
                $host = "{$host}:{$port}";
            }
        }

        return "cassandra://" . implode(',', $hosts) . "/{$database}";
    }

    /**
     * Get the elapsed time since a given starting point.
     *
     * @param  int $start
     * @return float
     */
    public function getElapsedTime($start)
    {
        return parent::getElapsedTime($start);
    }

    /**
     * Get the PDO driver name.
     *
     * @return string
     */
    public function getDriverName()
    {
        return 'cassandra';
    }

    /**
     * Run a select statement against the database.
     *
     * @param  string $query
     * @param  array $bindings
     * @param  bool $useReadPdo
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        return $this->run($query, $bindings, function ($query, $bindings) use ($useReadPdo) {
            if ($this->pretending()) {
                return [];
            }
            $statement = $this->connection->prepare($query);
            return $this->connection->execute($statement, ['arguments' => $this->prepareBindings($bindings)]);
        });
    }

    /**
     * Run a select statement against the database and returns a generator.
     *
     * @param  string $query
     * @param  array $bindings
     * @param  bool $useReadPdo
     * @return \Generator
     */
    public function cursor($query, $bindings = [], $useReadPdo = true)
    {
        $result = $this->select($query, $bindings, $useReadPdo);

        foreach ($result as $record) {
            yield $record;
        }
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string $query
     * @param  array $bindings
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            $statement = $this->connection->prepare($query);

            return $this->connection->execute($statement, ['arguments' => $this->prepareBindings($bindings)]);
        });
    }

    public function affectingStatement($query, $bindings = [])
    {
        throw new \RuntimeException('affectingStatement not supported');
    }

    /**
     * Bind values to their parameters in the given statement.
     *
     * @param  \PDOStatement $statement
     * @param  array $bindings
     * @return void
     */
    public function bindValues($statement, $bindings)
    {
        throw new \RuntimeException('bindValues not supported');
    }

    /**
     * Dynamically pass methods to the connection.
     *
     * @param  string $method
     * @param  array $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array([$this->db, $method], $parameters);
    }
}
