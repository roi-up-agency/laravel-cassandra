<?php

namespace sashalenz\Cassandra\Schema;

use Closure;
use Illuminate\Database\Connection;
use \Illuminate\Database\Schema\Grammars\Grammar;
use Illuminate\Support\Fluent;

class Blueprint extends \Illuminate\Database\Schema\Blueprint
{
    /**
     * The table the blueprint describes.
     *
     * @var string
     */
    protected $collection;

    /**
     * The columns that should be added to the table.
     *
     * @var array
     */
    protected $columns = [];

    /**
     * The commands that should be run for the table.
     *
     * @var array
     */
    protected $commands = [];

    /**
     * The storage engine that should be used for the table.
     *
     * @var string
     */
    public $engine;

    /**
     * The default character set that should be used for the table.
     */
    public $charset;

    /**
     * The collation that should be used for the table.
     */
    public $collation;

    /**
     * Whether to make the table temporary.
     *
     * @var bool
     */
    public $temporary = false;

    /**
     * Options of the statement
     *
     * @var null
     */
    public $options = null;

    public $primary_columns = [];

    /**
     * Create a new schema blueprint.
     *
     * @param  string $collection
     * @param  \Closure|null $callback
     */
    public function __construct($collection, Closure $callback = null)
    {
        $this->collection = $collection;
        $this->table = $collection;

        if (!is_null($callback)) {
            $callback($this);
        }
    }

    /**
     * @inheritdoc
     */
    public function build(Connection $connection, Grammar $grammar)
    {
        foreach ($this->toSql($connection, $grammar) as $statement) {
            $connection->statement($statement);
        }
    }

    /**
     * @inheritdoc
     */
    public function toSql(Connection $connection, Grammar $grammar)
    {
        $this->addImpliedCommands();

        $statements = [];

        // Each type of command has a corresponding compiler function on the schema
        // grammar which is used to build the necessary SQL statements to build
        // the blueprint element, so we'll just call that compilers function.
        foreach ($this->commands as $command) {
            $method = 'compile' . ucfirst($command->name);

            if (method_exists($grammar, $method)) {
                if (!is_null($sql = $grammar->$method($this, $command, $connection))) {
                    $statements = array_merge($statements, (array)$sql);
                }
            } else {
                var_dump($command);
            }
        }

        return $statements;
    }

    /**
     * Add the commands that are implied by the blueprint's state.
     *
     * @return void
     */
    protected function addImpliedCommands()
    {
        if (count($this->getAddedColumns()) > 0 && !$this->creating()) {
            array_unshift($this->commands, $this->createCommand('add'));
        }

        if (count($this->getChangedColumns()) > 0 && !$this->creating()) {
            array_unshift($this->commands, $this->createCommand('change'));
        }

        $this->addFluentIndexes();
    }

    /**
     * Add the index commands fluently specified on columns.
     *
     * @return void
     */
    protected function addFluentIndexes()
    {
        foreach ($this->columns as $column) {
            foreach (['primary', 'index'] as $index) {
                // If the index has been specified on the given column, but is simply equal
                // to "true" (boolean), no name has been specified for this index so the
                // index method can be called without a name and it will generate one.
                if ($column->{$index} === true) {
                    $this->{$index}($column->name);

                    continue 2;
                }

                // If the index has been specified on the given column, and it has a string
                // value, we'll go ahead and call the index method and pass the name for
                // the index since the developer specified the explicit name for this.
                elseif (isset($column->{$index})) {
                    $this->{$index}($column->name, $column->{$index});

                    continue 2;
                }
            }
        }
    }

    /**
     * Determine if the blueprint has a create command.
     *
     * @return bool
     */
    protected function creating()
    {
        return collect($this->commands)->contains(function ($command) {
            return $command->name == 'create';
        });
    }

    /**
     * Indicate that the table needs to be created.
     *
     * @return Fluent
     */
    public function create()
    {
        return $this->addCommand('create');
    }

    /**
     * Indicate that the table needs to be temporary.
     *
     * @return void
     */
    public function temporary()
    {
        $this->temporary = true;
    }

    /**
     * Indicate that the table should be dropped.
     *
     * @return Fluent
     */
    public function drop()
    {
        return $this->addCommand('drop');
    }

    /**
     * Indicate that the table should be dropped if it exists.
     *
     * @return Fluent
     */
    public function dropIfExists()
    {
        return $this->addCommand('dropIfExists');
    }

    /**
     * Indicate that the given columns should be dropped.
     *
     * @param  array|mixed $columns
     * @return Fluent
     */
    public function dropColumn($columns)
    {
        $columns = is_array($columns) ? $columns : (array)func_get_args();

        return $this->addCommand('dropColumn', compact('columns'));
    }

    /**
     * Indicate that the given columns should be renamed.
     *
     * @param  string $from
     * @param  string $to
     * @return Fluent
     */
    public function renameColumn($from, $to)
    {
        return $this->addCommand('renameColumn', compact('from', 'to'));
    }

    /**
     * Indicate that the given index should be dropped.
     *
     * @param  string|array $index
     * @return Fluent
     */
    public function dropIndex($index)
    {
        return $this->dropIndexCommand('dropIndex', 'index', $index);
    }

    /**
     * Indicate that the timestamp columns should be dropped.
     *
     * @return void
     */
    public function dropTimestamps()
    {
        $this->dropColumn('created_at', 'updated_at');
    }

    /**
     * Indicate that the timestamp columns should be dropped.
     *
     * @return void
     */
    public function dropTimestampsTz()
    {
        $this->dropTimestamps();
    }

    /**
     * Indicate that the soft delete column should be dropped.
     *
     * @return void
     */
    public function dropSoftDeletes()
    {
        $this->dropColumn('deleted_at');
    }

    /**
     * Indicate that the soft delete column should be dropped.
     *
     * @return void
     */
    public function dropSoftDeletesTz()
    {
        $this->dropSoftDeletes();
    }

    /**
     * Indicate that the remember token column should be dropped.
     *
     * @return void
     */
    public function dropRememberToken()
    {
        $this->dropColumn('remember_token');
    }

    /**
     * Rename the table to a given name.
     *
     * @param  string $to
     * @return Fluent
     */
    public function rename($to)
    {
        return $this->addCommand('rename', compact('to'));
    }

    /**
     * Specify the primary key(s) for the table.
     *
     * @param  string|array $columns
     * @param  string $name
     * @param  string|null $algorithm
     * @return Fluent
     */
    public function primary($columns, $name = null, $algorithm = null)
    {
        $this->primary_columns = $columns;
    }

    /**
     * Specify an index for the table.
     *
     * @param  string|array $columns
     * @param  string $name
     * @param  string|null $algorithm
     * @return Fluent
     */
    public function index($columns, $name = null, $algorithm = null)
    {
        return $this->indexCommand('index', $columns, $name, $algorithm);
    }

    /**
     * Create a new auto-incrementing integer (4-byte) column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function increments($column)
    {
        return $this->bigint($column, true);
    }

    /**
     * Create a new string column on the table.
     *
     * @param  string $column
     * @param  int $length
     * @return Fluent
     */
    public function string($column, $length = null)
    {
        return $this->addColumn('text', $column);
    }

    /**
     * Create a new text column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function text($column)
    {
        return $this->addColumn('text', $column);
    }

    /**
     * Create a new integer (4-byte) column on the table.
     *
     * @param  string $column
     * @param  bool $autoIncrement
     * @param  bool $unsigned
     * @return Fluent
     */
    public function integer($column, $autoIncrement = false, $unsigned = false)
    {
        return $this->addColumn('integer', $column);
    }


    /**
     * Create a new float column on the table.
     *
     * @param  string $column
     * @param  int $total
     * @param  int $places
     * @return Fluent
     */
    public function float($column, $total = 8, $places = 2)
    {
        return $this->addColumn('float', $column);
    }

    /**
     * Create a new double column on the table.
     *
     * @param  string $column
     * @param  int|null $total
     * @param  int|null $places
     * @return Fluent
     */
    public function double($column, $total = null, $places = null)
    {
        return $this->addColumn('double', $column);
    }

    /**
     * Create a new boolean column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function boolean($column)
    {
        return $this->addColumn('boolean', $column);
    }

    /**
     * Create a new date-time column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function dateTime($column)
    {
        return $this->addColumn('timestamp', $column);
    }

    /**
     * Create a new timestamp column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function timestamp($column)
    {
        return $this->addColumn('timestamp', $column);
    }

    /**
     * Add nullable creation and update timestamps to the table.
     *
     * @return void
     */
    public function timestamps()
    {
        $this->timestamp('created_at')->nullable();

        $this->timestamp('updated_at')->nullable();
    }

    /**
     * Add nullable creation and update timestamps to the table.
     *
     * Alias for self::timestamps().
     *
     * @return void
     */
    public function nullableTimestamps()
    {
        $this->timestamps();
    }

    /**
     * Add creation and update timestampTz columns to the table.
     *
     * @return void
     */
    public function timestampsTz()
    {
        $this->timestampTz('created_at')->nullable();

        $this->timestampTz('updated_at')->nullable();
    }

    /**
     * Add a "deleted at" timestamp for the table.
     *
     * @return Fluent
     */
    public function softDeletes()
    {
        return $this->timestamp('deleted_at')->nullable();
    }

    /**
     * Add a "deleted at" timestampTz for the table.
     *
     * @return Fluent
     */
    public function softDeletesTz()
    {
        return $this->timestampTz('deleted_at')->nullable();
    }

    /**
     * Create a new binary column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function binary($column)
    {
        return $this->addColumn('blob', $column);
    }

    /**
     * Create a new uuid column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function uuid($column)
    {
        return $this->addColumn('uuid', $column);
    }

    /**
     * Create a new IP address column on the table.
     *
     * @param  string $column
     * @return Fluent
     */
    public function ipAddress($column)
    {
        return $this->addColumn('inet', $column);
    }

    /**
     * Add the proper columns for a polymorphic table.
     *
     * @param  string $name
     * @param  string|null $indexName
     * @return void
     */
    public function morphs($name, $indexName = null)
    {
        $this->unsignedInteger("{$name}_id");

        $this->string("{$name}_type");

        $this->index(["{$name}_id", "{$name}_type"], $indexName);
    }

    /**
     * Add nullable columns for a polymorphic table.
     *
     * @param  string $name
     * @param  string|null $indexName
     * @return void
     */
    public function nullableMorphs($name, $indexName = null)
    {
        $this->unsignedInteger("{$name}_id")->nullable();

        $this->string("{$name}_type")->nullable();

        $this->index(["{$name}_id", "{$name}_type"], $indexName);
    }

    /**
     * Adds the `remember_token` column to the table.
     *
     * @return Fluent
     */
    public function rememberToken()
    {
        return $this->string('remember_token', 100)->nullable();
    }

    /**
     * Add a new index command to the blueprint.
     *
     * @param  string $type
     * @param  string|array $columns
     * @param  string $index
     * @param  string|null $algorithm
     * @return Fluent
     */
    protected function indexCommand($type, $columns, $index, $algorithm = null)
    {
        $columns = (array)$columns;

        // If no name was specified for this index, we will create one using a basic
        // convention of the table name, followed by the columns, followed by an
        // index type, such as primary or index, which makes the index unique.
        $index = $index ?: $this->createIndexName($type, $columns);

        return $this->addCommand(
            $type, compact('index', 'columns', 'algorithm')
        );
    }

    /**
     * Create a new drop index command on the blueprint.
     *
     * @param  string $command
     * @param  string $type
     * @param  string|array $index
     * @return Fluent
     */
    protected function dropIndexCommand($command, $type, $index)
    {
        $columns = [];

        // If the given "index" is actually an array of columns, the developer means
        // to drop an index merely by specifying the columns involved without the
        // conventional name, so we will build the index name from the columns.
        if (is_array($index)) {
            $index = $this->createIndexName($type, $columns = $index);
        }

        return $this->indexCommand($command, $columns, $index);
    }

    /**
     * Create a default index name for the table.
     *
     * @param  string $type
     * @param  array $columns
     * @return string
     */
    protected function createIndexName($type, array $columns)
    {
        $index = strtolower($this->collection . '_' . implode('_', $columns) . '_' . $type);

        return str_replace(['-', '.'], '_', $index);
    }

    /**
     * Add a new column to the blueprint.
     *
     * @param  string $type
     * @param  string $name
     * @param  array $parameters
     * @return Fluent
     */
    public function addColumn($type, $name, array $parameters = [])
    {
        $this->columns[] = $column = new Fluent(
            array_merge(compact('type', 'name'), $parameters)
        );

        return $column;
    }

    /**
     * Remove a column from the schema blueprint.
     *
     * @param  string $name
     * @return $this
     */
    public function removeColumn($name)
    {
        $this->columns = array_values(array_filter($this->columns, function ($c) use ($name) {
            return $c['attributes']['name'] != $name;
        }));

        return $this;
    }

    /**
     * Add a new command to the blueprint.
     *
     * @param  string $name
     * @param  array $parameters
     * @return Fluent
     */
    protected function addCommand($name, array $parameters = [])
    {
        $this->commands[] = $command = $this->createCommand($name, $parameters);

        return $command;
    }

    /**
     * Create a new Fluent command.
     *
     * @param  string $name
     * @param  array $parameters
     * @return Fluent
     */
    protected function createCommand($name, array $parameters = [])
    {
        return new Fluent(array_merge(compact('name'), $parameters));
    }

    /**
     * Get the table the blueprint describes.
     *
     * @return string
     */
    public function getTable()
    {
        return $this->collection;
    }

    /**
     * Get the columns on the blueprint.
     *
     * @return array
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * Get the commands on the blueprint.
     *
     * @return array
     */
    public function getCommands()
    {
        return $this->commands;
    }

    /**
     * Get the columns on the blueprint that should be added.
     *
     * @return array
     */
    public function getAddedColumns()
    {
        return array_filter($this->columns, function ($column) {
            return !$column->change;
        });
    }

    /**
     * Get the columns on the blueprint that should be changed.
     *
     * @return array
     */
    public function getChangedColumns()
    {
        return array_filter($this->columns, function ($column) {
            return (bool)$column->change;
        });
    }
}
