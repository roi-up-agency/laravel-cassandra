Laravel Cassandra
===============

Content
-------

Integrate Cassandra into your Laravel app, using Laravel's own API and methods.

Table of contents
-----------------
* [Installation](#installation)
* [Configuration](#configuration)
* [Eloquent](#eloquent)
* [Optional: Alias](#optional-alias)
* [Query Builder](#query-builder)
* [Schema](#schema)
* [Extensions](#extensions)
* [Troubleshooting](#troubleshooting)
* [Examples](#examples)

Installation
------------

Make sure you have the Cassandra PHP driver installed. https://github.com/datastax/php-driver

Installation using composer:

```
composer require sashalenz/laravel-cassandra
```

### Laravel version Compatibility

 Laravel  | Package
:---------|:----------
 5.8.x    | 1.0.x


Add Cassandra service provider in `config/app.php`:

```php
RoiupAgency\Cassadra\CassandraServiceProvider::class,
```

For usage with [Lumen](http://lumen.laravel.com), add the service provider in `bootstrap/app.php`. In this file, you will also need to enable Eloquent. You must however ensure that your call to `$app->withEloquent();` is **below** where you have registered the `CassandraServiceProvider`:

```php
$app->register('RoiupAgency\Cassadra\CassandraServiceProvider');

$app->withEloquent();
```

The service provider will register a Cassandra database extension with the original database manager. There is no need to register additional facades or objects. When using cassandra connections, Laravel will automatically provide you with the corresponding cassandra objects.




Configuration
-------------

Change your default database connection name in `app/config/database.php`:

```php
'default' => env('DB_CONNECTION', 'cassandra'),
```

And add a new cassandra connection:

```php
'cassandra' => [
    'driver'   => 'cassandra',
    'host'     => env('DB_HOST', 'localhost'),
    'port'     => env('DB_PORT', 27017),
    'database' => env('DB_DATABASE', ''),
    'username' => env('DB_USERNAME', ''),
    'password' => env('DB_PASSWORD', ''),
    'options' => [
        'db' => 'admin' // sets the authentication database required by cassandra 3
    ]
],
```

You can connect to multiple servers or replica sets with the following configuration:

```php
'cassandra' => [
    'driver'   => 'cassandra',
    'host'     => ['server1', 'server2'],
    'port'     => env('DB_PORT', 27017),
    'database' => env('DB_DATABASE', ''),
    'username' => env('DB_USERNAME', ''),
    'password' => env('DB_PASSWORD', ''),
    'options'  => ['replicaSet' => 'replicaSetName']
],
```

Eloquent
--------

This package includes a Cassandra enabled Eloquent class that you can use to define models for corresponding collections.

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class User extends Eloquent {}
```

Note that we did not tell Eloquent which collection to use for the `User` model. Just like the original Eloquent, the lower-case, plural name of the class will be used as the table name unless another name is explicitly specified. You may specify a custom collection (alias for table) by defining a `collection` property on your model:

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class User extends Eloquent {

    protected $collection = 'users_collection';

}
```

**NOTE:** Eloquent will also assume that each collection has a primary key column named id. You may define a `primaryKey` property to override this convention. Likewise, you may define a `connection` property to override the name of the database connection that should be used when utilizing the model.

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class MyModel extends Eloquent {

    protected $connection = 'cassandra';

}
```

Everything else (should) work just like the original Eloquent model. Read more about the Eloquent on http://laravel.com/docs/eloquent

### Optional: Alias

You may also register an alias for the Cassandra model by adding the following to the alias array in `app/config/app.php`:

```php
'Moloquent'       => 'RoiupAgency\Cassadra\Eloquent\Model',
```

This will allow you to use the registered alias like:

```php
class MyModel extends Moloquent {}
```

Query Builder
-------------

The database driver plugs right into the original query builder. When using cassandra connections, you will be able to build fluent queries to perform database operations. For your convenience, there is a `collection` alias for `table` as well as some additional cassandra specific operators/operations.

```php
$users = DB::collection('users')->get();

$user = DB::collection('users')->where('name', 'John')->first();
```

If you did not change your default database connection, you will need to specify it when querying.

```php
$user = DB::connection('cassandra')->collection('users')->get();
```

Read more about the query builder on http://laravel.com/docs/queries

Schema
------

The database driver also has (limited) schema builder support. You can easily manipulate collections and set indexes:

```php
Schema::create('users', function($collection)
{
    $collection->index('name');

    $collection->unique('email');
});
```

Supported operations are:

 - create and drop
 - collection
 - hasCollection
 - index and dropIndex (compound indexes supported as well)
 - unique
 - background, sparse, expire (Cassandra specific)

All other (unsupported) operations are implemented as dummy pass-through methods, because Cassandra does not use a predefined schema. Read more about the schema builder on http://laravel.com/docs/schema

Extensions
----------

### Auth

If you want to use Laravel's native Auth functionality, register this included service provider:

```php
'RoiupAgency\Cassadra\Auth\PasswordResetServiceProvider',
```

This service provider will slightly modify the internal DatabaseReminderRepository to add support for Cassandra based password reminders. If you don't use password reminders, you don't have to register this service provider and everything else should work just fine.

### Queues

If you want to use Cassandra as your database backend, change the the driver in `config/queue.php`:

```php
'connections' => [
    'database' => [
        'driver' => 'cassandra',
        'table'  => 'jobs',
        'queue'  => 'default',
        'expire' => 60,
    ],
```

Examples
--------

### Basic Usage

**Retrieving All Models**

```php
$users = User::all();
```

**Retrieving A Record By Primary Key**

```php
$user = User::find('517c43667db388101e00000f');
```

**Wheres**

```php
$users = User::where('votes', '>', 100)->take(10)->get();
```

**Or Statements**

```php
$users = User::where('votes', '>', 100)->orWhere('name', 'John')->get();
```

**And Statements**

```php
$users = User::where('votes', '>', 100)->where('name', '=', 'John')->get();
```

**Using Where In With An Array**

```php
$users = User::whereIn('age', [16, 18, 20])->get();
```

When using `whereNotIn` objects will be returned if the field is non existent. Combine with `whereNotNull('age')` to leave out those documents.

**Using Where Between**

```php
$users = User::whereBetween('votes', [1, 100])->get();
```

**Where null**

```php
$users = User::whereNull('updated_at')->get();
```

**Order By**

```php
$users = User::orderBy('name', 'desc')->get();
```

**Offset & Limit**

```php
$users = User::skip(10)->take(5)->get();
```

**Distinct**

Distinct requires a field for which to return the distinct values.

```php
$users = User::distinct()->get(['name']);
// or
$users = User::distinct('name')->get();
```

Distinct can be combined with **where**:

```php
$users = User::where('active', true)->distinct('name')->get();
```

**Advanced Wheres**

```php
$users = User::where('name', '=', 'John')->orWhere(function($query)
    {
        $query->where('votes', '>', 100)
              ->where('title', '<>', 'Admin');
    })
    ->get();
```

**Group By**

Selected columns that are not grouped will be aggregated with the $last function.

```php
$users = Users::groupBy('title')->get(['title', 'name']);
```

**Aggregation**

*Aggregations are only available for Cassandra versions greater than 2.2.*

```php
$total = Order::count();
$price = Order::max('price');
$price = Order::min('price');
$price = Order::avg('price');
$total = Order::sum('price');
```

Aggregations can be combined with **where**:

```php
$sold = Orders::where('sold', true)->sum('price');
```

**Like**

```php
$user = Comment::where('body', 'like', '%spam%')->get();
```

**Incrementing or decrementing a value of a column**

Perform increments or decrements (default 1) on specified attributes:

```php
User::where('name', 'John Doe')->increment('age');
User::where('name', 'Jaques')->decrement('weight', 50);
```

The number of updated objects is returned:

```php
$count = User->increment('age');
```

You may also specify additional columns to update:

```php
User::where('age', '29')->increment('age', 1, ['group' => 'thirty something']);
User::where('bmi', 30)->decrement('bmi', 1, ['category' => 'overweight']);
```

**Soft deleting**

When soft deleting a model, it is not actually removed from your database. Instead, a deleted_at timestamp is set on the record. To enable soft deletes for a model, apply the SoftDeletingTrait to the model:

```php
use RoiupAgency\Cassadra\Eloquent\SoftDeletes;

class User extends Eloquent {

    use SoftDeletes;

    protected $dates = ['deleted_at'];

}
```

For more information check http://laravel.com/docs/eloquent#soft-deleting

### Cassandra specific operators

**Exists**

Matches documents that have the specified field.

```php
User::where('age', 'exists', true)->get();
```

**All**

Matches arrays that contain all elements specified in the query.

```php
User::where('roles', 'all', ['moderator', 'author'])->get();
```

**Size**

Selects documents if the array field is a specified size.

```php
User::where('tags', 'size', 3)->get();
```

**Regex**

Selects documents where values match a specified regular expression.

```php
User::where('name', 'regex', new CassandraRegex("/.*doe/i"))->get();
```

**NOTE:** you can also use the Laravel regexp operations. These are a bit more flexible and will automatically convert your regular expression string to a CassandraRegex object.

```php
User::where('name', 'regexp', '/.*doe/i'))->get();
```

And the inverse:

```php
User::where('name', 'not regexp', '/.*doe/i'))->get();
```

**Type**

Selects documents if a field is of the specified type. For more information check: http://docs.cassandra.org/manual/reference/operator/query/type/#op._S_type

```php
User::where('age', 'type', 2)->get();
```

**Mod**

Performs a modulo operation on the value of a field and selects documents with a specified result.

```php
User::where('age', 'mod', [10, 0])->get();
```

**Where**

Matches documents that satisfy a JavaScript expression. For more information check http://docs.cassandra.org/manual/reference/operator/query/where/#op._S_where

### Inserts, updates and deletes

Inserting, updating and deleting records works just like the original Eloquent.

**Saving a new model**

```php
$user = new User;
$user->name = 'John';
$user->save();
```

You may also use the create method to save a new model in a single line:

```php
User::create(['name' => 'John']);
```

**Updating a model**

To update a model, you may retrieve it, change an attribute, and use the save method.

```php
$user = User::first();
$user->email = 'john@foo.com';
$user->save();
```

*There is also support for upsert operations, check https://github.com/sashalenz/laravel-cassandra#cassandra-specific-operations*

**Deleting a model**

To delete a model, simply call the delete method on the instance:

```php
$user = User::first();
$user->delete();
```

Or deleting a model by its key:

```php
User::destroy('517c43667db388101e00000f');
```

For more information about model manipulation, check http://laravel.com/docs/eloquent#insert-update-delete

### Dates

Eloquent allows you to work with Carbon/DateTime objects instead of CassandraDate objects. Internally, these dates will be converted to CassandraDate objects when saved to the database. If you wish to use this functionality on non-default date fields you will need to manually specify them as described here: http://laravel.com/docs/eloquent#date-mutators

Example:

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class User extends Eloquent {

    protected $dates = ['birthday'];

}
```

Which allows you to execute queries like:

```php
$users = User::where('birthday', '>', new DateTime('-18 years'))->get();
```

### Relations

Supported relations are:

 - hasOne
 - hasMany
 - belongsTo
 - belongsToMany
 - embedsOne
 - embedsMany

Example:

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class User extends Eloquent {

    public function items()
    {
        return $this->hasMany('Item');
    }

}
```

And the inverse relation:

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class Item extends Eloquent {

    public function user()
    {
        return $this->belongsTo('User');
    }

}
```

The belongsToMany relation will not use a pivot "table", but will push id's to a __related_ids__ attribute instead. This makes the second parameter for the belongsToMany method useless. If you want to define custom keys for your relation, set it to `null`:

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class User extends Eloquent {

    public function groups()
    {
        return $this->belongsToMany('Group', null, 'users', 'groups');
    }

}
```


Other relations are not yet supported, but may be added in the future. Read more about these relations on http://laravel.com/docs/eloquent#relationships

### EmbedsMany Relations

If you want to embed models, rather than referencing them, you can use the `embedsMany` relation. This relation is similar to the `hasMany` relation, but embeds the models inside the parent object.

**REMEMBER**: these relations return Eloquent collections, they don't return query builder objects!

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class User extends Eloquent {

    public function books()
    {
        return $this->embedsMany('Book');
    }

}
```

You access the embedded models through the dynamic property:

```php
$books = User::first()->books;
```

The inverse relation is auto*magically* available, you don't need to define this reverse relation.

```php
$user = $book->user;
```

Inserting and updating embedded models works similar to the `hasMany` relation:

```php
$book = new Book(['title' => 'A Game of Thrones']);

$user = User::first();

$book = $user->books()->save($book);
// or
$book = $user->books()->create(['title' => 'A Game of Thrones'])
```

You can update embedded models using their `save` method (available since release 2.0.0):

```php
$book = $user->books()->first();

$book->title = 'A Game of Thrones';

$book->save();
```

You can remove an embedded model by using the `destroy` method on the relation, or the `delete` method on the model (available since release 2.0.0):

```php
$book = $user->books()->first();

$book->delete();
// or
$user->books()->destroy($book);
```

If you want to add or remove an embedded model, without touching the database, you can use the `associate` and `dissociate` methods. To eventually write the changes to the database, save the parent object:

```php
$user->books()->associate($book);

$user->save();
```

Like other relations, embedsMany assumes the local key of the relationship based on the model name. You can override the default local key by passing a second argument to the embedsMany method:

```php
return $this->embedsMany('Book', 'local_key');
```

Embedded relations will return a Collection of embedded items instead of a query builder. Check out the available operations here: https://laravel.com/docs/master/collections

### EmbedsOne Relations

The embedsOne relation is similar to the EmbedsMany relation, but only embeds a single model.

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class Book extends Eloquent {

    public function author()
    {
        return $this->embedsOne('Author');
    }

}
```

You access the embedded models through the dynamic property:

```php
$author = Book::first()->author;
```

Inserting and updating embedded models works similar to the `hasOne` relation:

```php
$author = new Author(['name' => 'John Doe']);

$book = Books::first();

$author = $book->author()->save($author);
// or
$author = $book->author()->create(['name' => 'John Doe']);
```

You can update the embedded model using the `save` method (available since release 2.0.0):

```php
$author = $book->author;

$author->name = 'Jane Doe';
$author->save();
```

You can replace the embedded model with a new model like this:

```php
$newAuthor = new Author(['name' => 'Jane Doe']);
$book->author()->save($newAuthor);
```

### MySQL Relations

If you're using a hybrid Cassandra and SQL setup, you're in luck! The model will automatically return a Cassandra- or SQL-relation based on the type of the related model. Of course, if you want this functionality to work both ways, your SQL-models will need use the `RoiupAgency\Cassadra\Eloquent\HybridRelations` trait. Note that this functionality only works for hasOne, hasMany and belongsTo relations.

Example SQL-based User model:

```php
use RoiupAgency\Cassadra\Eloquent\HybridRelations;

class User extends Eloquent {

    use HybridRelations;

    protected $connection = 'mysql';

    public function messages()
    {
        return $this->hasMany('Message');
    }

}
```

And the Cassandra-based Message model:

```php
use RoiupAgency\Cassadra\Eloquent\Model as Eloquent;

class Message extends Eloquent {

    protected $connection = 'cassandra';

    public function user()
    {
        return $this->belongsTo('User');
    }

}
```

### Raw Expressions

These expressions will be injected directly into the query.

```php
User::whereRaw(['age' => array('$gt' => 30, '$lt' => 40]))->get();
```

You can also perform raw expressions on the internal CassandraCollection object. If this is executed on the model class, it will return a collection of models. If this is executed on the query builder, it will return the original response.

```php
// Returns a collection of User models.
$models = User::raw(function($collection)
{
    return $collection->find();
});

// Returns the original CassandraCursor.
$cursor = DB::collection('users')->raw(function($collection)
{
    return $collection->find();
});
```

Optional: if you don't pass a closure to the raw method, the internal CassandraCollection object will be accessible:

```php
$model = User::raw()->findOne(['age' => array('$lt' => 18]));
```

The internal CassandraClient and Cassandra objects can be accessed like this:

```php
$client = DB::getCassandraClient();
$db = DB::getCassandra();
```

### Cassandra specific operations

**Cursor timeout**

To prevent CassandraCursorTimeout exceptions, you can manually set a timeout value that will be applied to the cursor:

```php
DB::collection('users')->timeout(-1)->get();
```

**Upsert**

Update or insert a document. Additional options for the update method are passed directly to the native update method.

```php
DB::collection('users')->where('name', 'John')
                       ->update($data, ['upsert' => true]);
```

**Projections**

You can apply projections to your queries using the `project` method.

```php
DB::collection('items')->project(['tags' => array('$slice' => 1]))->get();
```

**Projections with Pagination**

```php
$limit = 25;
$projections = ['id', 'name'];
DB::collection('items')->paginate($limit, $projections);
```


**Push**

Add an items to an array.

```php
DB::collection('users')->where('name', 'John')->push('items', 'boots');
DB::collection('users')->where('name', 'John')->push('messages', ['from' => 'Jane Doe', 'message' => 'Hi John']);
```

If you don't want duplicate items, set the third parameter to `true`:

```php
DB::collection('users')->where('name', 'John')->push('items', 'boots', true);
```

**Pull**

Remove an item from an array.

```php
DB::collection('users')->where('name', 'John')->pull('items', 'boots');
DB::collection('users')->where('name', 'John')->pull('messages', ['from' => 'Jane Doe', 'message' => 'Hi John']);
```

**Unset**

Remove one or more fields from a document.

```php
DB::collection('users')->where('name', 'John')->unset('note');
```

You can also perform an unset on a model.

```php
$user = User::where('name', 'John')->first();
$user->unset('note');
```

### Query Caching

You may easily cache the results of a query using the remember method:

```php
$users = User::remember(10)->get();
```

*From: http://laravel.com/docs/queries#caching-queries*

### Query Logging

By default, Laravel keeps a log in memory of all queries that have been run for the current request. However, in some cases, such as when inserting a large number of rows, this can cause the application to use excess memory. To disable the log, you may use the `disableQueryLog` method:

```php
DB::connection()->disableQueryLog();
```

*From: http://laravel.com/docs/database#query-logging*
