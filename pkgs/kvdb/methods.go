package kvdb

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/taubyte/go-interfaces/kvdb"
)

// use kvdb and ds to interact with your particular key value database in a way
// that does not depend on its particular implementation, only need to specify keys and values
// and if you want to change the database you use to with one a different structure,
// can reuse existing code


// make a get request to your database to get the value for the input key
func (kvd *kvDatabase) Get(ctx context.Context, key string) ([]byte, error) {
	//NewKey reformats the input string into a key structure the database accepts
	k := ds.NewKey(key)
	//get request with an acceptable key
	return kvd.datastore.Get(ctx, k)
	//return the corresponding value as an array of bytes
	//or an error if the key is not the db
}

// put request to add a new key/value pair to your db
func (kvd *kvDatabase) Put(ctx context.Context, key string, v []byte) error {
	k := ds.NewKey(key)
	return kvd.datastore.Put(ctx, k, v)
	//doesn't return anything on success
	//returns an error if the k/v pair was not added to the db
}

//delete the key/value pair specified by the input key from the database
func (kvd *kvDatabase) Delete(ctx context.Context, key string) error {
	k := ds.NewKey(key)
	return kvd.datastore.Delete(ctx, k)
	//doesn't return anything on success
	//returns an error if the k/v pair was not deleted from the db
}

func (kvd *kvDatabase) List(ctx context.Context, prefix string) ([]string, error) {
	//initialize result to hold a list of all "entries" in the database
	//whose (presumably) Keys match the input prefix (presumambly a property of the Key structure)
	//if there are no matches, then an error is created and nil, err is returned
	result, err := kvd.list(ctx, prefix)
	if err != nil {
		return nil, err
	}

	//create an array of strings
	//iterate over result and for each entry in result, add entry.Key to this array
	//return the keys array

	//I assume entry.Key is a sub_key in string format (the kind that was the input in get/put/del func above)
	//while the result array is filled with Keys in the format returned by ds.NewKey

	keys := make([]string, 0)
	for entry := range result.Next() {
		keys = append(keys, entry.Key)
	}
	return keys, nil
}

//this function is similar to the one above except the request is async
//and a string channel is returned instead of an array
func (kvd *kvDatabase) ListAsync(ctx context.Context, prefix string) (chan string, error) {
	result, err := kvd.list(ctx, prefix)
	if err != nil {
		return nil, err
	}

	//result is created as in the previous function but this time accessing each
	//entry is an async operation
	//inside go func you can make async calls and wait for the response
	//each result.Next() is async and unlike above it is possible to get an error response
	//if for any entry you get an error, you return (nothing) immediately
	//presumably for the use case you must have data for every single entry or you can't proceed
	c := make(chan string, QueryBufferSize)
	go func() {
		defer close(c)
		defer result.Close()
		source := result.Next()
		for {
			select {
			case entry, ok := <-source:
				if !ok || entry.Error != nil {
					return
				}

				//sending the non-error value of entry.Key into the channel
				c <- entry.Key
			case <-ctx.Done():
				return
			}
		}
	}()

	return c, nil
}

//query the database for keys matching a prefix
//it appears similar to the previous two functions except it has a third return type
//a query "object" vs a list vs a channel
func (kvd *kvDatabase) list(ctx context.Context, prefix string) (query.Results, error) {
	return kvd.datastore.Query(ctx, query.Query{
		Prefix:   prefix,
		KeysOnly: true,
	})
}

func (kvd *kvDatabase) Batch(ctx context.Context) (kvdb.Batch, error) {
	b, err := kvd.datastore.Batch(ctx)
	if err != nil {
		return nil, err
	}
	return &Batch{ctx: ctx, store: b}, nil
}

//looked this up and found
//Batch is a write-only database that commits changes to its host database when Write is called
//below functions are

//presumably make put and delete requests to Batch and store that info locally in between
//Syncs with the cloud
//more efficient if most entries won't end up needing to be stored long term, that is
//they are often deleted before a sync

type Batch struct {
	ctx   context.Context
	store ds.Batch
}

func (b *Batch) Put(key string, value []byte) error {
	k := ds.NewKey(key)
	return b.store.Put(b.ctx, k, value)
}

func (b *Batch) Delete(key string) error {
	k := ds.NewKey(key)
	return b.store.Delete(b.ctx, k)
}

func (b *Batch) Commit() error {
	return b.store.Commit(b.ctx)
}

func (kvd *kvDatabase) Sync(ctx context.Context, key string) error {
	k := ds.NewKey(key)
	return kvd.datastore.Sync(ctx, k)
}
