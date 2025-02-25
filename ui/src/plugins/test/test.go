package answer

import (
	"context"
	"embed"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/apache/answer-plugins/util"
	"github.com/apache/answer/plugin"
	"github.com/segmentfault/pacman/log"
)

//go:embed  info.yaml
var Info embed.FS

type Test struct {
}

func init() {
	plugin.Register(&Test{})
}

func (Test) Info() plugin.Info {
	info := &util.Info{}
	info.GetInfo(Info)

	return plugin.Info{
		Name:        plugin.MakeTranslator("Test"),
		SlugName:    info.SlugName,
		Description: plugin.MakeTranslator(""),
		Author:      info.Author,
		Version:     info.Version,
		Link:        info.Link,
	}
}

func (t *Test) SetOperator(operator *plugin.KVOperator) {
	// Test basic CRUD operations
	t.testBasicCRUD(operator)

	// Test transaction rollback
	t.testTransactionRollback(operator)

	// Test cache consistency
	t.testCacheConsistency(operator)

	// Test edge cases
	t.testEdgeCases(operator)

	// Test concurrent safety
	t.testConcurrentOperations(operator)

	// Test cache penetration protection
	t.testCachePenetration(operator)

	// Test nested transactions
	t.testNestedTransaction(operator)

	log.Info("All KV storage tests completed")
}

// Basic CRUD test
func (t *Test) testBasicCRUD(kv *plugin.KVOperator) {
	ctx := context.Background()

	defer func() {
		// Clean up test data
		kv.Del(ctx, "group1", "key1")
		kv.Del(ctx, "group1", "key2")
		kv.Del(ctx, "group1", "key3")
	}()

	// TestSet/Get
	_ = kv.Set(ctx, "group1", "key1", "value1")
	val, _ := kv.Get(ctx, "group1", "key1")
	if val != "value1" {
		log.Errorf("Basic Set/Get test failed, expected value1 got %s", val)
	}

	// Test update
	_ = kv.Set(ctx, "group1", "key1", "new_value")
	updatedVal, _ := kv.Get(ctx, "group1", "key1")
	if updatedVal != "new_value" {
		log.Errorf("Update test failed, expected new_value got %s", updatedVal)
	}

	// Test delete
	_ = kv.Del(ctx, "group1", "key1")
	deletedVal, err := kv.Get(ctx, "group1", "key1")
	if err != plugin.ErrKVKeyNotFound {
		log.Errorf("Delete test failed, expected ErrKVKeyNotFound but got: %v", err)
	}
	if deletedVal != "" {
		log.Errorf("Delete test failed, expected empty value but got: %s", deletedVal)
	}

	// Test group operation
	_ = kv.Set(ctx, "group1", "key2", "value2")
	_ = kv.Set(ctx, "group1", "key3", "value3")
	groupData, _ := kv.GetByGroup(ctx, "group1", 1, 10)
	if len(groupData) != 2 {
		log.Errorf("Group query failed, expected 2 items got %d", len(groupData))
	}
	if groupData["key2"] != "value2" || groupData["key3"] != "value3" {
		log.Errorf("Group query failed, expected key2: value2, key3: value3 got %v", groupData)
	}
}

// Test transaction rollback
func (t *Test) testTransactionRollback(kv *plugin.KVOperator) {
	ctx := context.Background()

	defer func() {
		// Clean up test data
		kv.Del(ctx, "tx_group", "tx_key1")
		kv.Del(ctx, "tx_group", "tx_key2")
		kv.Del(ctx, "tx_group", "tx_key3")
	}()

	// Successful transaction
	kv.Tx(ctx, func(ctx context.Context, txKv *plugin.KVOperator) error {
		txKv.Set(ctx, "tx_group", "tx_key1", "tx_value1")
		txKv.Set(ctx, "tx_group", "tx_key2", "tx_value2")
		return nil
	})

	// Get value from successful transaction
	val, err := kv.Get(ctx, "tx_group", "tx_key1")
	if err != nil {
		log.Error("Transaction success test failed, got error: %v", err)
	}
	if val != "tx_value1" {
		log.Error("Transaction success test failed, tx_key1 should have value 'tx_value1' but got: %s", val)
	}
	val, err = kv.Get(ctx, "tx_group", "tx_key2")
	if err != nil {
		log.Error("Transaction success test failed, got error for tx_key2: %v", err)
	}
	if val != "tx_value2" {
		log.Error("Transaction success test failed, tx_key2 should have value 'tx_value2' but got: %s", val)
	}

	// Failed transaction
	kv.Tx(ctx, func(ctx context.Context, txKv *plugin.KVOperator) error {
		txKv.Set(ctx, "tx_group", "tx_key3", "tx_value3")
		return fmt.Errorf("mock error")
	})

	val, err = kv.Get(ctx, "tx_group", "tx_key3")
	if err != plugin.ErrKVKeyNotFound && val != "" {
		log.Error("Transaction rollback failed, tx_key3 still exists")
	}
}

// Test cache consistency
func (t *Test) testCacheConsistency(kv *plugin.KVOperator) {
	ctx := context.Background()

	defer func() {
		// Clean up test data
		kv.Del(ctx, "cache_group", "cache_key")
	}()

	// Initial setup
	kv.Set(ctx, "cache_group", "cache_key", "cache_value")

	// Get cache before modification
	kv.Get(ctx, "cache_group", "cache_key")

	// Update value
	kv.Set(ctx, "cache_group", "cache_key", "updated_value")

	// Immediately get should get the new value
	secondGet, err := kv.Get(ctx, "cache_group", "cache_key")
	if err != nil {
		log.Error("Cache consistency test failed, got error: %v", err)
	}
	if secondGet != "updated_value" {
		log.Error("Cache consistency test failed, expected 'updated_value' but got: %s", secondGet)
	}
}

// Test edge cases
func (t *Test) testEdgeCases(kv *plugin.KVOperator) {
	ctx := context.Background()

	// Empty key test
	err := kv.Set(ctx, "group", "", "value")
	if err != plugin.ErrKVKeyEmpty {
		log.Error("Empty key test failed, should return error")
	}

	// Empty group query
	_, err = kv.GetByGroup(ctx, "", 1, 10)
	if err != plugin.ErrKVGroupEmpty {
		log.Error("Empty group query test failed, should return error")
	}

	// Non-exist key test
	val, err := kv.Get(ctx, "non_exist_group", "non_exist_key")
	if err != plugin.ErrKVKeyNotFound && val != "" {
		log.Error("Non-exist key test failed, should return empty")
	}
}

// Test concurrent safety
func (t *Test) testConcurrentOperations(kv *plugin.KVOperator) {
	ctx := context.Background()
	const parallel = 10
	var wg sync.WaitGroup

	defer func() {
		// Clean up test data
		for i := 0; i < parallel; i++ {
			kv.Del(ctx, "concurrent", fmt.Sprintf("key%d", i))
		}
	}()

	// Concurrent write test
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		go func(index int) {
			defer wg.Done()
			err := kv.Set(ctx, "concurrent", fmt.Sprintf("key%d", index), "value")
			if err != nil {
				log.Errorf("Concurrent write error: %v", err)
			}
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		}(i)
	}
	wg.Wait()

	// Verify write result
	groupData, err := kv.GetByGroup(ctx, "concurrent", 1, 100)
	if err != nil {
		log.Errorf("Concurrent write test failed, %v", err)
	}
	if len(groupData) != parallel {
		log.Errorf("Concurrent write test failed, expected %d items got %d", parallel, len(groupData))
	}
	for i := 0; i < parallel; i++ {
		val, err := kv.Get(ctx, "concurrent", fmt.Sprintf("key%d", i))
		if err != nil {
			log.Errorf("Concurrent write test failed, expected value for key%d got error", i)
		}
		if val != "value" {
			log.Errorf("Concurrent write test failed, expected value for key%d got %s", i, val)
		}
		if val, ok := groupData[fmt.Sprintf("key%d", i)]; !ok || val != "value" {
			log.Errorf("Concurrent write test failed, expected value for key%d got %s", i, val)
		}
	}
}

// Test cache penetration protection
func (t *Test) testCachePenetration(kv *plugin.KVOperator) {
	ctx := context.Background()
	key := "non_exist_cache_key"

	// First query (should cache empty value)
	val1, err := kv.Get(ctx, "cache_penetration", key)
	if err != nil && err != plugin.ErrKVKeyNotFound {
		log.Errorf("Cache penetration protection test failed, unexpected error: %v", err)
	}
	if val1 != "" {
		log.Errorf("Cache penetration protection test failed, expected empty value but got: %s", val1)
	}
}

// Test nested transactions
func (t *Test) testNestedTransaction(kv *plugin.KVOperator) {
	ctx := context.Background()

	defer func() {
		// Clean up test data
		kv.Del(ctx, "nested", "key1")
		kv.Del(ctx, "nested", "key2")
	}()

	kv.Tx(ctx, func(ctx context.Context, txKv *plugin.KVOperator) error {
		txKv.Set(ctx, "nested", "key1", "value1")

		return txKv.Tx(ctx, func(ctx context.Context, nestedKv *plugin.KVOperator) error {
			nestedKv.Set(ctx, "nested", "key2", "value2")
			return fmt.Errorf("mock nested error")
		})
	})

	// Verify outer transaction also rolled back
	val1, err1 := kv.Get(ctx, "nested", "key1")
	val2, err2 := kv.Get(ctx, "nested", "key2")

	if err1 != plugin.ErrKVKeyNotFound {
		log.Errorf("Nested transaction test failed, key1 should not exist but got error: %v", err1)
	}
	if val1 != "" {
		log.Errorf("Nested transaction test failed, key1 should be empty but got: %s", val1)
	}
	if err2 != plugin.ErrKVKeyNotFound {
		log.Errorf("Nested transaction test failed, key2 should not exist but got error: %v", err2)
	}
	if val2 != "" {
		log.Errorf("Nested transaction test failed, key2 should be empty but got: %s", val2)
	}
}
