/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.kernel.cache

import sirius.kernel.BaseSpecification
import sirius.kernel.commons.Wait
import sirius.kernel.commons.Watch
import spock.lang.Shared

class ManagedCacheSpec extends BaseSpecification {

    def "test run eviction removes old entries"() {
        given:
        def cache = new ManagedCache("test-cache", null, null)
        when:
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        Wait.millis(1001)
        cache.put("key3", "value3")
        cache.put("key4", "value4")
        then:
        cache.getSize() == 4
        cache.runEviction()
        cache.getSize() == 2
        cache.get("key3") == "value3"
        cache.get("key4") == "value4"
    }

    def "long running map test"() {
        given:
        Cache<String, Map> concurrentCache = CacheManager.createCache("concurrent-cache")
        and:
        for (long i = 0; i < 100; i++) {
            concurrentCache.get(cacheKey(i)) {
                key -> createCachingObject(key)
            }
        }
        and:
        Watch w = Watch.start()
        expect:
        long counter = 0
        boolean containedIt = false
        while (w.elapsedMillis() < 10000l) {
            counter++
            Map map
            String key = cacheKey(w.elapsedMillis())

            if (concurrentCache.contains(key)) {
                map = concurrentCache.get(key)
                containedIt = true
            } else {
                map = createCachingObject(key)
                concurrentCache.put(key, map)
                containedIt = false
            }

            assert map != null
        }
    }

    def cacheKey(long l) {
        return "key" + (l % 100)
    }


    def createCachingObject(String key) {
        def map = new HashMap<String, String>()
        map.put("key1", UUID.randomUUID().toString())
        map.put("key2", UUID.randomUUID().toString())
        map.put("key3", UUID.randomUUID().toString())
        return map
    }


}
