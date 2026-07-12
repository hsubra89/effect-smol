import { NodeRedis } from "@effect/platform-node"
import { assert, it } from "@effect/vitest"
import { RedisContainer } from "@testcontainers/redis"
import { Effect, Fiber, Latch, Layer, Schema } from "effect"
import * as PersistedCacheTest from "effect-test/unstable/persistence/PersistedCacheTest"
import * as PersistedQueueTest from "effect-test/unstable/persistence/PersistedQueueTest"
import { PersistedQueue, Persistence } from "effect/unstable/persistence"

const RedisLayer = Layer.unwrap(
  Effect.gen(function*() {
    const container = yield* Effect.acquireRelease(
      Effect.promise(() => new RedisContainer("redis:alpine").start()),
      (container) => Effect.promise(() => container.stop())
    )
    return NodeRedis.layer({
      host: container.getHost(),
      port: container.getMappedPort(6379)
    })
  }).pipe(
    Effect.catchCause(() => Effect.fail(new PersistedCacheTest.TransientError()))
  )
)

PersistedCacheTest.suite(
  "NodeRedis",
  Persistence.layerRedis.pipe(Layer.provide(RedisLayer))
)

PersistedQueueTest.suite(
  "NodeRedis",
  // short intervals so the periodic reset runs while the suite's takes are
  // in flight
  PersistedQueue.layerStoreRedis({
    pollInterval: "50 millis",
    lockRefreshInterval: "100 millis"
  }).pipe(Layer.provide(RedisLayer))
)

const PersistedQueueRedisLayer = Layer.mergeAll(
  RedisLayer,
  PersistedQueue.layer.pipe(
    Layer.provideMerge(
      PersistedQueue.layerStoreRedis().pipe(Layer.provide(RedisLayer))
    )
  )
)

it.layer(PersistedQueueRedisLayer, { timeout: "30 seconds" })(
  "PersistedQueue (NodeRedis)",
  (it) => {
    // The shared PersistedQueue suite can only assert that exhausted elements
    // are no longer delivered, which is also true if they are silently
    // dropped. There is no public API for reading failed elements, so
    // verifying they are preserved in the dead-letter list requires
    // inspecting Redis directly.
    it.effect("moves exhausted elements to the failed list", () =>
      Effect.gen(function*() {
        const redis = yield* NodeRedis.NodeRedis
        const queueName = "test-redis-failed"

        const queue = yield* PersistedQueue.make({
          name: queueName,
          schema: RedisItem
        })
        const id = yield* queue.offer({ n: 42 })
        const error = yield* queue.take(() => Effect.fail("boom"), { maxAttempts: 1 }).pipe(Effect.flip)
        assert.strictEqual(error, "boom")

        const failed = yield* redis.use((client) => client.lrange(`effectq:${queueName}:failed`, 0, -1))
        assert.strictEqual(failed.length, 1)
        const failedItem = JSON.parse(failed[0])
        assert.strictEqual(failedItem.id, id)
        assert.deepStrictEqual(failedItem.element, { n: 42 })
        assert.strictEqual(failedItem.attempts, 1)

        const pending = yield* redis.use((client) => client.hlen(`effectq:${queueName}:pending`))
        assert.strictEqual(pending, 0)
      }))

    it.effect("fences a stale completion", () => staleFinalizerScenario("complete"))
    it.effect("fences a stale retry", () => staleFinalizerScenario("retry"))
    it.effect("fences a stale dead-letter", () => staleFinalizerScenario("failed"))
  }
)

const staleFinalizerScenario = Effect.fnUntraced(function*(outcome: "complete" | "retry" | "failed") {
  const redis = yield* NodeRedis.NodeRedis
  const queueName = `test-redis-stale-${outcome}`
  const id = `${queueName}-id`
  const lockKey = `effectq:${id}:lock`
  const pendingKey = `effectq:${queueName}:pending`
  const queueKey = `effectq:${queueName}`
  const failedKey = `${queueKey}:failed`
  const storeOptions = {
    pollInterval: "10 millis",
    lockRefreshInterval: "1 hour",
    lockExpiration: "1 hour"
  } as const

  const storeA = yield* PersistedQueue.makeStoreRedis(storeOptions)
  const storeB = yield* PersistedQueue.makeStoreRedis(storeOptions)
  const factoryA = yield* PersistedQueue.makeFactory.pipe(
    Effect.provideService(PersistedQueue.PersistedQueueStore, storeA)
  )
  const factoryB = yield* PersistedQueue.makeFactory.pipe(
    Effect.provideService(PersistedQueue.PersistedQueueStore, storeB)
  )
  const queueA = yield* factoryA.make({ name: queueName, schema: RedisItem })
  const queueB = yield* factoryB.make({ name: queueName, schema: RedisItem })

  yield* queueA.offer({ n: 42 }, { id })

  const takenA = Latch.makeUnsafe()
  const releaseA = Latch.makeUnsafe()
  const fiberA = yield* queueA.take(
    () =>
      Effect.andThen(
        takenA.open,
        Effect.andThen(releaseA.await, outcome === "complete" ? Effect.void : Effect.fail("boom"))
      ),
    { maxAttempts: outcome === "failed" ? 1 : 2 }
  ).pipe(Effect.forkScoped)
  yield* takenA.await

  const ownerA = yield* redis.use((client) => client.get(lockKey))
  assert.notStrictEqual(ownerA, null)
  yield* redis.use((client) => client.del(lockKey))

  const takenB = Latch.makeUnsafe()
  const releaseB = Latch.makeUnsafe()
  const fiberB = yield* queueB.take(() => Effect.andThen(takenB.open, releaseB.await)).pipe(
    Effect.forkScoped
  )
  yield* takenB.await

  const ownerB = yield* redis.use((client) => client.get(lockKey))
  assert.notStrictEqual(ownerB, null)
  assert.notStrictEqual(ownerB, ownerA)

  yield* releaseA.open
  if (outcome === "complete") {
    yield* Fiber.join(fiberA)
  } else {
    assert.strictEqual(yield* Fiber.join(fiberA).pipe(Effect.flip), "boom")
  }

  assert.strictEqual(yield* redis.use((client) => client.get(lockKey)), ownerB)
  assert.strictEqual(yield* redis.use((client) => client.hexists(pendingKey, id)), 1)
  assert.strictEqual(yield* redis.use((client) => client.llen(queueKey)), 0)
  assert.strictEqual(yield* redis.use((client) => client.llen(failedKey)), 0)

  yield* releaseB.open
  yield* Fiber.join(fiberB)

  assert.strictEqual(yield* redis.use((client) => client.exists(lockKey)), 0)
  assert.strictEqual(yield* redis.use((client) => client.hexists(pendingKey, id)), 0)
  assert.strictEqual(yield* redis.use((client) => client.llen(queueKey)), 0)
  assert.strictEqual(yield* redis.use((client) => client.llen(failedKey)), 0)
})

const RedisItem = Schema.Struct({
  n: Schema.Number
})
