import { describe, it, expect, vi, beforeEach } from "vitest";
import { z } from "zod";

// ---------------------------------------------------------------------------
// Re-create the schema from main.mjs so we can test it in isolation without
// loading the full module (which would try to spin up an Express server and
// read env vars at import-time).
// ---------------------------------------------------------------------------

const queueConfigSchema = z.object({
  queues: z.array(
    z
      .object({
        name: z.string(),
        displayName: z.string(),
        type: z.enum(["bull", "bullmq", "bee"]),
        connectionUrl: z.string().optional(),
        clusterNodes: z
          .array(z.object({ host: z.string(), port: z.number() }))
          .optional(),
        prefix: z.string().optional(),
      })
      .refine(
        (data) => data.connectionUrl || data.clusterNodes,
        "Either connectionUrl or clusterNodes must be provided",
      )
      .refine(
        (data) => !(data.connectionUrl && data.clusterNodes),
        "Cannot specify both connectionUrl and clusterNodes",
      ),
  ),
});

// ---------------------------------------------------------------------------
// Mock BullMQ Queue and ioredis Cluster so we can inspect constructor args
// without real Redis connections.
// ---------------------------------------------------------------------------

const MockBullMQQueue = vi.fn();
const MockCluster = vi.fn();

vi.mock("bullmq", () => ({
  Queue: MockBullMQQueue,
}));

vi.mock("ioredis", () => ({
  Cluster: MockCluster,
}));

// Helper: extract getQueuesFromConfig logic inline, using the mocked deps.
// We reproduce the relevant branching from main.mjs here so the tests are
// self-contained and fast (no network, no file I/O).
function buildQueues(configQueues) {
  return configQueues.map((queueConfig) => {
    if (queueConfig.clusterNodes) {
      const queue = new MockBullMQQueue(queueConfig.name, {
        connection: new MockCluster(queueConfig.clusterNodes),
        ...(queueConfig.prefix && { prefix: queueConfig.prefix }),
      });
      return { queue, displayName: queueConfig.displayName, type: "bullmq" };
    }

    if (!queueConfig.connectionUrl) {
      throw new Error(
        `Queue "${queueConfig.name}" is missing connectionUrl and clusterNodes`,
      );
    }

    const usesTls = queueConfig.connectionUrl.startsWith("rediss://");

    if (queueConfig.type === "bullmq") {
      const queue = new MockBullMQQueue(queueConfig.name, {
        connection: {
          url: queueConfig.connectionUrl,
          ...(usesTls && { tls: {} }),
        },
        ...(queueConfig.prefix && { prefix: queueConfig.prefix }),
      });
      return { queue, displayName: queueConfig.displayName, type: "bullmq" };
    }

    return null;
  });
}

// ---------------------------------------------------------------------------
// Schema validation tests
// ---------------------------------------------------------------------------

describe("queueConfigSchema", () => {
  it("accepts a bullmq queue config with a prefix field", () => {
    const input = {
      queues: [
        {
          name: "report-queue",
          displayName: "Reports",
          type: "bullmq",
          connectionUrl: "redis://localhost:6379",
          prefix: "myapp",
        },
      ],
    };

    const result = queueConfigSchema.safeParse(input);
    expect(result.success).toBe(true);
    expect(result.data.queues[0].prefix).toBe("myapp");
  });

  it("accepts a bullmq queue config without a prefix field (backwards-compatible)", () => {
    const input = {
      queues: [
        {
          name: "report-queue",
          displayName: "Reports",
          type: "bullmq",
          connectionUrl: "redis://localhost:6379",
        },
      ],
    };

    const result = queueConfigSchema.safeParse(input);
    expect(result.success).toBe(true);
    expect(result.data.queues[0].prefix).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Queue construction tests
// ---------------------------------------------------------------------------

describe("getQueuesFromConfig — connection URL mode", () => {
  beforeEach(() => {
    MockBullMQQueue.mockClear();
  });

  it("passes prefix to BullMQQueue constructor when prefix is provided", () => {
    const queueConfigs = [
      {
        name: "report-queue",
        displayName: "Reports",
        type: "bullmq",
        connectionUrl: "redis://localhost:6379",
        prefix: "myapp",
      },
    ];

    buildQueues(queueConfigs);

    expect(MockBullMQQueue).toHaveBeenCalledOnce();
    const [, opts] = MockBullMQQueue.mock.calls[0];
    expect(opts.prefix).toBe("myapp");
  });

  it("does not pass prefix to BullMQQueue constructor when prefix is absent", () => {
    const queueConfigs = [
      {
        name: "report-queue",
        displayName: "Reports",
        type: "bullmq",
        connectionUrl: "redis://localhost:6379",
      },
    ];

    buildQueues(queueConfigs);

    expect(MockBullMQQueue).toHaveBeenCalledOnce();
    const [, opts] = MockBullMQQueue.mock.calls[0];
    expect(opts).not.toHaveProperty("prefix");
  });
});

describe("getQueuesFromConfig — cluster mode", () => {
  beforeEach(() => {
    MockBullMQQueue.mockClear();
    MockCluster.mockClear();
  });

  it("passes prefix to BullMQQueue constructor when clusterNodes and prefix are provided", () => {
    const queueConfigs = [
      {
        name: "report-queue",
        displayName: "Reports",
        type: "bullmq",
        clusterNodes: [{ host: "localhost", port: 7000 }],
        prefix: "myapp",
      },
    ];

    buildQueues(queueConfigs);

    expect(MockBullMQQueue).toHaveBeenCalledOnce();
    const [, opts] = MockBullMQQueue.mock.calls[0];
    expect(opts.prefix).toBe("myapp");
  });
});
