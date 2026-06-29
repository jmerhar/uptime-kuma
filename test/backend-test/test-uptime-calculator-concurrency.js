const { describe, test, before, after } = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const dayjs = require("dayjs");
dayjs.extend(require("dayjs/plugin/utc"));
dayjs.extend(require("../../server/modules/dayjs/plugin/timezone"));
const { UP } = require("../../src/util");

/**
 * Regression test for #5357: concurrent stat writes must not collide on the
 * UNIQUE(monitor_id, timestamp) constraint.
 *
 * UptimeCalculator.update() is called concurrently from the push endpoint and
 * the beat watcher, so two beats for the same bucket can be in flight at once.
 * The write must be an atomic upsert: a separate read and insert lets both
 * calls INSERT and fail with "UNIQUE constraint failed".
 *
 * This needs the real DB write path, which is skipped when TEST_BACKEND is set
 * (see UptimeCalculator.update()), so the suite spins up a real SQLite database
 * and clears TEST_BACKEND for the duration.
 */
describe("UptimeCalculator concurrent stat writes (#5357)", () => {
    const testDbPath = path.join(__dirname, "../../data/test-uptime-calculator-concurrency.db");
    const savedTestBackend = process.env.TEST_BACKEND;
    let R;
    let UptimeCalculator;

    before(async () => {
        // The write path under test is bypassed while TEST_BACKEND is set.
        delete process.env.TEST_BACKEND;

        const testDbDir = path.dirname(testDbPath);
        if (!fs.existsSync(testDbDir)) {
            fs.mkdirSync(testDbDir, { recursive: true });
        }
        if (fs.existsSync(testDbPath)) {
            fs.unlinkSync(testDbPath);
        }

        // Use the same SQLite driver as the project.
        const Dialect = require("knex/lib/dialects/sqlite3/index.js");
        Dialect.prototype._driver = () => require("@louislam/sqlite3");

        const knex = require("knex");
        const db = knex({
            client: Dialect,
            connection: {
                filename: testDbPath,
            },
            useNullAsDefault: true,
        });

        ({ R } = require("redbean-node"));
        R.setup(db);

        // Initialise the schema like production first-run does.
        const { createTables } = require("../../db/knex_init_db.js");
        await createTables();
        await R.knex.migrate.latest({
            directory: path.join(__dirname, "../../db/knex_migrations"),
        });

        ({ UptimeCalculator } = require("../../server/uptime-calculator"));
    });

    after(async () => {
        await R.knex.destroy();
        if (fs.existsSync(testDbPath)) {
            fs.unlinkSync(testDbPath);
        }
        if (savedTestBackend !== undefined) {
            process.env.TEST_BACKEND = savedTestBackend;
        }
    });

    test("concurrent updates for the same bucket do not collide on the UNIQUE constraint", async () => {
        const insertResult = await R.knex("monitor").insert({ name: "concurrency-test" });
        const monitorID = Array.isArray(insertResult) ? insertResult[0] : insertResult;

        const calculator = new UptimeCalculator();
        await calculator.init(monitorID);

        // Two beats for the same bucket fired together, mirroring a push and the beat
        // watcher landing in the same tick. Without an atomic upsert one of these
        // rejects with "SQLITE_CONSTRAINT: UNIQUE constraint failed".
        await assert.doesNotReject(Promise.all([calculator.update(UP, 100), calculator.update(UP, 100)]));

        // Exactly one row per granularity, counting both beats: no duplicate, no lost update.
        for (const table of ["stat_minutely", "stat_hourly", "stat_daily"]) {
            const rows = await R.knex(table).where({ monitor_id: monitorID });
            assert.strictEqual(rows.length, 1, `${table} should have exactly one row`);
            assert.strictEqual(rows[0].up, 2, `${table}.up should count both beats`);
        }
    });

    test("reading the chart data does not leak its timestamp tag into stored extras", async () => {
        const insertResult = await R.knex("monitor").insert({ name: "extras-test" });
        const monitorID = Array.isArray(insertResult) ? insertResult[0] : insertResult;

        const calculator = new UptimeCalculator();
        await calculator.init(monitorID);

        await calculator.update(UP, 100);
        // getDataArray() tags each live bucket with its key as `timestamp`
        calculator.getDataArray(1, "minute");
        calculator.getDataArray(1, "hour");
        calculator.getDataArray(1, "day");
        await calculator.update(UP, 100);

        for (const table of ["stat_minutely", "stat_hourly", "stat_daily"]) {
            const [row] = await R.knex(table).where({ monitor_id: monitorID });
            assert.strictEqual(row.extras, null, `${table}.extras should stay empty`);
        }
    });
});
