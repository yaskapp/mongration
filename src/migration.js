const assert = require('assert');
const promisify = require('util').promisify;

const fs = require('fs');
const path = require('path');

const statuses = require('./utils/constants').statuses;
const MongoConnection = require('./utils/mongo-connection');
const StepFileReader = require('./steps').Reader;
const StepVersionCollection = require('./steps').VersionCollection;


class Migration {
  constructor(dbConfig) {
    assert.notEqual(dbConfig.migrationCollection, null);
    this.dbConfig = dbConfig;
    this.migrationFiles = [];
    this.collection = dbConfig.migrationCollection;
  }

  add(fileList) {
    this.migrationFiles = this.migrationFiles.concat(fileList);
  }

  addAllFromPath(dirPath) {
    const fileList = fs.readdirSync(dirPath);
    fileList.forEach(file => {
      this.migrationFiles.push(path.join(dirPath, file));
    });
  }

  async migrate(done) {
    const connection = new MongoConnection(this.dbConfig);
    this.db = await connection.connect();
    let steps = this._loadSteps();

    steps = await this._verify(steps);

    const errored = steps.find(step => step.status === statuses.error);
    if (errored) {
      return done(errored);
    }

    for (let step of steps) {
      if (step.status !== statuses.pending) {
        continue;
      }
      const up = promisify(step.up);
      try {
        await up(this.db);
        step.status = statuses.ok;
        await this.db.collection(this.collection).insertOne(
          new StepVersionCollection(step.id, step.checksum, step.order, new Date())
        );
      } catch (err) {
        await this.rollbackStep(step);
        if (step.status === statuses.rollback) {
          step.error = `Failed migration: ${err}.`;
        }
        break;
      }
    }
    await connection.close();
    done(null, this._formatSteps(steps));
  }

  async rollback(done) {
    const connection = new MongoConnection(this.dbConfig);
    this.db = await connection.connect();
    const lastStep = await this.db.collection(this.collection).findOne({}, { sort: { order: -1 } });
    if (!lastStep) {
      return done(new Error('Nothing to rollback.'));
    }

    let steps = this._loadSteps();
    const step = steps.find(step => {
      return step.id === lastStep.id;
    });

    if (!step) {
      return done(new Error(`Step ${step.id} not found.`));
    }

    await this.rollbackStep(step);
    await connection.close();
    done(null, this._formatSteps(steps));
  }

  async rollbackStep(step) {
    if (!step.down) {
      step.error = `Tried to rollback ${step.id} but there is no down method.`;
      step.status = statuses.rollbackError;
      return;
    }

    const down = promisify(step.down);

    if (step.status === statuses.ok) {
      try {
        await down(this.db);
        step.status = statuses.rollback;
        await this.db.collection(this.collection).remove({ id: step.id });
      } catch (err) {
        step.error = `[${step.id}] failed to remove migration version: ${err}`;
        step.status = statuses.rollbackError;
      }
    }
  }

  _formatSteps(steps) {
    return steps.map(step => {
      if (step.error) {
        return {
          id: step.id,
          status: step.status,
          error: step.error
        };
      }
      return {
        id: step.id,
        status: step.status
      };
    });
  }

  _loadSteps() {
    const steps = [];
    this.migrationFiles.forEach((path, index) => {
      const step = new StepFileReader(path).read().getStep();
      step.order = index;
      steps.push(step);
    });
    return steps;
  }

  async _verify(steps) {
    const dbSteps = await this.db.collection(this.collection).find({}).sort({ order: 1 }).toArray();
    dbSteps.forEach((dbStep, index) => {
      const step = steps[index];

      if (!step) {
        return;
      }

      if (step.id !== dbStep.id || step.order !== dbStep.order) {
        step.status = statuses.error;
        step.error = `${step.id} changed order from ${dbStep.order} to ${step.order}.`;
        return;
      }

      if (dbStep.checksum !== step.checksum) {
        step.status = statuses.error;
        step.error = `[${dbStep.id}] was already migrated on [${dbStep.date}] in a different version.`;
        return;
      }

      steps[index].status = statuses.skipped;
    });

    return steps.map(step => {
      if (step.status === statuses.notRun) {
        step.status = statuses.pending;
      }
      return step;
    });
  }
}

module.exports = Migration;
