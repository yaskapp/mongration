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

  async migrate(updateChecksum, done) {
    const connection = new MongoConnection(this.dbConfig);
    this.db = await connection.connect();
    let steps = this._loadSteps();

    steps = await this._verify(steps, updateChecksum);

    const errored = steps.find(step => step.status === statuses.error);
    if (errored) {
      await connection.close();
      return done(`Already migrated: ${errored.error}`, this._formatSteps(steps));
    }

    for (let step of steps) {
      if (step.status !== statuses.pending) {
        continue;
      }
      const up = promisify(step.up);
      try {
        console.log(`Running ${step.id}...`);
        await up(this.db);
        console.log('Done');
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

    const errorStep = steps.find(step => step.error);
    const error = errorStep ? errorStep.error : null;
    if (errorStep) {
      delete errorStep.error;
    }
    done(error, this._formatSteps(steps));
  }

  async rollback(done) {
    const connection = new MongoConnection(this.dbConfig);
    this.db = await connection.connect();
    const lastStep = await this.db.collection(this.collection).findOne({}, { sort: { order: -1 } });
    if (!lastStep) {
      await connection.close();
      return done(new Error('Nothing to rollback.'));
    }

    let steps = this._loadSteps();
    const step = steps.find(step => {
      return step.id === lastStep.id;
    });

    if (!step) {
      return done(new Error(`Step ${step.id} not found.`));
    }

    step.status = statuses.pending;

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

    if (step.status === statuses.ok || step.status === statuses.pending) {
      try {
        await down(this.db);
        step.status = statuses.rollback;
        await this.db.collection(this.collection).deleteOne({ id: step.id });
      } catch (err) {
        step.error = step.status === statuses.rollback ?
          `[${step.id}] failed to remove migration version: ${err}`:
          `[${step.id}] unable to rollback migration: ${err}`;
        step.status = statuses.rollbackError;
      }
    }
  }

  _formatSteps(steps) {
    return steps.map(step => {
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

  async _verify(steps, updateChecksum = false) {
    const dbSteps = await this.db.collection(this.collection).find({}).sort({ order: 1 }).toArray();
    dbSteps.forEach(async (dbStep, index) => {
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
        if (!updateChecksum) {
          step.status = statuses.error;
          step.error = `[${dbStep.id}] was already migrated on [${dbStep.date}] in a different version.`;
          return;
        }
        await this.db.collection(this.collection).updateOne(
        { _id: dbStep._id }, { $set: { checksum: step.checksum }}
        );
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
