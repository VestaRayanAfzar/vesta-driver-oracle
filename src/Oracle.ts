import {
    Condition,
    Database,
    DatabaseError,
    Err,
    Field,
    FieldType,
    IDatabaseConfig,
    IDeleteResult,
    IFieldProperties,
    IModel,
    IModelCollection,
    IModelFields,
    IQueryOption,
    IQueryResult,
    ISchemaList,
    IUpsertResult,
    Model,
    RelationType,
    Schema,
    Transaction,
    Vql,
} from "@vesta/core";
import { Connection, createPool, IConnection, IConnectionPool } from "oracledb";
import { isUndefined } from "util";

interface IRelation {
    name: string;
    fields: string[];
}

interface ICalculatedQueryOptions {
    limit: string;
    orderBy: string;
    fields: string;
    fieldsList: string[];
    condition: string;
    join: string;

}

export interface IOracleConfig extends IDatabaseConfig {
    disableTransaction: boolean;
    charset: string;
}

export class Oracle implements Database {

    private pool: IConnectionPool;
    private connection: IConnection;
    private schemaList: ISchemaList = {};
    private config: IOracleConfig;
    private models: IModelCollection;
    private primaryKeys: { [name: string]: string } = {};
    private quote = "<#quote#>";

    constructor(config: IOracleConfig, models: IModelCollection) {
        this.schemaList = {};
        this.models = {};
        for (const model in models) {
            if (models.hasOwnProperty(model)) {
                this.schemaList[models[model].schema.name] = models[model].schema;
                this.models[models[model].schema.name] = models[model];
                this.pk(models[model].schema.name);
            }
        }

        this.config = config;
        this.config.charset = this.config.charset || "utf8mb4";
    }

    public connect(force = false): Promise<Database> {
        if (this.pool && !force) {
            return Promise.resolve(this);
        }
        return new Promise<Database>((resolve, reject) => {
            return createPool({
                connectString: `${this.config.host}:${this.config.port}/${this.config.database}`,
                password: this.config.password,
                user: this.config.user,
            }).then((pool) => {
                this.pool = pool;
                resolve(this);
            }).catch((err) => reject(err));

        });
    }

    public init(): Promise<any> {
        let createSchemaPromise = this.initializeDatabase();
        for (let i = 0, schemaNames = Object.keys(this.schemaList), il = schemaNames.length; i < il; i++) {
            createSchemaPromise = createSchemaPromise.then(this.createTable(this.schemaList[schemaNames[i]]));
        }
        return createSchemaPromise;
    }

    public count<T>(model: string, modelValues: T, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>>;
    public count<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>>;
    public count<T>(arg1: string | Vql, modelValues?: T, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>> {
        const prepare: Promise<any> = transaction ? this.prepareTransaction(transaction) : Promise.resolve({});
        return prepare.then(() => {
            if ("string" == typeof arg1) {
                return this.countByModelValues<T>(arg1 as string, modelValues, option, transaction);
            } else {
                return this.countByQuery<T>(arg1 as Vql, transaction);
            }
        });
    }

    public find<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>>;
    public find<T>(model: string, id: number | string | T, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>>;
    // tslint:disable-next-line:unified-signatures
    public find<T>(model: string, modelValues: T, option?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>>;
    public find<T>(arg1: string | Vql, arg2?: number | string | T, arg3?: IQueryOption, transaction?: Transaction): Promise<IQueryResult<T>> {
        const prepare: Promise<IQueryResult<T> | Transaction> = transaction ? this.prepareTransaction(transaction) : Promise.resolve({} as Promise<IQueryResult<T>>);
        return prepare.then(() => {
            if ("string" == typeof arg1) {
                if (+arg2) { return this.findById<T>(arg1, arg2 as number | string, arg3, transaction); } else { return this.findByModelValues<T>(arg1, arg2 as T, arg3, transaction); }
            } else {
                return this.findByQuery<T>(arg1 as Vql, transaction);
            }
        });
    }

    public increase<T>(model: string, id: number | string, field: string, value: number, transaction?: Transaction): Promise<IUpsertResult<T>> {
        const start: Promise<Transaction> = !transaction ? Promise.resolve(null) : this.prepareTransaction(transaction);
        return start.then((tr) => this.query(`UPDATE \`${model}\` SET \`${field}\` = \`${field}\` + (?) WHERE ${this.pk(model)} = ?`, [value, id], tr))
            .then((data) => {
                return this.findById<T>(model, id) as Promise<IUpsertResult<T>>;
            });
    }

    // public insert<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>>;
    // public insert<T>(model: string, values: T|T[], transaction?: Transaction): Promise<IUpsertResult<T>>;
    public insert<T>(model: string, value: T[] | T, transaction?: Transaction): Promise<IUpsertResult<T>> {
        if (value instanceof Array) {
            return this.insertAll<T>(model, value as T[], transaction);
        } else {
            return this.insertOne<T>(model, value as T, transaction);
        }
    }

    public update<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>>;
    public update<T>(model: string, newValues: T, condition: Condition, transaction?: Transaction): Promise<IUpsertResult<T>>;
    public update<T>(model: string, value: T, arg3?: Condition | Transaction, arg4?: Transaction): Promise<IUpsertResult<T>> {
        if (arg3 instanceof Condition) {
            return this.updateAll(model, value, arg3 as Condition, arg4 as Transaction);
        } else {
            return this.updateOne(model, value, arg3 as Transaction);
        }
    }

    public remove(model: string, id: number | string, transaction?: Transaction): Promise<IDeleteResult>;
    // tslint:disable-next-line:unified-signatures
    public remove(model: string, condition: Condition, transaction?: Transaction): Promise<IDeleteResult>;
    public remove(model: string, arg2: Condition | number | string, transaction?: Transaction): Promise<IDeleteResult> {
        if ("string" == typeof arg2 || "number" == typeof arg2) {
            return this.deleteOne(model, arg2 as number | string, transaction);
        } else if (arg2 instanceof Condition) {
            return this.deleteAll(model, arg2, transaction);
        } else {
            return Promise.reject<IDeleteResult>({
                error: new Err(Err.Code.WrongInput, "invalid delete request"),
                items: null,
            });
        }
    }

    public query<T>(query: string, data?: Array<number | string | Array<number | string>>, transaction?: Transaction): Promise<T> {
        if (!transaction) {
            return new Promise((resolve, reject) => {
                this.getConnection().then((connection) => {
                    connection.execute(query, data, (err, result) => {
                        connection.release();
                        if (err && err.fatal) {
                            this.close(connection).then(() => reject(err)).catch(() => reject(err));
                        } else if (err) {
                            return reject(err);
                        } else {
                            connection.commit();
                            resolve(result as T);
                        }
                    });
                });

            });
        } else {
            return this.prepareTransaction(transaction)
                .then((tr) => new Promise<T>((resolve, reject) => {
                    const connection: IConnection = tr.connection as IConnection;
                    connection.execute(query, data, (err, result) => {
                        if (err && err.fatal) {
                            reject(err);
                        } else if (err) {
                            return reject(err);
                        } else {
                            resolve(result as T);
                        }
                    });
                }),
            );
        }
    }

    public close(connection: IConnection): Promise<boolean> {
        return new Promise((resolve, reject) => {
            if (connection) {
                connection.close((err) => {
                    if (err) {
                        connection.release();
                    }
                    resolve(true);
                });
            } else {
                resolve(true);
            }

        });
    }

    private getConnection(): Promise<IConnection> {
        return new Promise<IConnection>((resolve, reject) => {
            this.pool.getConnection((err, connection) => {
                if (err) { return reject(new DatabaseError(Err.Code.DBConnection, err)); }
                resolve(connection);
            });
        });
    }

    private pk(modelName): string {
        let pk = "id";
        if (this.primaryKeys[modelName]) {
            return this.primaryKeys[modelName];
        } else {
            const fields = this.schemaList[modelName].getFields();
            for (let i = 0, keys = Object.keys(fields), il = keys.length; i < il; i++) {
                if (fields[keys[i]].properties.primary) {
                    pk = keys[i];
                    break;
                }
            }
        }
        this.primaryKeys[modelName] = pk;
        return pk;
    }

    private prepareTransaction(transaction?: Transaction): Promise<Transaction> {
        if (!transaction) { transaction = new Transaction(); }
        if (transaction.connection) { return Promise.resolve(transaction); }
        return this.getConnection().then((connection) => {
            transaction.connection = connection;
            transaction.commit = () => new Promise((resolve, reject) => connection.commit((err) => {
                if (err && err.fatal) {
                    this.close(connection).then(() => reject(err)).catch(() => reject(err));
                } else {
                    connection.release();
                }
                err ? reject(err) : resolve(true);
            }));
            transaction.rollback = () => new Promise((resolve, reject) => connection.rollback(() => {
                connection.release();
                resolve(true);
            }));
            return transaction;
        });
    }

    private findById<T>(model: string, id: number | string, option: IQueryOption = {}, transaction?: Transaction): Promise<IQueryResult<T>> {
        const query = new Vql(model);
        query.where(new Condition(Condition.Operator.EqualTo).compare(this.pk(model), id));
        if (option.fields) { query.select(...option.fields); }
        if (option.relations) { query.fetchRecordFor(...option.relations); }
        query.orderBy = option.orderBy || [];
        query.limitTo(1);
        return this.findByQuery(query, transaction);
    }

    private findByModelValues<T>(model: string, modelValues: T, option: IQueryOption = {}, transaction?: Transaction): Promise<IQueryResult<T>> {
        const condition = new Condition(Condition.Operator.And);
        for (let i = 0, keys = Object.keys(modelValues), il = keys.length; i < il; i++) {
            if (modelValues[keys[i]] !== undefined) {
                condition.append((new Condition(Condition.Operator.EqualTo)).compare(keys[i], modelValues[keys[i]]));
            }
        }
        const query = new Vql(model);
        if (option.fields) { query.select(...option.fields); }
        if (option.offset || option.page) {
            query.fromOffset(option.offset ? option.offset : (option.page - 1) * option.limit);
        }
        if (option.relations) { query.fetchRecordFor(...option.relations); }
        if (+option.limit) { query.limitTo(option.limit); }
        query.where(condition);
        query.orderBy = option.orderBy || [];
        return this.findByQuery(query, transaction);
    }

    private findByQuery<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>> {
        const params: ICalculatedQueryOptions = this.getQueryParams(query);
        const result: IQueryResult<T> = {} as IQueryResult<T>;
        params.condition = params.condition ? "WHERE " + params.condition : "";
        params.orderBy = params.orderBy ? "ORDER BY " + params.orderBy : "";
        return this.query<T[]>(`SELECT ${params.fields} FROM \`${query.model}\` ${params.join} ${params.condition} ${params.orderBy} ${params.limit}`, null, transaction)
            .then((list) => {
                return Promise.all([
                    this.getManyToManyRelation(list, query, transaction),
                    this.getLists(list, query, transaction),
                ]).then(() => list);
            })
            .then((list) => {
                result.items = this.normalizeList(this.schemaList[query.model], list);
                result.total = result.items.length;
                return result;
            })
            .catch((err) => {
                if (err) {
                    return Promise.reject(new DatabaseError(Err.Code.DBQuery, err));
                }
            });
    }

    private countByModelValues<T>(model: string, modelValues: T, option: IQueryOption = {}, transaction?: Transaction): Promise<IQueryResult<T>> {
        const condition = new Condition(Condition.Operator.And);
        for (let i = 0, keys = Object.keys(modelValues), il = keys.length; i < il; i++) {
            condition.append((new Condition(Condition.Operator.EqualTo)).compare(keys[i], modelValues[keys[i]]));
        }
        const query = new Vql(model);
        if (option.fields) { query.select(...option.fields); }
        if (option.offset || option.page) {
            query.fromOffset(option.offset ? option.offset : (option.page - 1) * option.limit);
        }
        if (option.relations) { query.fetchRecordFor(...option.relations); }
        if (+option.limit) { query.limitTo(option.limit); }
        query.where(condition);
        query.orderBy = option.orderBy || [];
        return this.countByQuery(query, transaction);
    }

    private countByQuery<T>(query: Vql, transaction?: Transaction): Promise<IQueryResult<T>> {
        const result: IQueryResult<T> = {} as IQueryResult<T>;
        const params: ICalculatedQueryOptions = this.getQueryParams(query);
        params.condition = params.condition ? "WHERE " + params.condition : "";
        const queryString = `SELECT COUNT(*) as total FROM \`${query.model}\` ${params.join} ${params.condition}`;
        return this.query(queryString, null, transaction)
            .then((data) => {
                result.total = data[0].total;
                return result;
            })
            .catch((error) => {
                return Promise.reject(new DatabaseError(Err.Code.DBQuery, error));
            });
    }

    private insertOne<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>> {
        const localTransaction = !transaction;
        const prepare: Promise<Transaction> = this.prepareTransaction(transaction).then((tr) => transaction = tr);
        const result: IUpsertResult<T> = {} as IUpsertResult<T>;
        const analysedValue = this.getAnalysedValue<T>(model, value);
        const properties = [];
        const propertiesValue = [];
        for (let i = analysedValue.properties.length; i--;) {
            properties.push(`\`${analysedValue.properties[i].field}\` = ?`);
            propertiesValue.push(analysedValue.properties[i].value);
        }
        return prepare.then((tr) => this.query(`INSERT INTO \`${model}\` SET ${properties.join(",")}`, propertiesValue, tr))
            .then((insertResult: any) => {
                const steps = [];
                for (const key in analysedValue.relations) {
                    if (analysedValue.relations.hasOwnProperty(key)) {
                        steps.push(this.addRelation(new this.models[model]({ id: insertResult.insertId }), key, analysedValue.relations[key], transaction));
                    }

                }
                for (const key in analysedValue.lists) {
                    if (analysedValue.lists.hasOwnProperty(key)) {
                        steps.push(this.addList(new this.models[model]({ id: insertResult.insertId }), key, analysedValue.lists[key], transaction));
                    }
                }
                const id = insertResult.insertId;
                return Promise.all(steps)
                    .then(() => this.query(`SELECT * FROM \`${model}\` WHERE ${this.pk(model)} = ?`, [id], transaction));
            })
            .then((list) => {
                result.items = list as T[];
                return localTransaction ? transaction.commit().then(() => result) : result;
            })
            .catch((err) => {
                const error = new Err(Err.Code.DBInsert, err && err.message);
                return localTransaction ?
                    transaction.rollback().then(() => Promise.reject(error)) :
                    Promise.reject(error);
            });
    }

    private updateList<T>(model: T, list, value, transaction: Transaction) {
        const modelName = (model as any).schema.name;
        const table = modelName + this.pascalCase(list) + "List";
        return this.query(`DELETE FROM ${table} WHERE fk = ?`, [model[this.pk(modelName)]], transaction)
            .then(() => {
                return this.addList(model, list, value, transaction);
            });
    }

    private addList<T>(model: T, list: string, value: any[], transaction: Transaction): Promise<any> {
        const modelName = (model as any).schema.name;
        if (!value || !value.length) {
            return Promise.resolve([]);
        }
        const values = [];
        const condition = value.reduce((prev, thisValue, index, items) => {
            let result = prev;
            result += `(?,?)`;
            if (index < items.length - 1) { result += ","; }
            values.push(model[this.pk(modelName)]);
            values.push(thisValue);
            return result;
        }, "");
        const table = modelName + this.pascalCase(list) + "List";
        return this.query(`INSERT INTO ${table} (\`fk\`,\`value\`) VALUES ${condition}`, values, transaction);

    }

    private insertAll<T>(model: string, value: T[], transaction?: Transaction): Promise<IUpsertResult<T>> {
        const result: IUpsertResult<T> = {} as IUpsertResult<T>;
        const fields = this.schemaList[model].getFields();
        const fieldsName = [];
        const insertList = [];
        const pk = this.pk(model);
        for (const field in fields) {
            if (fields.hasOwnProperty(field) && fields[field].properties.type != FieldType.Relation ||
                fields[field].properties.relation.type == RelationType.One2Many) {
                // escape primary key with empty value
                if (field != pk || value[0][pk]) {
                    fieldsName.push(field);
                }
            }
        }
        for (let i = value.length; i--;) {
            const insertPart = [];
            for (let j = 0, jl = fieldsName.length; j < jl; j++) {
                if (fields[fieldsName[j]].properties.type == FieldType.Object) {
                    insertPart.push(value[i].hasOwnProperty(fieldsName[j]) ? this.escape(JSON.stringify(value[i][fieldsName[j]])) : "''");
                } else {
                    const itemValue = value[i][fieldsName[j]];
                    let isNum = false;
                    if ([FieldType.Number, FieldType.Integer, FieldType.Timestamp, FieldType.Relation, FieldType.Enum].indexOf(fields[fieldsName[j]].properties.type) >= 0) {
                        isNum = true;
                    }
                    insertPart.push(value[i].hasOwnProperty(fieldsName[j]) ? this.escape(itemValue) : (isNum ? 0 : "''"));
                }
            }
            insertList.push(`(${insertPart.join(",")})`);
        }

        if (!insertList.length) {
            result.items = [];
            return Promise.resolve(result);
        }

        const prepare: Promise<Transaction> = transaction ?
            this.prepareTransaction(transaction).then((tr) => transaction = tr) :
            Promise.resolve(null);
        return prepare.then((tr) => this.query<any>(`INSERT INTO ${model} (${fieldsName.map((filed) => "\`" + filed + "\`").join(",")}) VALUES ${insertList}`, null, tr))
            .then((insertResult) => {
                const lastId = insertResult.insertId;
                const count = insertResult.affectedRows;
                if (count != value.length) {
                    throw new DatabaseError(Err.Code.DBInsert, null);
                }
                // for (let i = count; i--;) {
                //     value[i][this.pk(model)] = lastId--;
                // }
                result.items = value;
                return result;
            })
            .catch((err) => {
                const error = new Err(Err.Code.DBInsert, err && err.message);
                return Promise.reject(error);
            });

    }

    private addRelation<T, M>(model: T, relation: string, value: number | number[] | M | M[], transaction?: Transaction): Promise<IUpsertResult<M>> {
        const modelName = (model.constructor as IModel).schema.name;
        const fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation && (value || +value === 0)) {
            switch (fields[relation].properties.relation.type) {
                case RelationType.One2Many:
                    return this.addOneToManyRelation(model, relation, value, transaction);
                case RelationType.Many2Many:
                    return this.addManyToManyRelation(model, relation, value, transaction);
                default:
                    return Promise.resolve({ items: [] } as IUpsertResult<M>);
            }
        }
        return Promise.reject(new DatabaseError(Err.Code.DBRelation, null));
    }

    private removeRelation<T>(model: T, relation: string, condition?: Condition | number | number[], transaction?: Transaction): Promise<any> {
        const modelName = (model.constructor as IModel).schema.name;
        const relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        let safeCondition: Condition;
        if (typeof condition == "number") {
            safeCondition = new Condition(Condition.Operator.EqualTo);
            safeCondition.compare(this.pk(relatedModelName), condition);
        } else if (condition instanceof Array && condition.length) {
            safeCondition = new Condition(Condition.Operator.Or);
            for (let i = condition.length; i--;) {
                safeCondition.append((new Condition(Condition.Operator.EqualTo)).compare(this.pk(relatedModelName), condition[i]));
            }
        } else if (condition instanceof Condition) {
            safeCondition = condition as Condition;
        }
        const fields = this.schemaList[modelName].getFields();
        if (fields[relation] && fields[relation].properties.type == FieldType.Relation) {
            switch (fields[relation].properties.relation.type) {
                case RelationType.One2Many:
                    return this.removeOneToManyRelation(model, relation, transaction);
                case RelationType.Many2Many:
                    return this.removeManyToManyRelation(model, relation, safeCondition, transaction);
                default:
                    return Promise.resolve({});
            }
        }
        return Promise.reject(new DatabaseError(Err.Code.DBDelete, null));
    }

    private updateRelations(model: Model, relation, relatedValues, transaction?: Transaction) {
        const modelName = (model.constructor as IModel).schema.name;
        const relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        const isWeek = this.schemaList[modelName].getFields()[relation].properties.relation.isWeek;
        const ids = [0];
        if (relatedValues instanceof Array) {
            for (let i = relatedValues.length; i--;) {
                if (relatedValues[i]) {
                    ids.push(typeof relatedValues[i] == "object" && relatedValues[i][this.pk(relatedModelName)] ? relatedValues[i][this.pk(relatedModelName)] : relatedValues[i]);
                }
            }
        }
        return this.query(`DELETE FROM ${this.pascalCase(modelName)}Has${this.pascalCase(relation)}
                    WHERE ${this.camelCase(modelName)} = ?`, [model[this.pk(modelName)]], transaction)
            .then(() => this.addRelation(model, relation, ids, transaction));
    }

    private updateOne<T>(model: string, value: T, transaction?: Transaction): Promise<IUpsertResult<T>> {
        const localTransaction = !transaction;
        const prepare: Promise<Transaction> = this.prepareTransaction(transaction).then((tr) => transaction = tr);
        const result: IUpsertResult<T> = {} as IUpsertResult<T>;
        const analysedValue = this.getAnalysedValue<T>(model, value);
        const properties = [];
        const propertiesData = [];
        for (let i = analysedValue.properties.length; i--;) {
            if (analysedValue.properties[i].field != this.pk(model)) {
                properties.push(`\`${analysedValue.properties[i].field}\` = ?`);
                propertiesData.push(analysedValue.properties[i].value);
            }
        }
        const id = value[this.pk(model)];
        const steps = [];
        const relationsNames = Object.keys(analysedValue.relations);
        const modelFields = this.schemaList[model].getFields();

        return prepare.then((tr) => {
            for (let i = relationsNames.length; i--;) {
                const relation = relationsNames[i];
                const relationValue = analysedValue.relations[relation];
                // todo check if it is required
                if (!relationValue && (relationValue !== 0)) { continue; }
                switch (modelFields[relation].properties.relation.type) {
                    case RelationType.One2Many:
                        let fk = +relationValue;
                        if (!fk && "object" == typeof relationValue) {
                            const relatedModelName = modelFields[relation].properties.relation.model.schema.name;
                            fk = +relationValue[this.pk(relatedModelName)];
                        }
                        if (fk || fk === 0) {
                            properties.push(`\`${relation}\` = ?`);
                            propertiesData.push(fk);
                        }
                        break;
                    case RelationType.Many2Many:
                        if (!relationValue) { continue; }
                        steps.push(this.updateRelations(new this.models[model](value), relation, relationValue, tr));
                        break;
                }
            }
            for (const key in analysedValue.lists) {
                if (analysedValue.lists.hasOwnProperty(key)) {
                    steps.push(this.updateList(new this.models[model]({ id }), key, analysedValue.lists[key], tr));
                }
            }
            return Promise.all<any>(steps).then(() => tr);
        })

            .then((tr) => properties.length ? this.query<T[]>(`UPDATE \`${model}\` SET ${properties.join(",")} WHERE ${this.pk(model)} = ?`, propertiesData.concat([id]), tr) : [])
            .then(() => localTransaction ? transaction.commit() : true)
            .then(() => (this.findById<T>(model, id) as Promise<IUpsertResult<T>>))
            .catch((err) => {
                const error = new Err(Err.Code.DBQuery, err && err.message);
                return localTransaction ?
                    transaction.rollback().then(() => Promise.reject(error)) :
                    Promise.reject(error);
            });

    }

    private updateAll<T>(model: string, newValues: T, condition: Condition, transaction?: Transaction): Promise<IUpsertResult<T>> {
        const localTransaction = !transaction;
        const prepare: Promise<Transaction> = this.prepareTransaction(transaction).then((tr) => transaction = tr);
        const sqlCondition = this.getCondition(model, condition);
        const result: IUpsertResult<T> = {} as IUpsertResult<T>;
        const properties = [];
        const propertiesData = [];
        for (const key in newValues) {
            if (newValues.hasOwnProperty(key) && this.schemaList[model].getFieldsNames().indexOf(key) >= 0 && key != this.pk(model)) {
                properties.push(`\`${model}\`.${key} = ?`);
                propertiesData.push(newValues[key]);
            }
        }
        return prepare.then((tr) => this.query<T[]>(`SELECT ${this.pk(model)} FROM \`${model}\` ${sqlCondition ? `WHERE ${sqlCondition}` : ""}`, null, tr))
            .then((list) => {
                const ids = [];
                for (let i = list.length; i--;) {
                    ids.push(list[i][this.pk(model)]);
                }
                if (!ids.length) { return []; }
                return this.query<any>(`UPDATE \`${model}\` SET ${properties.join(",")}  WHERE ${this.pk(model)} IN (?)`, propertiesData.concat([ids]), transaction)
                    .then((updateResult) => {
                        return this.query<T[]>(`SELECT * FROM \`${model}\` WHERE ${this.pk(model)} IN (?)`, [ids], transaction);
                    });
            })
            .then((list) => {
                result.items = list;
                return localTransaction ? transaction.commit().then(() => result) : result;
            })
            .catch((err) => {
                const error = new Err(Err.Code.DBUpdate, err && err.message);
                return localTransaction ?
                    transaction.rollback().then(() => Promise.reject(error)) :
                    Promise.reject(error);
            });
    }

    private deleteOne(model: string, id: number | string, transaction?: Transaction): Promise<IDeleteResult> {
        const localTransaction = !transaction;
        const prepare: Promise<Transaction> = this.prepareTransaction(transaction).then((tr) => transaction = tr);
        const result: IDeleteResult = {} as IDeleteResult;
        const fields = this.schemaList[model].getFields();
        return prepare.then((tr) => this.query(`DELETE FROM \`${model}\` WHERE ${this.pk(model)} = ?`, [id], tr))
            .then((deleteResult) => {
                const instance = new this.models[model]();
                instance[this.pk(model)] = id;
                const steps = [];
                for (const field in this.schemaList[model].getFields()) {
                    if (fields.hasOwnProperty(field) && fields[field].properties.type == FieldType.Relation) {
                        steps.push(this.removeRelation(instance, field, 0, transaction));
                    }
                }
                result.items = [id];
                return Promise.all(steps).then(() => result);
            })
            .then((rslt) => localTransaction ? transaction.commit().then(() => rslt) : rslt)
            .catch((err) => {
                const error = new Err(Err.Code.DBDelete, err && err.message);
                return localTransaction ? transaction.rollback().then(() => Promise.reject(error)) : Promise.reject(error);
            });
    }

    private deleteAll<T>(model: string, condition: Condition, transaction?: Transaction): Promise<IDeleteResult> {
        const localTransaction = !transaction;
        const prepare: Promise<Transaction> = this.prepareTransaction(transaction).then((tr) => transaction = tr);

        const sqlCondition = this.getCondition(model, condition);
        const result: IDeleteResult = {} as IDeleteResult;
        return prepare.then((tr) => this.query<T[]>(`SELECT ${this.pk(model)} FROM \`${model}\` ${sqlCondition ? `WHERE ${sqlCondition}` : ""}`, null, tr))
            .then((list) => {
                const ids = [];
                for (let i = list.length; i--;) {
                    ids.push(list[i][this.pk(model)]);
                }
                if (!ids.length) { return []; }
                return this.query(`DELETE FROM \`${model}\` WHERE ${this.pk(model)} IN (?)`, [ids], transaction)
                    .then((deleteResult) => ids);
            })
            .then((ids) => {
                result.items = ids;
                return localTransaction ? transaction.commit().then(() => result) : result;
            })
            .catch((err) => {
                const error = new Err(Err.Code.DBDelete, err && err.message);
                return localTransaction ? transaction.rollback().then(() => Promise.reject(error)) : Promise.reject(error);
            });
    }

    private getAnalysedValue<T>(model: string, value: T) {
        const properties = [];
        const schemaFieldsName = this.schemaList[model].getFieldsNames();
        const schemaFields = this.schemaList[model].getFields();
        const relations = {};
        const lists = {};

        for (const key in value) {
            if (value.hasOwnProperty(key) && schemaFieldsName.indexOf(key) >= 0 && value[key] !== undefined) {
                if (schemaFields[key].properties.type == FieldType.Relation) {
                    relations[key as string] = value[key as string];
                } else if (schemaFields[key].properties.type == FieldType.List) {
                    lists[key as string] = value[key as string];
                } else {
                    const thisValue: any = schemaFields[key].properties.type == FieldType.Object ? JSON.stringify(value[key]) : value[key];
                    properties.push({ field: key, value: thisValue });
                }
            }
        }
        return { properties, relations, lists };
    }

    private getQueryParams(query: Vql, alias: string = query.model): ICalculatedQueryOptions {
        const params: ICalculatedQueryOptions = {} as ICalculatedQueryOptions;
        query.offset = query.offset ? query.offset : (query.page ? query.page - 1 : 0) * query.limit;
        params.limit = "";
        if (+query.limit) {
            params.limit = `LIMIT ${query.offset ? +query.offset : 0}, ${+query.limit} `;
        }
        params.orderBy = "";
        if (query.orderBy.length) {
            const orderArray = [];
            for (const orderBy of query.orderBy) {
                if (this.models[query.model].schema.getField(orderBy.field)) {
                    orderArray.push(`\`${alias}\`.${orderBy.field} ${orderBy.ascending ? "ASC" : "DESC"}`);
                }
            }
            params.orderBy = orderArray.join(",");
        }
        const fields: string[] = [];
        const modelFields = this.schemaList[query.model].getFields();
        if (query.fields && query.fields.length) {
            for (const field of query.fields) {
                if (field instanceof Vql) {
                    fields.push(this.getSubQuery(field as Vql));
                } else if (modelFields[field as string]) {
                    if (modelFields[field as string].properties.type == FieldType.List) { continue; }
                    fields.push(`\`${alias}\`.${field}`);
                }
            }
        } else {
            for (const key in modelFields) {
                if (modelFields.hasOwnProperty(key)) {
                    if (modelFields[key].properties.type == FieldType.List) { continue; }
                    if (modelFields[key].properties.type != FieldType.Relation) {
                        fields.push(`\`${alias}\`.${modelFields[key].fieldName}`);
                    } else if ((!query.relations || query.relations.indexOf(modelFields[key].fieldName) < 0) && (modelFields[key].properties.relation.type == RelationType.One2Many)) {
                        fields.push(`\`${alias}\`.${modelFields[key].fieldName}`);
                    }
                }
            }
        }

        for (const relation of query.relations) {
            const relationName: string = typeof relation == "string" ? relation as string : (relation as IRelation).name;
            const field: Field = modelFields[relationName];
            if (!field) {
                throw new Error(`FIELD ${relationName} NOT FOUND IN model ${query.model} as ${alias}`);
            }
            const properties = field.properties;
            if (properties.type == FieldType.Relation) {
                if (properties.relation.type == RelationType.One2Many) {
                    const modelFiledList = [];
                    const filedNameList = properties.relation.model.schema.getFieldsNames();
                    const relatedModelFields = properties.relation.model.schema.getFields();
                    for (const filedName of filedNameList) {
                        if (typeof relation == "string" || (relation as IRelation).fields.indexOf(filedName) >= 0) {
                            if (relatedModelFields[filedName].properties.type != FieldType.List && (relatedModelFields[filedName].properties.type != FieldType.Relation ||
                                relatedModelFields[filedName].properties.relation.type == RelationType.One2Many)) {
                                modelFiledList.push(`'${this.quote}${filedName}${this.quote}:','${this.quote}',COALESCE(c.${filedName},''),'${this.quote}'`);
                            }
                        }
                    }
                    const name = properties.relation.model.schema.name;
                    if (modelFiledList.length) {
                        fields.push(`(SELECT CONCAT('{',${modelFiledList.join(',",",')},'}') FROM \`${name}\` as c WHERE c.${this.pk(name)} = \`${alias}\`.${field.fieldName}  LIMIT 1) as ${field.fieldName}`);
                    }
                }
            }
        }
        params.condition = "";
        if (query.condition) {
            params.condition = this.getCondition(query.model, query.condition, alias);
            params.condition = params.condition ? params.condition : "";
        }
        params.join = "";
        if (query.joins && query.joins.length) {
            const joins = [];
            for (const join of query.joins) {
                let type = "";
                switch (join.type) {
                    case Vql.Join:
                        type = "FULL OUTER JOIN";
                        break;
                    case Vql.LeftJoin:
                        type = "LEFT JOIN";
                        break;
                    case Vql.RightJoin:
                        type = "RIGHT JOIN";
                        break;
                    case Vql.InnerJoin:
                        type = "INNER JOIN";
                        break;
                    default:
                        type = "LEFT JOIN";
                }
                const modelsAlias = join.vql.model + "_" + join.field; // + '__' + Math.floor(Math.random() * 100).toString(); // creating alias need refactoring some part code so i ignored it for this time.
                if (this.models[alias].schema.getField(join.field) && this.models[join.vql.model]) {
                    joins.push(`${type} ${join.vql.model} as ${modelsAlias} ON (${alias}.${join.field} = ${modelsAlias}.${this.pk(join.vql.model)})`);
                    const joinParam = this.getQueryParams(join.vql, modelsAlias);
                    if (joinParam.fields) {
                        fields.push(joinParam.fields);
                    }
                    if (joinParam.condition) {
                        params.condition = params.condition ? `(${params.condition} AND ${joinParam.condition})` : joinParam.condition;
                    }
                    if (joinParam.orderBy) {
                        params.orderBy = params.orderBy ? `${params.orderBy},${joinParam.orderBy}` : joinParam.orderBy;
                    }
                    if (joinParam.join) {
                        joins.push(joinParam.join);
                    }
                }
            }
            params.join = joins.join("\n");
        }
        params.fields = fields.join(",");
        params.fieldsList = fields;
        return params;
    }

    private getSubQuery<T>(query: Vql) {
        query.relations = []; // relations not handle in next version;
        query.joins = [];
        const params: ICalculatedQueryOptions = this.getQueryParams(query, query.model);
        params.condition = params.condition ? "WHERE " + params.condition : "";
        params.orderBy = params.orderBy ? "ORDER BY " + params.orderBy : "";
        const modelFiledList = [];
        for (let i = 0, il = params.fieldsList.length; i < il; i++) {
            const field = params.fieldsList[i].replace(`\`${query.model}\`.`, "");
            modelFiledList.push(`'${this.quote}${field}${this.quote}:','${this.quote}',COALESCE(${field},''),'${this.quote}'`);
        }
        const modelAs = query.model[0].toLowerCase() + query.model.substr(1, query.model.length - 1);
        return `(SELECT CONCAT('{',${modelFiledList.join(',",",')},'}') FROM \`${query.model}\` ${params.condition} ${params.orderBy} limit 1) as \`${modelAs}\``;
    }

    private getCondition(model: string, condition: Condition, alias: string = model) {
        model = condition.model || model;
        const operator = this.getOperatorSymbol(condition.operator);
        if (!condition.isConnector) {
            if (!this.models[model].schema.getField(condition.comparison.field)) {
                return "";
            }
            return `(\`${alias}\`.${condition.comparison.field} ${operator} ${condition.comparison.isValueOfTypeField ? condition.comparison.value : `${this.escape(isUndefined(condition.comparison.value.id) ? condition.comparison.value : +condition.comparison.value.id)}`})`;
        } else {
            const childrenCondition = [];
            for (const child of condition.children) {
                const childCondition = this.getCondition(model, child).trim();
                if (childCondition) {
                    childrenCondition.push(childCondition);
                }
            }
            const childrenConditionStr = childrenCondition.join(` ${operator} `).trim();
            return childrenConditionStr ? `(${childrenConditionStr})` : "";
        }
    }

    private getManyToManyRelation(list: any[], query: Vql, transaction?: Transaction) {
        const ids = [];

        for (let i = list.length; i--;) {
            ids.push(list[i][this.pk(query.model)]);
        }
        const relations: Array<Promise<any>> = [];
        if (ids.length && query.relations && query.relations.length) {
            for (let i = query.relations.length; i--;) {
                const relationName = typeof query.relations[i] == "string" ? query.relations[i] as string : (query.relations[i] as IRelation).name;
                const field = this.schemaList[query.model].getFields()[relationName];
                const relationship = field.properties.relation;
                if (relationship.type == RelationType.Many2Many) {
                    relations.push(this.runRelatedQuery(query, i, ids, transaction));
                } else if (relationship.type == RelationType.Reverse) {
                    const reverseField = this.getReverseRelation(query, field);
                    if (reverseField.properties.relation.type == RelationType.One2Many) {
                        relations.push(this.runReverseQueryOne2Many(query, i, ids, reverseField, transaction));
                    } else if (reverseField.properties.relation.type == RelationType.Many2Many) {
                        relations.push(this.runRelatedQueryMany2Many(query, i, ids, reverseField, transaction));
                    }

                }
            }
        }
        if (!relations.length) { return Promise.resolve(list); }
        return Promise.all(relations)
            .then((data) => {
                const leftKey = this.camelCase(query.model);
                for (let i = data.length; i--;) {
                    for (const related in data[i]) {
                        if (data[i].hasOwnProperty(related)) {
                            const relationship = this.schemaList[query.model].getFields()[related].properties.relation;
                            const rightKey = this.camelCase(relationship.model.schema.name);
                            for (let k = list.length; k--;) {
                                const id = list[k][this.pk(query.model)];
                                list[k][related] = [];
                                for (let j = data[i][related].length; j--;) {
                                    if (id == data[i][related][j][this.camelCase(query.model)]) {
                                        const relatedData = data[i][related][j];
                                        relatedData[this.pk(relationship.model.schema.name)] = relatedData[rightKey];
                                        delete relatedData[rightKey];
                                        delete relatedData[leftKey];
                                        list[k][related].push(relatedData);
                                    }
                                }

                            }
                        }
                    }
                }
                return list;
            });

    }

    private runRelatedQuery(query: Vql, i: number, ids: number[], transaction?: Transaction) {
        const relationName = typeof query.relations[i] == "string" ? query.relations[i] as string : (query.relations[i] as IRelation).name;
        const relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
        let fields = "*";
        if (typeof query.relations[i] != "string") {
            for (let j = (query.relations[i] as IRelation).fields.length; j--;) {
                (query.relations[i] as IRelation).fields[j] = `m.${(query.relations[i] as IRelation).fields[j]}`;
            }
            fields = (query.relations[i] as IRelation).fields.join(",");
        }
        const leftKey = this.camelCase(query.model);
        const rightKey = this.camelCase(relationship.model.schema.name);
        return this.query(`SELECT ${fields},r.${leftKey},r.${rightKey}  FROM \`${relationship.model.schema.name}\` m
                LEFT JOIN \`${query.model + "Has" + this.pascalCase(relationName)}\` r
                ON (m.${this.pk(relationship.model.schema.name)} = r.${rightKey})
                WHERE r.${leftKey} IN (?)`, [ids], transaction)
            .then((relatedList) => {
                const result = {};
                result[relationName] = relatedList;
                return result;
            });

    }

    private runReverseQueryOne2Many(query: Vql, i: number, ids: number[], reverseField: Field, transaction?: Transaction) {
        const relationName = typeof query.relations[i] == "string" ? query.relations[i] as string : (query.relations[i] as IRelation).name;
        const relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
        let fields = ["*"];
        if (typeof query.relations[i] != "string") {
            fields = (query.relations[i] as IRelation).fields;
        }
        const leftKey = this.camelCase(query.model);
        const rightKey = this.camelCase(relationship.model.schema.name);
        fields.push(`${reverseField.fieldName} as ${leftKey}`);
        fields.push(`${this.pk(relationship.model.schema.name)} as ${rightKey}`);
        return this.query(`SELECT ${fields.join(",")} FROM ${relationship.model.schema.name} WHERE ${reverseField.fieldName} IN (?)`, [ids], transaction)
            .then((list) => {
                const data = {};
                data[relationName] = list;
                return data;
            });
    }

    private runRelatedQueryMany2Many(query: Vql, i: number, ids: number[], reverseField: Field, transaction?: Transaction) {
        const relationName = typeof query.relations[i] == "string" ? query.relations[i] as string : (query.relations[i] as IRelation).name;
        const relationship = this.schemaList[query.model].getFields()[relationName].properties.relation;
        let fields = "*";
        if (typeof query.relations[i] != "string") {
            for (let j = (query.relations[i] as IRelation).fields.length; j--;) {
                (query.relations[i] as IRelation).fields[j] = `m.${(query.relations[i] as IRelation).fields[j]}`;
            }
            fields = (query.relations[i] as IRelation).fields.join(",");
        }
        const leftKey = this.camelCase(query.model);
        const rightKey = this.camelCase(relationship.model.schema.name);
        return this.query(`SELECT ${fields},r.${leftKey},r.${rightKey}  FROM \`${relationship.model.schema.name}\` m
                LEFT JOIN \`${relationship.model.schema.name + "Has" + this.pascalCase(reverseField.fieldName)}\` r
                ON (m.${this.pk(query.model)} = r.${rightKey})
                WHERE r.${leftKey} IN (?)`, [ids], transaction)
            .then((relatedList) => {
                const result = {};
                result[relationName] = relatedList;
                return result;
            });

    }

    private getReverseRelation(query: Vql, field: Field): Field | null {
        const modelName = query.model;
        let relatedField = null;
        const relatedModel = field.properties.relation.model;
        const fields = relatedModel.schema.getFields();
        const keys = relatedModel.schema.getFieldsNames();
        for (let i = 0, il = keys.length; i < il; i++) {
            const properties = fields[keys[i]].properties;
            if (properties.type == FieldType.Relation && properties.relation.model.schema.name == modelName) {
                relatedField = fields[keys[i]];
                break;
            }
        }
        return relatedField;
    }

    private getLists(list: any[], query: Vql, transaction?: Transaction) {
        const runListQuery = (listName) => {
            const name = query.model + this.pascalCase(listName) + "List";
            return this.query(`SELECT * FROM \`${name}\` WHERE fk IN (?)`, [ids], transaction)
                .then((listsData) => ({ name: listName, data: listsData }));

        };
        const primaryKey = this.pk(query.model);
        const ids = [];
        for (let i = list.length; i--;) {
            ids.push(list[i][primaryKey]);
        }
        const promiseList: Array<Promise<any>> = [];
        if (ids.length) {
            const fields = this.schemaList[query.model].getFields();
            for (let keys = Object.keys(fields), i = 0, il = keys.length; i < il; i++) {
                const field = keys[i];
                if (fields[field].properties.type == FieldType.List && (!query.fields || !query.fields.length || query.fields.indexOf(field) >= 0)) {
                    promiseList.push(runListQuery(field));
                }
            }
        }
        if (!promiseList.length) { return Promise.resolve(list); }
        const listJson = {};
        for (let i = list.length; i--;) {
            listJson[list[i][primaryKey]] = list[i];
        }
        return Promise.all(promiseList)
            .then((data) => {
                for (let i = data.length; i--;) {
                    const listName = data[i].name;
                    const listData = data[i].data;
                    for (let k = listData.length; k--;) {
                        const id = listData[k].fk;
                        listJson[id][listName] = listJson[id][listName] || [];
                        listJson[id][listName].push(listData[k].value);
                    }
                }
                return list;
            });
    }

    private normalizeList(schema: Schema, list: any[]) {
        const fields: IModelFields = schema.getFields();
        for (let i = list.length; i--;) {
            for (const key in list[i]) {
                if (list[i].hasOwnProperty(key) &&
                    fields.hasOwnProperty(key) && (fields[key].properties.type == FieldType.Object || (
                        fields[key].properties.type == FieldType.Relation &&
                        (fields[key].properties.relation.type == RelationType.One2Many)))) {
                    list[i][key] = this.parseJson(list[i][key], fields[key].properties.type == FieldType.Object);
                } else if (list[i].hasOwnProperty(key) && !fields.hasOwnProperty(key)) {
                    const isObject = list[i][key] && list[i][key].indexOf && list[i][key].indexOf(this.quote) < 0;
                    list[i][key] = this.parseJson(list[i][key], isObject);
                }
            }
        }
        return list;
    }

    private parseJson(str, isObject = false) {
        if (typeof str == "string" && str) {
            let json;
            try {
                if (!isObject) {
                    const replace = ["\\n", "”", "\\r", "\\t", "\\v", "’", '"'];
                    const search = [/\n/ig, /"/ig, /\r/ig, /\t/ig, /\v/ig, /'/ig, new RegExp(this.quote, "gi")];
                    for (let i = 0; i < search.length; i++) {
                        str = str.replace(search[i], replace[i]);
                    }
                }
                json = JSON.parse(str);
            } catch (e) {
                json = str;
            }
            return json;
        } else {
            return str;
        }
    }

    private createTable(schema: Schema) {
        const fields = schema.getFields();
        const createDefinition = this.createDefinition(fields, schema.name);
        const ownTablePromise =
            this.query(`DROP TABLE IF EXISTS \`${schema.name}\``)
                .then(() => {
                    return this.query(`CREATE TABLE \`${schema.name}\` (\n${createDefinition.ownColumn})\n`);
                });
        let translateTablePromise: Promise<any> = Promise.resolve(true);
        if (createDefinition.lingualColumn) {
            translateTablePromise =
                this.query(`DROP TABLE IF EXISTS ${schema.name}_translation`)
                    .then(() => {
                        return this.query(`CREATE TABLE ${schema.name}_translation (\n${createDefinition.lingualColumn}\n)`);
                    });
        }

        return () => Promise.all([ownTablePromise, translateTablePromise].concat(createDefinition.relations));

    }

    private relationTable(field: Field, table: string): Promise<any> {
        const name = table + "Has" + this.pascalCase(field.fieldName);
        const schema = new Schema(name);
        schema.addField("id").primary().type(FieldType.Integer).required();
        schema.addField(this.camelCase(table)).type(FieldType.Integer).required();
        schema.addField(this.camelCase(field.properties.relation.model.schema.name)).type(FieldType.Integer).required();
        this.schemaList[name] = schema;
        return this.createTable(schema)();
    }

    private listTable(field: Field, table: string): Promise<any> {
        const name = table + this.pascalCase(field.fieldName) + "List";
        const schema = new Schema(name);
        schema.addField("id").primary().type(FieldType.Integer).required();
        schema.addField("fk").type(FieldType.Integer).required();
        schema.addField("value").type(field.properties.list).required();
        this.schemaList[name] = schema;
        return this.createTable(schema)();
    }

    private camelCase(str) {
        return str[0].toLowerCase() + str.slice(1);
    }

    private pascalCase(str) {
        return str[0].toUpperCase() + str.slice(1);
    }

    private createDefinition(fields: IModelFields, table: string, checkMultiLingual = true) {
        const multiLingualDefinition: string[] = [];
        const columnDefinition: string[] = [];
        const relations: Array<Promise<boolean>> = [];
        let keyIndex;
        for (const field in fields) {
            if (fields.hasOwnProperty(field)) {
                keyIndex = fields[field].properties.primary ? field : keyIndex;
                const column = this.columnDefinition(fields[field]);
                if (column) {
                    if (fields[field].properties.multilingual && checkMultiLingual) {
                        multiLingualDefinition.push(column);
                    } else {
                        columnDefinition.push(column);
                    }
                } else if (fields[field].properties.type == FieldType.Relation && fields[field].properties.relation.type == RelationType.Many2Many) {
                    relations.push(this.relationTable(fields[field], table));
                } else if (fields[field].properties.type == FieldType.List) {
                    relations.push(this.listTable(fields[field], table));
                }
            }
        }
        let keyFiled;

        if (keyIndex) {
            keyFiled = fields[keyIndex];
        } else {
            keyFiled = new Field("id");
            keyFiled.primary().type(FieldType.Integer).required();
            columnDefinition.push(this.columnDefinition(keyFiled));
        }

        const keySyntax = `PRIMARY KEY (${keyFiled.fieldName})`;
        columnDefinition.push(keySyntax);

        if (multiLingualDefinition.length) {
            multiLingualDefinition.push(this.columnDefinition(keyFiled));
            multiLingualDefinition.push(keySyntax);
        }

        return {
            lingualColumn: multiLingualDefinition.join(" ,\n "),
            ownColumn: columnDefinition.join(" ,\n "),
            relations,
        };
    }

    private columnDefinition(filed: Field) {
        const properties = filed.properties;
        if (properties.type == FieldType.List || (properties.relation && properties.relation.type == RelationType.Many2Many) || (properties.relation && properties.relation.type == RelationType.Reverse)) {
            return "";
        }
        let defaultRelation;
        if (properties.relation && (properties.relation.type == RelationType.One2Many)) {
            defaultRelation = true;
        }
        const defaultValue = properties.type != FieldType.Boolean ? `'${defaultRelation ? 0 : properties.default}'` : !!properties.default;
        let columnSyntax = `\`${filed.fieldName}\` ${this.getType(properties)}`;
        columnSyntax += (properties.required && properties.type != FieldType.Relation) || properties.primary ? " NOT NULL" : "";
        columnSyntax += properties.default || properties.default === 0 || properties.default === "" || defaultRelation ? ` DEFAULT ${defaultValue}` : "";
        columnSyntax += properties.unique ? " UNIQUE " : "";
        columnSyntax += properties.primary ? " AUTO_INCREMENT " : "";
        return columnSyntax;
    }

    private getType(properties: IFieldProperties) {
        let typeSyntax;
        switch (properties.type) {
            case FieldType.Boolean:
                typeSyntax = "BOOLEAN";
                break;
            case FieldType.EMail:
            case FieldType.File:
            case FieldType.Password:
            case FieldType.Tel:
            case FieldType.URL:
            case FieldType.String:
                if (!properties.primary) {
                    typeSyntax = `VARCHAR2(${properties.maxLength ? properties.maxLength : 255})`;
                } else {
                    typeSyntax = "BIGINT";
                }
                break;
            case FieldType.Float:
            case FieldType.Number:
                typeSyntax = `DECIMAL(${properties.max ? properties.max.toString().length + 10 : 20},10)`;
                break;
            case FieldType.Enum:
            case FieldType.Integer:
                typeSyntax = `INT(${properties.max ? properties.max.toString(2).length : 20})`;
                break;
            case FieldType.Object:
            case FieldType.Text:
                typeSyntax = `CLOB`;
                break;
            case FieldType.Timestamp:
                typeSyntax = "BIGINT";
                break;
            case FieldType.Relation:
                if (properties.relation.type == RelationType.One2Many) {
                    typeSyntax = "BIGINT";
                }
                break;

        }
        return typeSyntax;
    }

    private initializeDatabase() {
        return this.query(`ALTER DATABASE \`${this.config.database}\` CHARACTER SET  ${this.config.charset};`);
    }

    private getOperatorSymbol(operator: number): string {
        switch (operator) {
            // Connectors
            case Condition.Operator.And:
                return "AND";
            case Condition.Operator.Or:
                return "OR";
            // Comparison
            case Condition.Operator.EqualTo:
                return "=";
            case Condition.Operator.NotEqualTo:
                return "<>";
            case Condition.Operator.GreaterThan:
                return ">";
            case Condition.Operator.GreaterThanOrEqualTo:
                return ">=";
            case Condition.Operator.LessThan:
                return "<";
            case Condition.Operator.LessThanOrEqualTo:
                return "<=";
            case Condition.Operator.Like:
                return "LIKE";
            case Condition.Operator.NotLike:
                return "NOT LIKE";
            case Condition.Operator.Regex:
                return "REGEXP";
            case Condition.Operator.NotRegex:
                return "NOT REGEXP";
        }
    }

    private addOneToManyRelation<T, M>(model: T, relation: string, value: number | { [property: string]: any }, transaction?: Transaction): Promise<IUpsertResult<M>> {
        const result: IUpsertResult<T> = {} as IUpsertResult<T>;
        const modelName = (model.constructor as IModel).schema.name;
        const fields = this.schemaList[modelName].getFields();
        const relatedModelName = fields[relation].properties.relation.model.schema.name;
        let readIdPromise;
        if (fields[relation].properties.relation.isWeek && typeof value == "object" && !value[this.pk(relatedModelName)]) {
            readIdPromise = this.insertOne(relatedModelName, value, transaction).then((rslt) => rslt.items[0][this.pk(relatedModelName)]);
        } else {
            let id = 0;
            if (+value || +value === 0) {
                id = +value;
            } else if (typeof value == "object") {
                id = +value[this.pk(relatedModelName)];
            }
            if (id !== 0 && (!id || id < 0)) {
                return Promise.reject(new Err(Err.Code.DBRelation, `invalid <<${relation}>> related model id`));
            }
            readIdPromise = Promise.resolve(id);
        }
        return readIdPromise
            .then((id) => {
                return this.query<T[]>(`UPDATE \`${modelName}\` SET \`${relation}\` = ? WHERE ${this.pk(relatedModelName)}=? `, [id, model[this.pk(relatedModelName)]], transaction);
            })
            .then((updateResult) => {
                result.items = updateResult;
                return result;
            })
            .catch((err) => {
                return Promise.reject(new Err(Err.Code.DBUpdate, err && err.message));
            });

    }

    private addManyToManyRelation<T, M>(model: T, relation: string, value: number | number[] | M | M[], transaction?: Transaction): Promise<IUpsertResult<M>> {
        const result: IUpsertResult<M> = {} as IUpsertResult<M>;
        const modelName = (model.constructor as IModel).schema.name;
        const fields = this.schemaList[modelName].getFields();
        const relatedModelName = fields[relation].properties.relation.model.schema.name;
        const newRelation = [];
        const relationIds = [];
        if (+value > 0) {
            relationIds.push(+value);
        } else if (value instanceof Array) {
            for (let i = value.length; i--;) {
                if (+value[i]) {
                    relationIds.push(+value[i]);
                } else if (value[i] && typeof value[i] == "object") {
                    if (+value[i][this.pk(relatedModelName)]) { relationIds.push(+value[i][this.pk(relatedModelName)]); } else if (fields[relation].properties.relation.isWeek) { newRelation.push(value[i]); }
                }
            }
        } else if (typeof value == "object") {
            if (+value[this.pk(relatedModelName)]) {
                relationIds.push(+value[this.pk(relatedModelName)]);
            } else if (fields[relation].properties.relation.isWeek) { newRelation.push(value); }
        }
        return Promise.resolve()
            .then(() => {
                if (!newRelation.length) {
                    return relationIds;
                }
                return Promise.all(newRelation.map((rel) => this.insertOne(relatedModelName, rel, transaction).then((rslt) => rslt.items[0]))).then((items) => ({ items }))
                    // return this.insertAll(relatedModelName, newRelation, transaction)
                    .then((rslt) => {
                        for (let i = rslt.items.length; i--;) {
                            relationIds.push(rslt.items[i][this.pk(relatedModelName)]);
                        }
                        return relationIds;
                    });

            })
            .then((relId) => {
                if (!relId || !relId.length) {
                    result.items = [];
                    return result;
                }
                const insertList = [];
                for (let i = relId.length; i--;) {
                    insertList.push(`(${model[this.pk(modelName)]},${this.escape(relId[i])})`);
                }
                return this.query<any>(`INSERT INTO ${modelName}Has${this.pascalCase(relation)}
                    (\`${this.camelCase(modelName)}\`,\`${this.camelCase(relatedModelName)}\`) VALUES ${insertList.join(",")}`, null, transaction)
                    .then((insertResult) => {
                        result.items = insertResult;
                        return result;
                    });

            })
            .catch((err) => {
                return Promise.reject(new Err(Err.Code.DBInsert, err && err.message));
            });

    }

    private removeOneToManyRelation<T>(model: T, relation: string, transaction: Transaction) {
        const modelName = (model.constructor as IModel).schema.name;
        const result: IUpsertResult<T> = {} as IUpsertResult<T>;
        const relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        const isWeek = this.schemaList[modelName].getFields()[relation].properties.relation.isWeek;
        const preparePromise: Promise<number> = Promise.resolve(0);
        if (isWeek) {
            const readRelationId: Promise<number> = +model[relation] ? Promise.resolve(+model[relation]) : this.findById(modelName, model[this.pk(modelName)]).then((rslt) => rslt.items[0][relation]);
            readRelationId.then((relationId) => {
                return this.deleteOne(relatedModelName, relationId, transaction).then(() => relationId);
            });
        }
        return preparePromise
            .then(() => {
                return this.query<any>(`UPDATE \`${modelName}\` SET ${relation} = 0 WHERE ${this.pk(modelName)} = ?`, [model[this.pk(modelName)]], transaction)
                    .then((updateResult) => {
                        result.items = updateResult;
                        return result;
                    });

            })
            .catch((err) => {
                return Promise.reject(new Err(Err.Code.DBUpdate, err && err.message));
            });

    }

    private removeManyToManyRelation<T>(model: T, relation: string, condition: Condition, transaction: Transaction): Promise<any> {
        const modelName = (model.constructor as IModel).schema.name;
        const relatedModelName = this.schemaList[modelName].getFields()[relation].properties.relation.model.schema.name;
        const isWeek = this.schemaList[modelName].getFields()[relation].properties.relation.isWeek;
        let preparePromise: Promise<any>;
        if (condition) {
            const vql = new Vql(relatedModelName);
            vql.select(this.pk(relatedModelName)).where(condition);
            preparePromise = this.findByQuery(vql);
        } else {
            preparePromise = Promise.resolve();
        }

        return preparePromise
            .then((result) => {
                const conditions = [];
                let conditionsStr;
                const conditionValues = [];
                const relatedField = this.camelCase(relatedModelName);
                if (result && result.items.length) {
                    for (let i = result.items.length; i--;) {
                        result.items.push(+result.items[0][this.pk(relatedModelName)]);
                        conditions.push(`${relatedField} = ?`);
                        conditionValues.push(+result.items[0][this.pk(relatedModelName)]);
                    }
                } else if (result) {
                    conditions.push("FALSE");
                }
                conditionsStr = conditions.length ? ` AND ${conditions.join(" OR ")}` : "";
                return this.query<any[]>(`SELECT * FROM ${modelName + "Has" + this.pascalCase(relation)} WHERE ${this.camelCase(modelName)} = ? ${conditionsStr}`, conditionValues.concat([model[this.pk(modelName)]]))
                    .then((items) => {
                        const ids: number[] = [];
                        for (let i = items.length; i--;) {
                            ids.push(items[i][relatedField]);
                        }
                        return ids;
                    });
            })
            .then((ids) => {
                const relatedField = this.camelCase(relatedModelName);
                const idConditions = [];
                const idConditionValues = [];
                const cndtn = new Condition(Condition.Operator.Or);
                for (let i = ids.length; i--;) {
                    idConditions.push(`${relatedField} = ?`);
                    idConditionValues.push(+ids[i]);
                    cndtn.append(new Condition(Condition.Operator.EqualTo).compare("id", ids[i]));
                }
                const idCondition = ids.length ? `(${ids.join(" OR ")})` : "FALSE";
                return this.query(`DELETE FROM ${modelName + "Has" + this.pascalCase(relation)} WHERE ${this.camelCase(modelName)} = ? AND ${idCondition}`, [model[this.pk(modelName)]].concat(idConditionValues), transaction)
                    .then(() => {
                        const result = { items: ids };
                        if (isWeek && ids.length) {
                            return this.deleteAll(relatedModelName, cndtn, transaction).then(() => result);
                        }
                        return result;
                    });
            });
    }

    private escape(value): any {
        if (value != value) { value = 0; }
        if (typeof value == "number") { return value; }
        if (typeof value == "boolean") { return value ? 1 : 0; }
        // return this.connection.escape(value);
        return value;
    }
}
