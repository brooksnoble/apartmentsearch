module Mongo

open FSharp.MongoDB.Bson.Serialization
open MongoDB.Bson
open MongoDB.Driver
open System
open System.Linq.Expressions
open Microsoft.FSharp.Quotations
open Util

do Conventions.register()
do Serializers.register()

type MongoWrapper<'a> =
    { _id: BsonObjectId; value: 'a }

let conn (connString: string): MongoClient =
    new MongoClient(connString)

let getDb name (client: MongoClient) =
    client.GetDatabase(name)

let insert<'a> (db: IMongoDatabase) collection (value: 'a) =
    async {
        let collection = db.GetCollection<MongoWrapper<'a>> collection
        let id = BsonObjectId(ObjectId.GenerateNewId())

        let insert = collection.InsertOneAsync { _id = id; value = value }
        do! insert |> Async.awaitPlainTask
    }

let replace<'a> (db: IMongoDatabase) collection bsonID (value: 'a) =
    async {
        let collection = db.GetCollection<MongoWrapper<'a>> collection

        let replace = collection.ReplaceOneAsync(<@ fun (mw: MongoWrapper<'a>) -> mw._id = bsonID @> |> Expr.toLinq, { _id = bsonID; value = value })
        do! replace |> Async.awaitPlainTask
    }

let find<'a> (db: IMongoDatabase) collection (filter: Expr<MongoWrapper<'a> -> bool>) =
    async {
        let collection = db.GetCollection<MongoWrapper<'a>> collection

        let! results = collection.FindAsync (filter |> Expr.toLinq) |> Async.AwaitTask
        let! list = results.ToListAsync() |> Async.AwaitTask

        let results = 
            list
            |> List.ofSeq
            |> List.map (fun x -> (x._id, x.value))

        return results
    }

let findRaw<'a> (db: IMongoDatabase) collection filter =
    async {
        let collection = db.GetCollection<MongoWrapper<'a>> collection
        
        let! results = collection.FindAsync<MongoWrapper<'a>>(filter) |> Async.AwaitTask
        let! list = results.ToListAsync() |> Async.AwaitTask

        let results = 
            list
            |> List.ofSeq
            |> List.map (fun x -> (x._id, x.value))

        return results
    }

let delete<'a> (db: IMongoDatabase) collection (filter: Expr<MongoWrapper<'a> -> bool>) =
    async {
        let collection = db.GetCollection<MongoWrapper<'a>> collection
        let! results = collection.DeleteManyAsync (filter |> Expr.toLinq) |> Async.AwaitTask
        return results.DeletedCount
    }
