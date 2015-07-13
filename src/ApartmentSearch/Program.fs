module Program

open System
open System.Threading
open HttpClient
open HtmlAgilityPack
open FSharp.MongoDB.Bson.Serialization
open Suave.Web
open Suave.Http
open Suave.Http.Applicatives
open Suave.Http.Files
open Suave.Http.Successful
open Suave.Types
open System.Net
open Scraping
open Util

let cacheInMongo client collection (f: Async<'a list>) =
    async {
        let! deletedCount = Mongo.delete<'a> client collection <@ fun x -> true @>
        
        let! results = f

        do! results
            |> List.map (Mongo.insert<'a> client collection)
            |> Async.InBatches 20 1000
            |> Async.Ignore
    }

module Suave =
    open Suave.Types
    open Suave.Http.Successful

    let handle f view: WebPart =
      fun (x : HttpContext) ->
        async {
          let! results = f
          return! OK (view results) x
        }

module Handlers =
    let rescrape client =
        doScrape "durham-nc" [1..9] 
        |> cacheInMongo client "apartmentScrapes"

    let getErrors client =
        async {
            let! results = 
                Mongo.find<ApartmentScrape> client "apartmentScrapes" <@ fun x -> true @>
        
            return
                results
                |> List.map snd
                |> List.choose (fun x -> match x.Result with | Success _ -> None | Failure ex -> Some (x.Url, ex))
        }

    let getSortedByApartment client =
        async {
            let! results = 
                Mongo.find<ApartmentScrape> client "apartmentScrapes" <@ fun x -> true @>

            return 
                results
                |> List.map snd
                |> List.choose (fun x -> match x.Result with | Success x -> Some x | Failure _ -> None)
                |> List.sortBy (fun ai -> ai.YearBuilt)
                |> List.map (fun ai -> ai.YearBuilt, ai.Name, ai.Address)
        }

module Views = 
    let viewAsTable (headers: string * string * string) (results: (string * string * string) list) =
        let viewRow (a,b,c) = 
            sprintf "<tr><td>%s</td><td>%s</td><td>%s</td></tr>" a b c
        let x,y,z = headers
        let topRow = sprintf "<tr><th>%s</th><th>%s</th><th>%s</th></tr>" x y z
        sprintf "<table><thead>%s</thead><tbody>%s</tbody></table>" topRow (results |> List.map viewRow |> String.concat "")

    let viewAsTable2 (results: (string * exn list) list) =
        let viewRow (a,b: exn list) = 
            sprintf "<tr><td>%s</td><td>%A</td></tr>" a b
        let topRow = "<tr><th>URL</th><th>Exception</th></tr>"
        sprintf "<table><thead>%s</thead><tbody>%s</tbody></table>" topRow (results |> List.map viewRow |> String.concat "")

let client = Mongo.conn (env "MongoConnectionString")
let db = Mongo.getDb "local" client

let staticFilePath = env "StaticFilePath"

let app =
    choose
        [
            GET >>= choose [
                path "/scrape" >>= Suave.handle (Handlers.rescrape db) (fun () -> "Done!")
                path "/errors" >>= Suave.handle (Handlers.getErrors db) Views.viewAsTable2
                path "/results" >>= Suave.handle (Handlers.getSortedByApartment db) (Views.viewAsTable ("Year", "Name", "Address"))
                path "/" >>= file (staticFilePath + "index.html")
                browse staticFilePath
            ]

            OK "Unknown URL"
        ]

[<EntryPoint>]
let main argv =
    startWebServer
        { defaultConfig with bindings = [ HttpBinding.mk HTTP (IPAddress.Parse "0.0.0.0") 999us ] }
        app
    0
